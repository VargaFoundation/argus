#include "bigquery_internal.h"
#include "argus/log.h"
#include "argus/compat.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <glib.h>

#ifdef ARGUS_HAS_OPENSSL
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#endif

/*
 * OAuth2 for BigQuery.
 *
 * Two modes: a caller-supplied static bearer (AccessToken=...) and the
 * service-account JWT-bearer grant (RFC 7523) built from a JSON key file.
 * The token endpoint, JWT audience and scope are all configurable so the
 * same flow works against sovereign-cloud IAM (S3NS) whose URLs differ
 * from public Google.
 */

/* ── Header list ─────────────────────────────────────────────── */

void bq_auth_set_headers(bq_conn_t *conn)
{
    if (conn->headers) {
        curl_slist_free_all(conn->headers);
        conn->headers = NULL;
    }
    conn->headers = curl_slist_append(NULL, "Content-Type: application/json");
    conn->headers = curl_slist_append(conn->headers, "Accept: application/json");
    if (conn->access_token && *conn->access_token) {
        char *h = g_strdup_printf("Authorization: Bearer %s",
                                  conn->access_token);
        conn->headers = curl_slist_append(conn->headers, h);
        g_free(h);
    }
}

#ifdef ARGUS_HAS_OPENSSL

/* base64url without padding (RFC 7515) */
static char *b64url(const unsigned char *data, size_t len)
{
    char *b64 = g_base64_encode(data, len);
    if (!b64) return NULL;
    for (char *p = b64; *p; p++) {
        if (*p == '+') *p = '-';
        else if (*p == '/') *p = '_';
    }
    char *pad = strchr(b64, '=');
    if (pad) *pad = '\0';
    char *out = strdup(b64);
    g_free(b64);
    return out;
}

/* RS256-sign `input` with the PEM private key; returns base64url signature. */
static char *rs256_sign(const char *pem_key, const char *input)
{
    char *out = NULL;
    BIO *bio = BIO_new_mem_buf(pem_key, -1);
    if (!bio) return NULL;
    EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!pkey) return NULL;

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    size_t sig_len = 0;
    if (ctx &&
        EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey) == 1 &&
        EVP_DigestSign(ctx, NULL, &sig_len,
                       (const unsigned char *)input, strlen(input)) == 1) {
        unsigned char *sig = malloc(sig_len);
        if (sig &&
            EVP_DigestSign(ctx, sig, &sig_len,
                           (const unsigned char *)input, strlen(input)) == 1)
            out = b64url(sig, sig_len);
        free(sig);
    }
    if (ctx) EVP_MD_CTX_free(ctx);
    EVP_PKEY_free(pkey);
    return out;
}

/* Exchange a signed JWT assertion for an access token. */
static int bq_fetch_token(bq_conn_t *conn)
{
    time_t now = time(NULL);
    const char *aud = conn->audience ? conn->audience : conn->token_url;

    char claims[2048];
    snprintf(claims, sizeof(claims),
             "{\"iss\":\"%s\",\"scope\":\"%s\",\"aud\":\"%s\","
             "\"iat\":%lld,\"exp\":%lld}",
             conn->sa_email, conn->scope, aud,
             (long long)now, (long long)(now + 3600));

    static const char header_json[] = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
    char *h64 = b64url((const unsigned char *)header_json,
                       sizeof(header_json) - 1);
    char *c64 = b64url((const unsigned char *)claims, strlen(claims));
    if (!h64 || !c64) { free(h64); free(c64); return -1; }

    char *signing_input = g_strdup_printf("%s.%s", h64, c64);
    char *sig = rs256_sign(conn->sa_private_key, signing_input);
    free(h64); free(c64);
    if (!sig) {
        g_free(signing_input);
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] Failed to sign the JWT assertion "
                 "(invalid private key in the key file?)");
        return -1;
    }
    char *assertion = g_strdup_printf("%s.%s", signing_input, sig);
    g_free(signing_input);
    free(sig);

    CURL *c = curl_easy_init();
    if (!c) { g_free(assertion); return -1; }
    char *e_assert = curl_easy_escape(c, assertion, 0);
    g_free(assertion);
    char *body = g_strdup_printf(
        "grant_type=urn%%3Aietf%%3Aparams%%3Aoauth%%3Agrant-type%%3Ajwt-bearer"
        "&assertion=%s", e_assert ? e_assert : "");
    curl_free(e_assert);

    bq_response_t resp = {0};
    curl_easy_setopt(c, CURLOPT_URL, conn->token_url);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body);
    /* The IAM token endpoint is on the sovereign cloud too, so it needs
     * the same private-CA / mTLS material as the API. */
    bq_apply_tls(conn, c);
    if (conn->connect_timeout_sec > 0)
        curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT,
                         (long)conn->connect_timeout_sec);
    extern size_t argus_bq_write_cb(void *, size_t, size_t, void *);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, argus_bq_write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);

    CURLcode cc = curl_easy_perform(c);
    long code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
    curl_easy_cleanup(c);
    g_free(body);

    if (cc != CURLE_OK || code >= 400 || !resp.data) {
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] Token endpoint %s failed (HTTP %ld): %.512s",
                 conn->token_url, code, resp.data ? resp.data : "no response");
        free(resp.data);
        return -1;
    }

    int rc = -1;
    JsonParser *p = json_parser_new();
    if (json_parser_load_from_data(p, resp.data, -1, NULL)) {
        JsonObject *o = json_node_get_object(json_parser_get_root(p));
        if (o && json_object_has_member(o, "access_token")) {
            free(conn->access_token);
            conn->access_token =
                strdup(json_object_get_string_member(o, "access_token"));
            gint64 ttl = json_object_has_member(o, "expires_in")
                ? json_object_get_int_member(o, "expires_in") : 3600;
            conn->token_expiry = time(NULL) + (time_t)ttl;
            bq_auth_set_headers(conn);
            rc = 0;
        }
    }
    g_object_unref(p);
    free(resp.data);
    if (rc != 0)
        snprintf(conn->last_error, sizeof(conn->last_error),
                 "[Argus][BigQuery] Token endpoint returned no access_token");
    return rc;
}

#endif /* ARGUS_HAS_OPENSSL */

int bq_auth_ensure(bq_conn_t *conn)
{
    if (!conn) return -1;

    /* Static token (or anonymous, e.g. emulator): nothing to refresh. */
    if (!conn->sa_private_key)
        return 0;

#ifdef ARGUS_HAS_OPENSSL
    if (conn->access_token && conn->token_expiry != 0 &&
        time(NULL) < conn->token_expiry - 60)
        return 0;
    return bq_fetch_token(conn);
#else
    snprintf(conn->last_error, sizeof(conn->last_error),
             "[Argus][BigQuery] BQKeyFile authentication requires OpenSSL; "
             "rebuild with OpenSSL or pass AccessToken=");
    return -1;
#endif
}
