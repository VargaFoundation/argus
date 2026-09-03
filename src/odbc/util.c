#include "argus/types.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* ── Connection string parsing ────────────────────────────────── */

void argus_conn_params_init(argus_conn_params_t *params)
{
    params->params   = NULL;
    params->count    = 0;
    params->capacity = 0;
}

void argus_conn_params_free(argus_conn_params_t *params)
{
    for (int i = 0; i < params->count; i++) {
        free(params->params[i].key);
        free(params->params[i].value);
    }
    free(params->params);
    params->params   = NULL;
    params->count    = 0;
    params->capacity = 0;
}

static int conn_params_add(argus_conn_params_t *params,
                           const char *key, size_t key_len,
                           const char *value, size_t value_len)
{
    if (params->count >= params->capacity) {
        int new_cap = params->capacity ? params->capacity * 2 : 8;
        argus_conn_param_t *new_params = realloc(
            params->params, (size_t)new_cap * sizeof(argus_conn_param_t));
        if (!new_params) return -1;
        params->params   = new_params;
        params->capacity = new_cap;
    }

    char *k = malloc(key_len + 1);
    char *v = malloc(value_len + 1);
    if (!k || !v) { free(k); free(v); return -1; }

    memcpy(k, key, key_len);
    k[key_len] = '\0';
    memcpy(v, value, value_len);
    v[value_len] = '\0';

    /* Normalize key to uppercase */
    for (size_t i = 0; i < key_len; i++)
        k[i] = (char)toupper((unsigned char)k[i]);

    params->params[params->count].key   = k;
    params->params[params->count].value = v;
    params->count++;
    return 0;
}

/*
 * Parse ODBC connection string of the form:
 *   KEY1=VALUE1;KEY2=VALUE2;...
 * Values may be enclosed in braces: KEY={value with;semicolons}
 */
int argus_conn_params_parse(argus_conn_params_t *params, const char *conn_str)
{
    if (!conn_str || !params) return -1;

    const char *p = conn_str;
    while (*p) {
        /* Skip leading whitespace and semicolons */
        while (*p == ';' || *p == ' ' || *p == '\t') p++;
        if (!*p) break;

        /* Find '=' */
        const char *key_start = p;
        while (*p && *p != '=') p++;
        if (!*p) break;

        size_t key_len = (size_t)(p - key_start);
        /* Trim trailing whitespace from key */
        while (key_len > 0 && (key_start[key_len - 1] == ' ' ||
                                key_start[key_len - 1] == '\t'))
            key_len--;

        p++; /* skip '=' */

        /* Skip whitespace before value */
        while (*p == ' ' || *p == '\t') p++;

        const char *val_start;
        size_t val_len;

        if (*p == '{') {
            /* Brace-enclosed value */
            p++; /* skip '{' */
            val_start = p;
            while (*p && *p != '}') p++;
            val_len = (size_t)(p - val_start);
            if (*p == '}') p++;
        } else {
            val_start = p;
            while (*p && *p != ';') p++;
            val_len = (size_t)(p - val_start);
            /* Trim trailing whitespace from value */
            while (val_len > 0 && (val_start[val_len - 1] == ' ' ||
                                    val_start[val_len - 1] == '\t'))
                val_len--;
        }

        if (key_len > 0) {
            if (conn_params_add(params, key_start, key_len,
                                val_start, val_len) != 0)
                return -1;
        }
    }
    return 0;
}

/* ── Secret-bearing keys ──────────────────────────────────────── */

/*
 * A key names a credential when its uppercased form is one of these or ends
 * with one of the suffixes below. Endpoints are deliberately not on the list:
 * TOKENURL, OAUTH2TOKENENDPOINT or BQTOKENENDPOINT name where a token comes
 * from, and an application that persists OutConnectionString needs them back
 * to reconnect. Keys the open driver does not parse (LicenseToken, AuditKey,
 * OtlpAuthHeader) are listed because they travel in the same string.
 */
static const char *const secret_keys[] = {
    "PWD", "PASSWORD", "TOKEN", "SECRET", "PASSPHRASE",
    "ACCESSTOKEN", "BQACCESSTOKEN", "REFRESHTOKEN", "IDTOKEN", "BEARERTOKEN",
    "CLIENTSECRET", "OAUTH2CLIENTSECRET",
    "APIKEY", "API_KEY", "PRIVATEKEY", "SSLKEYPASSWORD",
    "CREDENTIAL", "CREDENTIALS",
    "LICENSETOKEN", "AUDITKEY", "OTLPAUTHHEADER",
    NULL
};

static const char *const secret_suffixes[] = {
    "PWD", "PASSWORD", "PASSPHRASE", "SECRET", "TOKEN", "APIKEY", "AUTHHEADER",
    NULL
};

bool argus_connstr_key_is_secret(const char *key)
{
    if (!key) return false;
    size_t klen = strlen(key);
    char ukey[128];
    if (klen == 0 || klen >= sizeof(ukey)) return false;
    for (size_t i = 0; i < klen; i++)
        ukey[i] = (char)toupper((unsigned char)key[i]);
    ukey[klen] = '\0';

    for (int i = 0; secret_keys[i]; i++)
        if (strcmp(ukey, secret_keys[i]) == 0) return true;
    for (int i = 0; secret_suffixes[i]; i++) {
        size_t slen = strlen(secret_suffixes[i]);
        if (klen > slen && strcmp(ukey + klen - slen, secret_suffixes[i]) == 0)
            return true;
    }
    return false;
}

char *argus_connstr_redact(const char *conn_str)
{
    if (!conn_str) return NULL;

    size_t cap = strlen(conn_str) + 16;
    char *out = malloc(cap);
    if (!out) return NULL;
    size_t olen = 0;

    const char *p = conn_str;
    while (*p) {
        while (*p == ';' || *p == ' ' || *p == '\t') p++;
        if (!*p) break;

        const char *key_start = p;
        while (*p && *p != '=' && *p != ';') p++;
        if (*p != '=') {
            /* A fragment without '=': carries no value, kept as is. */
            size_t n = (size_t)(p - key_start);
            if (olen + n + 2 > cap) {
                char *grown = realloc(out, olen + n + 2);
                if (!grown) { free(out); return NULL; }
                out = grown; cap = olen + n + 2;
            }
            if (olen) out[olen++] = ';';
            memcpy(out + olen, key_start, n);
            olen += n;
            continue;
        }

        size_t key_len = (size_t)(p - key_start);
        while (key_len > 0 && (key_start[key_len - 1] == ' ' ||
                                key_start[key_len - 1] == '\t'))
            key_len--;
        p++;
        while (*p == ' ' || *p == '\t') p++;

        const char *val_start = p;
        size_t val_len;
        if (*p == '{') {
            p++;
            while (*p && *p != '}') p++;
            if (*p == '}') p++;
            val_len = (size_t)(p - val_start);
        } else {
            while (*p && *p != ';') p++;
            val_len = (size_t)(p - val_start);
            while (val_len > 0 && (val_start[val_len - 1] == ' ' ||
                                    val_start[val_len - 1] == '\t'))
                val_len--;
        }

        char ukey[128];
        size_t ulen = key_len < sizeof(ukey) - 1 ? key_len : sizeof(ukey) - 1;
        memcpy(ukey, key_start, ulen);
        ukey[ulen] = '\0';
        bool secret = (key_len < sizeof(ukey)) &&
                      argus_connstr_key_is_secret(ukey);

        const char *val = secret ? "***" : val_start;
        size_t vlen = secret ? 3 : val_len;
        size_t need = olen + 1 + key_len + 1 + vlen + 1;
        if (need > cap) {
            char *grown = realloc(out, need);
            if (!grown) { free(out); return NULL; }
            out = grown; cap = need;
        }
        if (olen) out[olen++] = ';';
        memcpy(out + olen, key_start, key_len);
        olen += key_len;
        out[olen++] = '=';
        memcpy(out + olen, val, vlen);
        olen += vlen;
    }
    out[olen] = '\0';
    return out;
}

/* ── SQL text helpers ────────────────────────────────────────── */

static char *quote_text(const char *value, size_t len, char quote,
                        bool double_backslash)
{
    if (!value) return NULL;
    size_t extra = 0;
    for (size_t i = 0; i < len; i++) {
        if (value[i] == '\0') return NULL;
        if (value[i] == quote || (double_backslash && value[i] == '\\'))
            extra++;
    }
    char *out = malloc(len + extra + 3);
    if (!out) return NULL;
    char *dst = out;
    *dst++ = quote;
    for (size_t i = 0; i < len; i++) {
        if (value[i] == quote || (double_backslash && value[i] == '\\'))
            *dst++ = value[i];
        *dst++ = value[i];
    }
    *dst++ = quote;
    *dst = '\0';
    return out;
}

char *argus_sql_quote_literal_n(const char *value, size_t len,
                                bool backslash_escapes)
{
    return quote_text(value, len, '\'', backslash_escapes);
}

char *argus_sql_quote_literal(const char *value, bool backslash_escapes)
{
    return value ? quote_text(value, strlen(value), '\'', backslash_escapes)
                 : NULL;
}

char *argus_sql_quote_ident(const char *name, char quote)
{
    return name ? quote_text(name, strlen(name), quote, false) : NULL;
}

const char *argus_conn_params_get(const argus_conn_params_t *params,
                                  const char *key)
{
    if (!params || !key) return NULL;

    /* Build uppercase version of search key */
    size_t klen = strlen(key);
    char ukey[256];
    if (klen >= sizeof(ukey)) return NULL;
    for (size_t i = 0; i < klen; i++)
        ukey[i] = (char)toupper((unsigned char)key[i]);
    ukey[klen] = '\0';

    for (int i = 0; i < params->count; i++) {
        if (strcmp(params->params[i].key, ukey) == 0)
            return params->params[i].value;
    }
    return NULL;
}

/* ── String helpers ───────────────────────────────────────────── */

/*
 * Safe copy of a string into a fixed SQLCHAR buffer.
 * Returns the full source length (not truncated).
 */
SQLSMALLINT argus_copy_string(const char *src,
                               SQLCHAR *dst, SQLSMALLINT dst_len)
{
    if (!src) {
        if (dst && dst_len > 0) dst[0] = '\0';
        return 0;
    }
    size_t src_len = strlen(src);
    if (dst && dst_len > 0) {
        size_t copy_len = src_len < (size_t)(dst_len - 1)
                          ? src_len : (size_t)(dst_len - 1);
        memcpy(dst, src, copy_len);
        dst[copy_len] = '\0';
    }
    if (src_len > 32767)
        return 32767; /* SQLSMALLINT_MAX — avoid signed overflow */
    return (SQLSMALLINT)src_len;
}

/*
 * Duplicate a string, handling SQL_NTS and explicit lengths.
 * Caller must free the result. See types.h for the NULL cases.
 */
char *argus_str_dup(const SQLCHAR *str, SQLINTEGER len)
{
    if (!str || !argus_odbc_len_valid(len)) return NULL;

    size_t actual_len;
    if (len == SQL_NTS)
        actual_len = strlen((const char *)str);
    else
        actual_len = (size_t)len;

    char *dup = malloc(actual_len + 1);
    if (!dup) return NULL;
    memcpy(dup, str, actual_len);
    dup[actual_len] = '\0';
    return dup;
}

/* Same but for SQLSMALLINT lengths */
char *argus_str_dup_short(const SQLCHAR *str, SQLSMALLINT len)
{
    if (!str || !argus_odbc_len_valid(len)) return NULL;

    size_t actual_len;
    if (len == SQL_NTS)
        actual_len = strlen((const char *)str);
    else
        actual_len = (size_t)len;

    char *dup = malloc(actual_len + 1);
    if (!dup) return NULL;
    memcpy(dup, str, actual_len);
    dup[actual_len] = '\0';
    return dup;
}
