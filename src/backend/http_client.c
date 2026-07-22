/*
 * http_client.c - Minimal shared HTTPS JSON POST helper.
 *
 * See http_client.h. Modelled on trino_http_post()/trino_apply_curl_settings()
 * but self-contained: its own easy handle, no auth, no response capture.
 */

#include "http_client.h"

#include <curl/curl.h>
#include <stddef.h>

/* Discard any response body without buffering it. */
static size_t http_discard_cb(void *contents, size_t size, size_t nmemb,
                              void *userp)
{
    (void)contents;
    (void)userp;
    return size * nmemb;
}

int argus_http_post_json(const char *url, const char *body, long timeout_sec)
{
    if (!url || !*url || !body)
        return -1;

    CURL *curl = curl_easy_init();
    if (!curl)
        return -1;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_discard_cb);

    /* TLS verification is always on; use the system trust store. */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    if (timeout_sec > 0) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_sec);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, timeout_sec);
    }
    /* Never let a stuck sender hang process teardown. */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    CURLcode res = curl_easy_perform(curl);
    int rc = -1;
    if (res == CURLE_OK) {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (http_code >= 200 && http_code < 300)
            rc = 0;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return rc;
}
