#include "trino_internal.h"
#include "argus/log.h"
#include <stdlib.h>
#include <string.h>

/* ── Base64 decode table ─────────────────────────────────────── */

static const unsigned char b64_table[256] = {
    ['A']=0,  ['B']=1,  ['C']=2,  ['D']=3,  ['E']=4,  ['F']=5,
    ['G']=6,  ['H']=7,  ['I']=8,  ['J']=9,  ['K']=10, ['L']=11,
    ['M']=12, ['N']=13, ['O']=14, ['P']=15, ['Q']=16, ['R']=17,
    ['S']=18, ['T']=19, ['U']=20, ['V']=21, ['W']=22, ['X']=23,
    ['Y']=24, ['Z']=25,
    ['a']=26, ['b']=27, ['c']=28, ['d']=29, ['e']=30, ['f']=31,
    ['g']=32, ['h']=33, ['i']=34, ['j']=35, ['k']=36, ['l']=37,
    ['m']=38, ['n']=39, ['o']=40, ['p']=41, ['q']=42, ['r']=43,
    ['s']=44, ['t']=45, ['u']=46, ['v']=47, ['w']=48, ['x']=49,
    ['y']=50, ['z']=51,
    ['0']=52, ['1']=53, ['2']=54, ['3']=55, ['4']=56, ['5']=57,
    ['6']=58, ['7']=59, ['8']=60, ['9']=61,
    ['+']=62, ['/']=63,
};

unsigned char *trino_base64_decode(const char *input, size_t *out_len)
{
    if (!input || !out_len) return NULL;

    size_t in_len = strlen(input);
    if (in_len == 0) {
        *out_len = 0;
        return calloc(1, 1);
    }

    /* Calculate output length (3 bytes per 4 base64 chars, minus padding) */
    size_t alloc_len = (in_len / 4) * 3 + 3;
    unsigned char *out = malloc(alloc_len);
    if (!out) return NULL;

    size_t j = 0;
    unsigned int accum = 0;
    int bits = 0;

    for (size_t i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char)input[i];
        if (c == '=' || c == '\n' || c == '\r' || c == ' ')
            continue;

        accum = (accum << 6) | b64_table[c];
        bits += 6;

        if (bits >= 8) {
            bits -= 8;
            out[j++] = (unsigned char)((accum >> bits) & 0xFF);
        }
    }

    out[j] = '\0';
    *out_len = j;
    return out;
}

/* ── Fetch a spooled segment by URI ──────────────────────────── */

int trino_fetch_segment(trino_conn_t *conn, const char *uri,
                        trino_response_t *resp)
{
    if (!conn || !uri || !resp) return -1;

    ARGUS_LOG_DEBUG("Fetching spooled segment: %s", uri);
    return trino_http_get(conn, uri, resp);
}

/* ── Acknowledge a spooled segment (fire-and-forget) ─────────── */

void trino_ack_segment(trino_conn_t *conn, const char *ack_uri)
{
    if (!conn || !ack_uri) return;

    ARGUS_LOG_DEBUG("Acknowledging spooled segment: %s", ack_uri);
    trino_http_delete(conn, ack_uri);
}

/* ── Helper: append rows from a JSON array node into existing cache ── */

static int append_rows_to_cache(JsonNode *data_node,
                                argus_row_cache_t *cache,
                                int num_cols)
{
    if (!data_node || !cache) return -1;

    JsonArray *rows_arr = json_node_get_array(data_node);
    if (!rows_arr) return -1;

    int nrows = (int)json_array_get_length(rows_arr);
    if (nrows == 0) return 0;

    /* Grow the cache to accommodate new rows */
    size_t old_count = cache->num_rows;
    size_t new_count = old_count + (size_t)nrows;

    argus_row_t *new_rows = realloc(cache->rows,
                                    new_count * sizeof(argus_row_t));
    if (!new_rows) return -1;

    cache->rows = new_rows;
    memset(&cache->rows[old_count], 0, (size_t)nrows * sizeof(argus_row_t));

    /* Use trino_parse_data on a temporary cache, then merge */
    argus_row_cache_t tmp = {0};
    int rc = trino_parse_data(data_node, &tmp, num_cols);
    if (rc != 0) return rc;

    /* Move rows from tmp into main cache */
    for (size_t i = 0; i < tmp.num_rows; i++) {
        cache->rows[old_count + i] = tmp.rows[i];
    }
    cache->num_rows = new_count;
    cache->capacity = new_count;
    cache->num_cols = num_cols;

    /* Free only the rows array, not individual rows (they were moved) */
    free(tmp.rows);

    return 0;
}

/* ── Parse v2 spooled data object ────────────────────────────── */

int trino_parse_spooled_data(trino_conn_t *conn, JsonObject *data_obj,
                             argus_row_cache_t *cache, int num_cols)
{
    if (!conn || !data_obj || !cache) return -1;

    /* Check encoding — only "json" is supported */
    if (json_object_has_member(data_obj, "encoding")) {
        const char *encoding = json_object_get_string_member(data_obj,
                                                              "encoding");
        if (encoding && strcmp(encoding, "json") != 0) {
            ARGUS_LOG_ERROR("Unsupported spooling encoding: %s "
                            "(only 'json' is supported)", encoding);
            return -1;
        }
    }

    /* Get segments array */
    if (!json_object_has_member(data_obj, "segments")) {
        ARGUS_LOG_WARN("v2 data object has no segments");
        cache->num_rows = 0;
        return 0;
    }

    JsonArray *segments = json_object_get_array_member(data_obj, "segments");
    if (!segments) {
        cache->num_rows = 0;
        return 0;
    }

    int num_segments = (int)json_array_get_length(segments);
    ARGUS_LOG_DEBUG("Processing %d v2 spooled segment(s)", num_segments);

    /* Initialize cache for accumulation */
    cache->num_rows = 0;
    cache->rows = NULL;
    cache->num_cols = num_cols;

    for (int i = 0; i < num_segments; i++) {
        JsonObject *seg = json_array_get_object_element(segments, (guint)i);
        if (!seg) continue;

        if (!json_object_has_member(seg, "type")) continue;
        const char *type = json_object_get_string_member(seg, "type");
        if (!type) continue;

        if (strcmp(type, "inline") == 0) {
            /* Inline segment: base64-decode the data field */
            if (!json_object_has_member(seg, "data")) continue;
            const char *b64_data = json_object_get_string_member(seg, "data");
            if (!b64_data) continue;

            size_t decoded_len = 0;
            unsigned char *decoded = trino_base64_decode(b64_data,
                                                         &decoded_len);
            if (!decoded) {
                ARGUS_LOG_ERROR("Failed to base64-decode inline segment %d",
                                i);
                continue;
            }

            /* Parse decoded JSON as array of arrays */
            JsonParser *parser = json_parser_new();
            if (json_parser_load_from_data(parser, (const char *)decoded,
                                           (gssize)decoded_len, NULL)) {
                JsonNode *root = json_parser_get_root(parser);
                if (root && JSON_NODE_HOLDS_ARRAY(root)) {
                    append_rows_to_cache(root, cache, num_cols);
                }
            } else {
                ARGUS_LOG_ERROR("Failed to parse JSON from inline segment %d",
                                i);
            }
            g_object_unref(parser);
            free(decoded);

        } else if (strcmp(type, "spooled") == 0) {
            /* Spooled segment: fetch URI, parse, then acknowledge */
            if (!json_object_has_member(seg, "uri")) continue;
            const char *uri = json_object_get_string_member(seg, "uri");
            if (!uri) continue;

            trino_response_t resp = {0};
            if (trino_fetch_segment(conn, uri, &resp) != 0) {
                ARGUS_LOG_ERROR("Failed to fetch spooled segment %d: %s",
                                i, uri);
                free(resp.data);
                continue;
            }

            if (resp.data) {
                JsonParser *parser = json_parser_new();
                if (json_parser_load_from_data(parser, resp.data,
                                               (gssize)resp.size, NULL)) {
                    JsonNode *root = json_parser_get_root(parser);
                    if (root && JSON_NODE_HOLDS_ARRAY(root)) {
                        append_rows_to_cache(root, cache, num_cols);
                    }
                } else {
                    ARGUS_LOG_ERROR("Failed to parse JSON from spooled "
                                    "segment %d", i);
                }
                g_object_unref(parser);
            }
            free(resp.data);

            /* Acknowledge the segment (best-effort) */
            if (json_object_has_member(seg, "ackUri")) {
                const char *ack_uri = json_object_get_string_member(seg,
                                                                     "ackUri");
                if (ack_uri) {
                    trino_ack_segment(conn, ack_uri);
                }
            }

        } else {
            ARGUS_LOG_WARN("Unknown segment type: %s", type);
        }
    }

    return 0;
}
