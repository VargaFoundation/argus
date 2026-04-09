#include "thrift_sasl.h"
#include <string.h>
#include <stdio.h>
#include <glib.h>
#include <arpa/inet.h>  /* htonl / ntohl */

#ifdef ARGUS_HAS_GSSAPI
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
#endif

/*
 * Thrift SASL transport protocol:
 *
 * Each SASL message is:  [1-byte status][4-byte big-endian length][payload]
 *
 * Handshake flow (PLAIN mechanism):
 *   Client -> Server:  START  + "PLAIN"
 *   Client -> Server:  OK     + "\0user\0pass"
 *   Server -> Client:  COMPLETE + "" (or ERROR + message)
 *
 * Handshake flow (GSSAPI mechanism):
 *   Client -> Server:  START  + "GSSAPI"
 *   Client -> Server:  OK     + <gss_init_sec_context token>
 *   Server -> Client:  OK     + <server token>  (repeat until COMPLETE)
 *   Server -> Client:  COMPLETE + ""
 *
 * After handshake, all Thrift frames are:
 *   [4-byte big-endian length][thrift binary data]
 * which matches ThriftFramedTransport.
 */

/* ── helpers ─────────────────────────────────────────────────── */

static int sasl_write_msg(ThriftTransport *t, guint8 status,
                          const guint8 *payload, guint32 len,
                          char *errmsg, size_t errmsg_size)
{
    GError *err = NULL;
    guint32 net_len = htonl(len);

    if (!thrift_transport_write(t, &status, 1, &err)) goto fail;
    if (!thrift_transport_write(t, (guint8 *)&net_len, 4, &err)) goto fail;
    if (len > 0 && !thrift_transport_write(t, (gpointer)payload, len, &err))
        goto fail;
    if (!thrift_transport_flush(t, &err)) goto fail;
    return 0;
fail:
    snprintf(errmsg, errmsg_size, "SASL write failed: %s",
             err ? err->message : "unknown");
    if (err) g_error_free(err);
    return -1;
}

static int sasl_read_msg(ThriftTransport *t, guint8 *status,
                         guint8 **out_buf, guint32 *out_len,
                         char *errmsg, size_t errmsg_size)
{
    GError *err = NULL;
    guint32 net_len;

    if (!thrift_transport_read(t, status, 1, &err)) goto fail;
    if (!thrift_transport_read(t, (guint8 *)&net_len, 4, &err)) goto fail;

    guint32 payload_len = ntohl(net_len);

    guint8 *buf = NULL;
    if (payload_len > 0) {
        buf = g_malloc(payload_len);
        if (!thrift_transport_read(t, buf, payload_len, &err)) {
            g_free(buf);
            goto fail;
        }
    }
    *out_buf = buf;
    *out_len = payload_len;
    return 0;
fail:
    snprintf(errmsg, errmsg_size, "SASL read failed: %s",
             err ? err->message : "unknown");
    if (err) g_error_free(err);
    return -1;
}

/* ── PLAIN mechanism ─────────────────────────────────────────── */

int argus_thrift_sasl_handshake_plain(ThriftTransport *transport,
                                      const char *username,
                                      const char *password,
                                      char *errmsg, size_t errmsg_size)
{
    /* Step 1: send START with mechanism name "PLAIN" */
    if (sasl_write_msg(transport, TSASL_START,
                       (const guint8 *)"PLAIN", 5,
                       errmsg, errmsg_size) != 0)
        return -1;

    /* Step 2: send OK with initial response: \0user\0pass */
    size_t ulen = username ? strlen(username) : 0;
    size_t plen = password ? strlen(password) : 0;
    guint32 resp_len = (guint32)(1 + ulen + 1 + plen);
    guint8 *resp = g_malloc(resp_len);

    resp[0] = '\0';                                   /* authzid (empty) */
    if (ulen > 0) memcpy(resp + 1, username, ulen);
    resp[1 + ulen] = '\0';                            /* separator */
    if (plen > 0) memcpy(resp + 1 + ulen + 1, password, plen);

    int rc = sasl_write_msg(transport, TSASL_OK, resp, resp_len,
                            errmsg, errmsg_size);
    g_free(resp);
    if (rc != 0) return -1;

    /* Step 3: read server response */
    guint8 status;
    guint8 *srv_buf = NULL;
    guint32 srv_len = 0;

    if (sasl_read_msg(transport, &status, &srv_buf, &srv_len,
                      errmsg, errmsg_size) != 0)
        return -1;

    if (status == TSASL_COMPLETE) {
        g_free(srv_buf);
        return 0;
    }

    /* Error case */
    if (srv_buf && srv_len > 0) {
        size_t copy = srv_len < errmsg_size - 60 ? srv_len : errmsg_size - 60;
        char tmp[512];
        memcpy(tmp, srv_buf, copy);
        tmp[copy] = '\0';
        snprintf(errmsg, errmsg_size,
                 "SASL handshake rejected (status=%d): %s",
                 (int)status, tmp);
    } else {
        snprintf(errmsg, errmsg_size,
                 "SASL handshake rejected (status=%d)", (int)status);
    }
    g_free(srv_buf);
    return -1;
}

/* ── GSSAPI mechanism ────────────────────────────────────────── */

#ifdef ARGUS_HAS_GSSAPI

int argus_thrift_sasl_handshake_gssapi(ThriftTransport *transport,
                                       const char *service_name,
                                       const char *hostname,
                                       char *errmsg, size_t errmsg_size)
{
    OM_uint32 major, minor;
    gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
    gss_name_t target_name = GSS_C_NO_NAME;
    gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
    int rc = -1;

    /* Build target principal: service@hostname */
    char principal[512];
    snprintf(principal, sizeof(principal), "%s@%s",
             service_name ? service_name : "impala", hostname);

    gss_buffer_desc name_buf;
    name_buf.value = principal;
    name_buf.length = strlen(principal);

    major = gss_import_name(&minor, &name_buf,
                            GSS_C_NT_HOSTBASED_SERVICE,
                            &target_name);
    if (GSS_ERROR(major)) {
        snprintf(errmsg, errmsg_size,
                 "gss_import_name failed for %s (major=%u, minor=%u)",
                 principal, major, minor);
        return -1;
    }

    /* Step 1: send START with mechanism name "GSSAPI" */
    if (sasl_write_msg(transport, TSASL_START,
                       (const guint8 *)"GSSAPI", 6,
                       errmsg, errmsg_size) != 0) {
        gss_release_name(&minor, &target_name);
        return -1;
    }

    /* GSSAPI token loop */
    bool first = true;
    for (;;) {
        major = gss_init_sec_context(
            &minor,
            GSS_C_NO_CREDENTIAL,   /* use default creds from ticket cache */
            &ctx,
            target_name,
            GSS_C_NO_OID,          /* default mechanism (Kerberos) */
            GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG,
            0,                     /* no time limit */
            GSS_C_NO_CHANNEL_BINDINGS,
            &input_token,
            NULL,                  /* actual mechanism */
            &output_token,
            NULL,                  /* ret_flags */
            NULL);                 /* time_rec */

        /* Free previous input token if it was allocated by us (from sasl_read) */
        if (input_token.value != NULL) {
            g_free(input_token.value);
            input_token.value = NULL;
            input_token.length = 0;
        }

        if (GSS_ERROR(major)) {
            OM_uint32 msg_minor;
            gss_buffer_desc msg_buf;
            OM_uint32 msg_ctx = 0;

            if (gss_display_status(&msg_minor, minor, GSS_C_MECH_CODE,
                                   GSS_C_NO_OID, &msg_ctx,
                                   &msg_buf) == GSS_S_COMPLETE) {
                snprintf(errmsg, errmsg_size,
                         "gss_init_sec_context failed: %.*s",
                         (int)msg_buf.length, (char *)msg_buf.value);
                gss_release_buffer(&msg_minor, &msg_buf);
            } else {
                snprintf(errmsg, errmsg_size,
                         "gss_init_sec_context failed (major=%u, minor=%u)",
                         major, minor);
            }
            goto cleanup;
        }

        /* Send output token to server (always send, even if empty) */
        if (sasl_write_msg(transport, TSASL_OK,
                           (const guint8 *)output_token.value,
                           (guint32)output_token.length,
                           errmsg, errmsg_size) != 0) {
            if (output_token.length > 0)
                gss_release_buffer(&minor, &output_token);
            goto cleanup;
        }
        if (output_token.length > 0)
            gss_release_buffer(&minor, &output_token);
        first = false;

        /* If context is complete, check server response */
        if (major == GSS_S_COMPLETE) {
            /* Read server SASL status */
            guint8 status;
            guint8 *srv_buf = NULL;
            guint32 srv_len = 0;

            if (sasl_read_msg(transport, &status, &srv_buf, &srv_len,
                              errmsg, errmsg_size) != 0)
                goto cleanup;

            if (status == TSASL_COMPLETE) {
                g_free(srv_buf);
                rc = 0;
                goto cleanup;
            }

            /*
             * Server may send a final SASL_OK with a wrapped QoP
             * negotiation message. Unwrap it, set QoP to auth-only,
             * and send back.
             */
            if (status == TSASL_OK && srv_len > 0) {
                /* Unwrap the QoP token */
                gss_buffer_desc wrapped;
                wrapped.value = srv_buf;
                wrapped.length = srv_len;

                gss_buffer_desc unwrapped = GSS_C_EMPTY_BUFFER;
                major = gss_unwrap(&minor, ctx, &wrapped,
                                   &unwrapped, NULL, NULL);
                g_free(srv_buf);

                if (GSS_ERROR(major)) {
                    snprintf(errmsg, errmsg_size,
                             "gss_unwrap QoP failed (major=%u)", major);
                    if (unwrapped.value)
                        gss_release_buffer(&minor, &unwrapped);
                    goto cleanup;
                }

                /*
                 * QoP token is 4 bytes: [bitmask][3 bytes max buf size]
                 * We respond with auth-only (0x01) and our max buf size.
                 */
                guint8 qop_resp[4];
                qop_resp[0] = 0x01;  /* auth-only, no integrity/confidentiality */
                qop_resp[1] = 0x00;
                qop_resp[2] = 0x40;  /* 16384 byte buffer */
                qop_resp[3] = 0x00;

                gss_buffer_desc qop_in;
                qop_in.value = qop_resp;
                qop_in.length = 4;

                gss_buffer_desc qop_wrapped = GSS_C_EMPTY_BUFFER;
                major = gss_wrap(&minor, ctx, 0, 0,
                                 &qop_in, NULL, &qop_wrapped);
                if (unwrapped.value)
                    gss_release_buffer(&minor, &unwrapped);

                if (GSS_ERROR(major)) {
                    snprintf(errmsg, errmsg_size,
                             "gss_wrap QoP failed (major=%u)", major);
                    if (qop_wrapped.value)
                        gss_release_buffer(&minor, &qop_wrapped);
                    goto cleanup;
                }

                if (sasl_write_msg(transport, TSASL_OK,
                                   (const guint8 *)qop_wrapped.value,
                                   (guint32)qop_wrapped.length,
                                   errmsg, errmsg_size) != 0) {
                    gss_release_buffer(&minor, &qop_wrapped);
                    goto cleanup;
                }
                gss_release_buffer(&minor, &qop_wrapped);

                /* Read final COMPLETE */
                guint8 final_status;
                guint8 *final_buf = NULL;
                guint32 final_len = 0;
                if (sasl_read_msg(transport, &final_status,
                                  &final_buf, &final_len,
                                  errmsg, errmsg_size) != 0)
                    goto cleanup;

                g_free(final_buf);
                if (final_status == TSASL_COMPLETE) {
                    rc = 0;
                    goto cleanup;
                }

                snprintf(errmsg, errmsg_size,
                         "SASL GSSAPI final status=%d (expected COMPLETE)",
                         (int)final_status);
                goto cleanup;
            }

            g_free(srv_buf);
            snprintf(errmsg, errmsg_size,
                     "Unexpected SASL status %d after GSS_S_COMPLETE",
                     (int)status);
            goto cleanup;
        }

        /* GSS_S_CONTINUE_NEEDED: read server token */
        if (major & GSS_S_CONTINUE_NEEDED) {
            guint8 status;
            guint8 *srv_buf = NULL;
            guint32 srv_len = 0;

            if (sasl_read_msg(transport, &status, &srv_buf, &srv_len,
                              errmsg, errmsg_size) != 0)
                goto cleanup;

            if (status == TSASL_ERROR || status == TSASL_BAD) {
                if (srv_buf && srv_len > 0) {
                    snprintf(errmsg, errmsg_size,
                             "SASL GSSAPI server error: %.*s",
                             (int)srv_len, (char *)srv_buf);
                } else {
                    snprintf(errmsg, errmsg_size,
                             "SASL GSSAPI server error (status=%d)",
                             (int)status);
                }
                g_free(srv_buf);
                goto cleanup;
            }

            input_token.value = srv_buf;
            input_token.length = srv_len;
            /* srv_buf ownership moves to input_token, freed at loop top */
        }
    }

cleanup:
    if (ctx != GSS_C_NO_CONTEXT)
        gss_delete_sec_context(&minor, &ctx, GSS_C_NO_BUFFER);
    if (target_name != GSS_C_NO_NAME)
        gss_release_name(&minor, &target_name);
    return rc;
}

#else /* !ARGUS_HAS_GSSAPI */

int argus_thrift_sasl_handshake_gssapi(ThriftTransport *transport,
                                       const char *service_name,
                                       const char *hostname,
                                       char *errmsg, size_t errmsg_size)
{
    (void)transport;
    (void)service_name;
    (void)hostname;
    snprintf(errmsg, errmsg_size,
             "GSSAPI (Kerberos) support not available: "
             "driver was built without libgssapi");
    return -1;
}

#endif /* ARGUS_HAS_GSSAPI */
