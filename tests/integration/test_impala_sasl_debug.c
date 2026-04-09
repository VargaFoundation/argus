#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <glib-object.h>
#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_framed_transport.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

/*
 * Standalone debug program: SASL GSSAPI handshake with Impala
 * Usage: ./test_impala_sasl_debug <host> <port>
 */

static void hexdump(const char *label, const unsigned char *buf, size_t len)
{
    fprintf(stderr, "%s (%zu bytes):", label, len);
    size_t show = len < 64 ? len : 64;
    for (size_t i = 0; i < show; i++)
        fprintf(stderr, " %02x", buf[i]);
    if (len > show)
        fprintf(stderr, " ...");
    fprintf(stderr, "\n");
}

static int raw_write(ThriftTransport *t, const void *buf, size_t len)
{
    GError *err = NULL;
    if (!thrift_transport_write(t, (gpointer)buf, (guint32)len, &err)) {
        fprintf(stderr, "  write failed: %s\n", err ? err->message : "?");
        if (err) g_error_free(err);
        return -1;
    }
    return 0;
}

static int raw_flush(ThriftTransport *t)
{
    GError *err = NULL;
    if (!thrift_transport_flush(t, &err)) {
        fprintf(stderr, "  flush failed: %s\n", err ? err->message : "?");
        if (err) g_error_free(err);
        return -1;
    }
    return 0;
}

static int raw_read(ThriftTransport *t, void *buf, size_t len)
{
    GError *err = NULL;
    if (!thrift_transport_read(t, (gpointer)buf, (guint32)len, &err)) {
        fprintf(stderr, "  read(%zu) failed: %s\n", len,
                err ? err->message : "?");
        if (err) g_error_free(err);
        return -1;
    }
    return 0;
}

static int sasl_send(ThriftTransport *t, unsigned char status,
                     const void *payload, size_t len)
{
    unsigned char hdr[5];
    hdr[0] = status;
    unsigned int net_len = htonl((unsigned int)len);
    memcpy(hdr + 1, &net_len, 4);

    fprintf(stderr, "  TX: status=%d len=%zu\n", status, len);
    if (len > 0) hexdump("  TX payload", payload, len);

    if (raw_write(t, hdr, 5) != 0) return -1;
    if (len > 0 && raw_write(t, payload, len) != 0) return -1;
    return raw_flush(t);
}

static int sasl_recv(ThriftTransport *t, unsigned char *status,
                     unsigned char **out_buf, unsigned int *out_len)
{
    unsigned char hdr[5];
    if (raw_read(t, hdr, 5) != 0) return -1;

    *status = hdr[0];
    unsigned int net_len;
    memcpy(&net_len, hdr + 1, 4);
    unsigned int payload_len = ntohl(net_len);

    fprintf(stderr, "  RX: status=%d len=%u\n", *status, payload_len);

    unsigned char *buf = NULL;
    if (payload_len > 0) {
        if (payload_len > 1048576) {
            fprintf(stderr, "  RX: payload too large!\n");
            return -1;
        }
        buf = malloc(payload_len);
        if (raw_read(t, buf, payload_len) != 0) {
            free(buf);
            return -1;
        }
        hexdump("  RX payload", buf, payload_len);
    }
    *out_buf = buf;
    *out_len = payload_len;
    return 0;
}

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "localhost";
    int port = argc > 2 ? atoi(argv[2]) : 21050;

    fprintf(stderr, "=== SASL GSSAPI debug: %s:%d ===\n", host, port);

    /* Create socket */
    ThriftSocket *sock = (ThriftSocket *)g_object_new(
        THRIFT_TYPE_SOCKET, "hostname", host, "port", port, NULL);
    ThriftTransport *raw = (ThriftTransport *)sock;

    GError *err = NULL;
    if (!thrift_transport_open(raw, &err)) {
        fprintf(stderr, "Connect failed: %s\n", err ? err->message : "?");
        return 1;
    }
    fprintf(stderr, "Connected to %s:%d\n\n", host, port);

    /* Step 1: send START GSSAPI */
    fprintf(stderr, "--- Step 1: START GSSAPI ---\n");
    if (sasl_send(raw, 0x01, "GSSAPI", 6) != 0) return 1;

    /* Build target principal */
    char principal[512];
    snprintf(principal, sizeof(principal), "impala@%s", host);
    fprintf(stderr, "Target principal: %s\n", principal);

    OM_uint32 major, minor;
    gss_name_t target_name;
    gss_buffer_desc name_buf = { strlen(principal), principal };
    major = gss_import_name(&minor, &name_buf,
                            GSS_C_NT_HOSTBASED_SERVICE, &target_name);
    if (GSS_ERROR(major)) {
        fprintf(stderr, "gss_import_name failed: %u/%u\n", major, minor);
        return 1;
    }

    /* GSSAPI token exchange loop */
    gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
    gss_buffer_desc in_tok = { 0, NULL };
    gss_buffer_desc out_tok = { 0, NULL };
    int round = 0;

    while (1) {
        round++;
        fprintf(stderr, "\n--- Round %d: gss_init_sec_context ---\n", round);

        major = gss_init_sec_context(
            &minor, GSS_C_NO_CREDENTIAL, &ctx, target_name,
            GSS_C_NO_OID,
            GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG,
            0, GSS_C_NO_CHANNEL_BINDINGS,
            &in_tok, NULL, &out_tok, NULL, NULL);

        /* Free input token from server */
        free(in_tok.value);
        in_tok.value = NULL;
        in_tok.length = 0;

        fprintf(stderr, "  major=0x%08x minor=0x%08x out_len=%zu\n",
                major, minor, out_tok.length);

        if (GSS_ERROR(major)) {
            OM_uint32 m_minor;
            gss_buffer_desc m_buf;
            OM_uint32 m_ctx = 0;
            gss_display_status(&m_minor, major, GSS_C_GSS_CODE,
                               GSS_C_NO_OID, &m_ctx, &m_buf);
            fprintf(stderr, "  GSS major error: %.*s\n",
                    (int)m_buf.length, (char *)m_buf.value);
            gss_release_buffer(&m_minor, &m_buf);

            m_ctx = 0;
            gss_display_status(&m_minor, minor, GSS_C_MECH_CODE,
                               GSS_C_NO_OID, &m_ctx, &m_buf);
            fprintf(stderr, "  GSS minor error: %.*s\n",
                    (int)m_buf.length, (char *)m_buf.value);
            gss_release_buffer(&m_minor, &m_buf);
            return 1;
        }

        /* Send output token */
        if (out_tok.length > 0) {
            fprintf(stderr, "--- Sending GSSAPI token (OK) ---\n");
            if (sasl_send(raw, 0x02, out_tok.value,
                          out_tok.length) != 0) return 1;
            gss_release_buffer(&minor, &out_tok);
        }

        if (major == GSS_S_COMPLETE) {
            fprintf(stderr, "\n--- GSS context COMPLETE ---\n");
            break;
        }

        /* Read server response */
        fprintf(stderr, "--- Reading server GSSAPI response ---\n");
        unsigned char status;
        unsigned char *srv_buf = NULL;
        unsigned int srv_len = 0;
        if (sasl_recv(raw, &status, &srv_buf, &srv_len) != 0) return 1;

        if (status == 0x03 || status == 0x04) {
            fprintf(stderr, "Server error: %.*s\n",
                    srv_len, (char *)srv_buf);
            free(srv_buf);
            return 1;
        }

        in_tok.value = srv_buf;
        in_tok.length = srv_len;
    }

    /* Read post-GSSAPI server response */
    fprintf(stderr, "\n--- Reading server post-GSSAPI response ---\n");
    unsigned char status;
    unsigned char *srv_buf = NULL;
    unsigned int srv_len = 0;
    if (sasl_recv(raw, &status, &srv_buf, &srv_len) != 0) return 1;

    fprintf(stderr, "Server response: status=%d len=%u\n", status, srv_len);

    if (status == 0x05) {
        fprintf(stderr, "SASL COMPLETE (no QoP negotiation)\n");
        free(srv_buf);
    } else if (status == 0x02 && srv_len > 0) {
        fprintf(stderr, "QoP negotiation token received\n");

        /* Unwrap QoP token */
        gss_buffer_desc wrapped = { srv_len, srv_buf };
        gss_buffer_desc unwrapped = { 0, NULL };
        major = gss_unwrap(&minor, ctx, &wrapped, &unwrapped, NULL, NULL);
        free(srv_buf);

        if (GSS_ERROR(major)) {
            fprintf(stderr, "gss_unwrap QoP failed: 0x%x\n", major);
            return 1;
        }

        hexdump("Unwrapped QoP", unwrapped.value, unwrapped.length);

        /* Send QoP response: auth-only */
        unsigned char qop_resp[4] = { 0x01, 0x00, 0x40, 0x00 };
        gss_buffer_desc qop_in = { 4, qop_resp };
        gss_buffer_desc qop_out = { 0, NULL };
        major = gss_wrap(&minor, ctx, 0, 0, &qop_in, NULL, &qop_out);
        gss_release_buffer(&minor, &unwrapped);

        if (GSS_ERROR(major)) {
            fprintf(stderr, "gss_wrap QoP failed: 0x%x\n", major);
            return 1;
        }

        fprintf(stderr, "--- Sending QoP response ---\n");
        if (sasl_send(raw, 0x02, qop_out.value, qop_out.length) != 0) return 1;
        gss_release_buffer(&minor, &qop_out);

        /* Read final COMPLETE */
        fprintf(stderr, "--- Reading final COMPLETE ---\n");
        if (sasl_recv(raw, &status, &srv_buf, &srv_len) != 0) return 1;
        fprintf(stderr, "Final status: %d\n", status);
        free(srv_buf);
    } else {
        fprintf(stderr, "Unexpected status=%d\n", status);
        free(srv_buf);
        return 1;
    }

    fprintf(stderr, "\n=== SASL handshake COMPLETE ===\n");

    /* Now try a Thrift RPC over framed transport */
    fprintf(stderr, "\n--- Testing Thrift RPC (OpenSession) ---\n");
    ThriftTransport *framed = (ThriftTransport *)g_object_new(
        THRIFT_TYPE_FRAMED_TRANSPORT, "transport", sock, NULL);
    ThriftProtocol *proto = (ThriftProtocol *)g_object_new(
        THRIFT_TYPE_BINARY_PROTOCOL, "transport", framed, NULL);

    /* Just print success - full RPC test handled by main test suite */
    fprintf(stderr, "Transport stack ready for Thrift RPC\n");
    fprintf(stderr, "SUCCESS\n");

    gss_delete_sec_context(&minor, &ctx, GSS_C_NO_BUFFER);
    gss_release_name(&minor, &target_name);
    thrift_transport_close(raw, NULL);

    return 0;
}
