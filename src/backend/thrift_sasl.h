#ifndef ARGUS_THRIFT_SASL_H
#define ARGUS_THRIFT_SASL_H

#include <thrift/c_glib/transport/thrift_transport.h>
#include <stdbool.h>

/*
 * Thrift SASL status bytes (TSaslNegotiationStatus).
 */
#define TSASL_START    0x01
#define TSASL_OK       0x02
#define TSASL_BAD      0x03
#define TSASL_ERROR    0x04
#define TSASL_COMPLETE 0x05

/*
 * Perform a SASL handshake over the given transport using the PLAIN mechanism.
 *
 * The PLAIN initial response is: \0<username>\0<password>
 *
 * After a successful handshake, the transport is ready for framed I/O.
 * Returns 0 on success, -1 on failure (errmsg filled in).
 */
int argus_thrift_sasl_handshake_plain(ThriftTransport *transport,
                                      const char *username,
                                      const char *password,
                                      char *errmsg, size_t errmsg_size);

/*
 * Perform a SASL handshake using GSSAPI (Kerberos) via the system GSSAPI library.
 *
 * The caller must have a valid Kerberos TGT (via kinit or keytab).
 * The service_principal is typically "impala/<hostname>" or "hive/<hostname>".
 *
 * Returns 0 on success, -1 on failure (errmsg filled in).
 */
int argus_thrift_sasl_handshake_gssapi(ThriftTransport *transport,
                                       const char *service_name,
                                       const char *hostname,
                                       char *errmsg, size_t errmsg_size);

#endif /* ARGUS_THRIFT_SASL_H */
