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
 * Perform a SASL handshake using GSSAPI (Kerberos) via the system GSSAPI
 * library (POSIX) or SSPI (Windows).
 *
 * The caller must have a valid Kerberos TGT (via kinit/keytab on POSIX, or the
 * logged-in domain on Windows). The target SPN is `service_name/hostname`; if
 * `realm` is non-NULL/non-empty the full principal `service/host@REALM` is used
 * (cross-realm / explicit realm), otherwise the realm is resolved from the
 * system Kerberos config. `hostname` should be the SPN host (which may differ
 * from the TCP connect host — e.g. behind a load balancer).
 *
 * Returns 0 on success, -1 on failure (errmsg filled in).
 */
int argus_thrift_sasl_handshake_gssapi(ThriftTransport *transport,
                                       const char *service_name,
                                       const char *hostname,
                                       const char *realm,
                                       char *errmsg, size_t errmsg_size);

#endif /* ARGUS_THRIFT_SASL_H */
