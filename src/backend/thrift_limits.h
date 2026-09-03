#ifndef ARGUS_THRIFT_LIMITS_H
#define ARGUS_THRIFT_LIMITS_H

#include <thrift/c_glib/thrift_configuration.h>
#include <thrift/c_glib/transport/thrift_transport.h>

/*
 * Size limits applied to every libthrift transport the driver builds.
 *
 * libthrift enforces them itself (an oversized frame or message fails the
 * read with THRIFT_TRANSPORT_ERROR_MAX_MESSAGE_SIZE_REACHED instead of
 * allocating whatever the peer announced), but only through a
 * ThriftConfiguration attached at construction — and its built-in defaults
 * are not ours to rely on. The frame cap is 4x libthrift's default (16 MB)
 * so a 1000-row FetchResults batch can carry 64 KiB per row; the message
 * cap matches the Thrift Java client, which is what HiveServer2 and Impala
 * are tuned against.
 */
#define ARGUS_THRIFT_MAX_FRAME_SIZE   (64 * 1024 * 1024)
#define ARGUS_THRIFT_MAX_MESSAGE_SIZE (100 * 1024 * 1024)
#define ARGUS_THRIFT_RECURSION_LIMIT  64

/* A new configuration carrying the limits above. The caller owns the ref;
 * transports take their own when it is passed as their "configuration"
 * construct property. */
ThriftConfiguration *argus_thrift_configuration_new(void);

/* Framed / buffered transports over `inner`, with the limits attached.
 * Ownership of `inner` is unchanged (the wrapper takes its own ref). */
ThriftTransport *argus_thrift_framed_transport_new(ThriftTransport *inner);
ThriftTransport *argus_thrift_buffered_transport_new(ThriftTransport *inner);

#endif /* ARGUS_THRIFT_LIMITS_H */
