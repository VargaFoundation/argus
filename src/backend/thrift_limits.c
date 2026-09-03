#include "thrift_limits.h"
#include <thrift/c_glib/transport/thrift_framed_transport.h>
#include <thrift/c_glib/transport/thrift_buffered_transport.h>

ThriftConfiguration *argus_thrift_configuration_new(void)
{
    return g_object_new(THRIFT_TYPE_CONFIGURATION,
                        "max-frame-size", (gint)ARGUS_THRIFT_MAX_FRAME_SIZE,
                        "max-message-size", (gint)ARGUS_THRIFT_MAX_MESSAGE_SIZE,
                        "recursion-limit", (gint)ARGUS_THRIFT_RECURSION_LIMIT,
                        NULL);
}

static ThriftTransport *wrap(GType type, ThriftTransport *inner)
{
    ThriftConfiguration *limits = argus_thrift_configuration_new();
    ThriftTransport *t = g_object_new(type,
                                      "transport", inner,
                                      "configuration", limits,
                                      NULL);
    g_object_unref(limits);
    return t;
}

ThriftTransport *argus_thrift_framed_transport_new(ThriftTransport *inner)
{
    return wrap(THRIFT_TYPE_FRAMED_TRANSPORT, inner);
}

ThriftTransport *argus_thrift_buffered_transport_new(ThriftTransport *inner)
{
    return wrap(THRIFT_TYPE_BUFFERED_TRANSPORT, inner);
}
