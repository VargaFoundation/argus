#!/bin/sh
# Idempotent KDC bootstrap: create the realm + principals on first boot,
# export the Hive service keytab into the shared /keytabs volume, then run
# the KDC in the foreground.
set -e

REALM=EXAMPLE.COM

if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "[kdc] creating realm $REALM"
    kdb5_util create -s -r "$REALM" -P masterpw

    echo "[kdc] adding principals"
    kadmin.local -q "addprinc -randkey hive/hive.example.com@$REALM"
    kadmin.local -q "addprinc -pw testpass testuser@$REALM"

    # HTTP transport authenticates with SPNEGO, which needs an HTTP/ service
    # principal alongside the hive/ one used by the binary SASL path.
    #
    # Two hosts, because the client derives the SPN differently per transport.
    # Binary SASL takes the SPN host from the KrbHostFQDN connection knob, so
    # it can dial 127.0.0.1 and still ask for hive/hive.example.com. SPNEGO has
    # no such override: libcurl builds the SPN from the URL host, so dialing
    # 127.0.0.1 asks for HTTP/127.0.0.1. Registering both lets the HTTP test
    # run either against the FQDN (resolvable from a container on the compose
    # network, or via /etc/hosts) or against the loopback address with no
    # host-level setup at all.
    kadmin.local -q "addprinc -randkey HTTP/hive.example.com@$REALM"
    kadmin.local -q "addprinc -randkey HTTP/127.0.0.1@$REALM"

    echo "[kdc] exporting keytabs"
    kadmin.local -q "ktadd -k /keytabs/hive.keytab hive/hive.example.com@$REALM"
    kadmin.local -q "ktadd -k /keytabs/spnego.keytab HTTP/hive.example.com@$REALM"
    kadmin.local -q "ktadd -k /keytabs/spnego.keytab HTTP/127.0.0.1@$REALM"
    chmod 644 /keytabs/hive.keytab /keytabs/spnego.keytab
    echo "[kdc] bootstrap complete"
fi

exec krb5kdc -n
