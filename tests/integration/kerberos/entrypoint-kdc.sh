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

    echo "[kdc] exporting hive keytab"
    kadmin.local -q "ktadd -k /keytabs/hive.keytab hive/hive.example.com@$REALM"
    chmod 644 /keytabs/hive.keytab
    echo "[kdc] bootstrap complete"
fi

exec krb5kdc -n
