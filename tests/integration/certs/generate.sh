#!/bin/bash
# Generate self-signed certificates for integration TLS tests.
# Usage: cd tests/integration/certs && ./generate.sh

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

# CA key + cert
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 \
    -nodes -keyout "$DIR/ca-key.pem" -out "$DIR/ca-cert.pem" \
    -subj "/CN=Argus Test CA"

# Server key + CSR
openssl req -newkey rsa:2048 -nodes \
    -keyout "$DIR/server-key.pem" -out "$DIR/server.csr" \
    -subj "/CN=localhost"

# Sign server cert with CA (include SAN for localhost + container names)
openssl x509 -req -in "$DIR/server.csr" \
    -CA "$DIR/ca-cert.pem" -CAkey "$DIR/ca-key.pem" -CAcreateserial \
    -out "$DIR/server-cert.pem" -days 3650 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:argus-trino-ssl,IP:127.0.0.1")

# Create Java keystore for Trino (PKCS12)
openssl pkcs12 -export -in "$DIR/server-cert.pem" -inkey "$DIR/server-key.pem" \
    -certfile "$DIR/ca-cert.pem" -out "$DIR/server.p12" \
    -name trino -passout pass:changeit

# Create Java truststore with CA cert
keytool -importcert -noprompt -alias argus-ca \
    -file "$DIR/ca-cert.pem" -keystore "$DIR/truststore.jks" \
    -storepass changeit 2>/dev/null || true

rm -f "$DIR/server.csr" "$DIR/ca-cert.srl"

echo "Certificates generated in $DIR"
