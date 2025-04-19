#!/bin/bash

set -e

CERT_DIR="./certs"
DAYS_VALID=825

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "Generating CA key and certificate..."
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt \
  -subj "/C=IN/ST=KA/L=Bangalore/O=UnisonDB Dev/CN=UnisonCA"

echo "Generating server key and CSR..."
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/C=IN/ST=KA/L=Bangalore/O=UnisonDB Dev/CN=localhost"

cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
EOF

echo "Signing server certificate..."
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days $DAYS_VALID -sha256 -extfile server.ext

echo "Generating client key and CSR..."
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/C=IN/ST=KA/L=Bangalore/O=UnisonDB Dev/CN=UnisonClient"

cat > client.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

echo "Signing client certificate..."
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days $DAYS_VALID -sha256 -extfile client.ext

rm -f *.csr *.ext

echo "All certificates generated in: $CERT_DIR"
