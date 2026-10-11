#!/usr/bin/env bash
# Generates the certificates the TLS tests use: a CA, and certificates it
# signs for a server on localhost, a server with another name, and a client;
# another CA, which signs nothing the tests trust; and the CA's revocation
# lists, one empty and one revoking the server's certificate. They're valid
# for a century, so they're generated once and committed.
set -euo pipefail
cd "$(dirname "$0")"

days=36500

key() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1.key"
}

ca() {
  key "$1"
  openssl req -x509 -new -key "$1.key" -sha256 -days "$days" -subj "/CN=$2" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$1.crt"
}

signed() {
  local name=$1 subject=$2 extensions=$3
  key "$name"
  openssl req -new -key "$name.key" -subj "/CN=$subject" -out "$name.csr"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -sha256 -days "$days" \
    -extfile <(printf '%s\n' "basicConstraints=critical,CA:FALSE" "$extensions") \
    -out "$name.crt"
  rm "$name.csr"
}

# A revocation list, revoking the certificates given.
crl() {
  local name=$1 directory
  shift
  directory=$(mktemp -d)
  touch "$directory/index.txt"
  echo 1000 > "$directory/crlnumber"
  printf '%s\n' "[ca]" "default_ca = test" "[test]" "database = $directory/index.txt" \
    "crlnumber = $directory/crlnumber" "default_md = sha256" "default_crl_days = $days" > "$directory/ca.cnf"
  for revoked in "$@"; do
    openssl ca -config "$directory/ca.cnf" -keyfile ca.key -cert ca.crt -revoke "$revoked"
  done
  openssl ca -config "$directory/ca.cnf" -keyfile ca.key -cert ca.crt -gencrl -out "$name.crl"
  rm -r "$directory"
}

ca ca "native-pg test CA"
ca other-ca "native-pg other test CA"
signed server localhost \
  "$(printf '%s\n' "keyUsage=critical,digitalSignature" "extendedKeyUsage=serverAuth" "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1")"
signed server-other-name other.example \
  "$(printf '%s\n' "keyUsage=critical,digitalSignature" "extendedKeyUsage=serverAuth" "subjectAltName=DNS:other.example")"
signed client native_pg_certificate \
  "$(printf '%s\n' "keyUsage=critical,digitalSignature" "extendedKeyUsage=clientAuth")"
crl empty
crl revoked server.crt
rm -f ca.srl other-ca.key
