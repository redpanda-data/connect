#!/usr/bin/env bash
# Generate test-only TLS material for as-ee-sec. Gitignored; not for production.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS="$DIR/certs"
mkdir -p "$CERTS"

cd "$CERTS"
if [[ ! -f ca.pem || ! -f ca.key || "${FORCE:-}" == "1" ]]; then
  rm -f ca.pem ca.key ca.srl server.pem server.key client.pem client.key
  openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
    -subj "/CN=Aerospike EE test CA" \
    -keyout ca.key -out ca.pem
fi

sign_cert() {
  local cn="$1" pem="$2" key="$3" ext="$4"
  if [[ -f "$pem" && -f "$key" && "${FORCE:-}" != "1" ]]; then
    return
  fi
  local csr="${pem%.pem}.csr" extfile="${pem%.pem}.ext"
  openssl req -newkey rsa:2048 -nodes \
    -subj "/CN=${cn}" \
    -keyout "$key" -out "$csr"
  printf '%s\n' "$ext" > "$extfile"
  openssl x509 -req -in "$csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out "$pem" -days 3650 -extfile "$extfile"
  rm -f "$csr" "$extfile"
  echo "wrote $CERTS/$pem"
}

sign_cert clusterA server.pem server.key "subjectAltName=DNS:localhost,DNS:as-ee-sec,DNS:clusterA,IP:127.0.0.1"
# CN must be the Aerospike username used for PKI login. This tools image
# cannot create a PKI-only user, and show users lists rpcn as password,PKI.
sign_cert rpcn client.pem client.key "extendedKeyUsage=clientAuth"
