#!/bin/bash

# this wrapper gets installed as /usr/bin/redpanda-connect-fips
# and overrides several environment variables to work with rpk-fips

export PATH="/opt/redpanda/bin:${PATH}"
export GOFIPS="1"
export LD_LIBRARY_PATH="/opt/redpanda/rpk-fips/lib"
export OPENSSL_CONF="/opt/redpanda/rpk-fips/openssl/openssl-rpk.cnf"
export OPENSSL_MODULES="/opt/redpanda/rpk-fips/lib/ossl-modules/"

exec -a "$0" "/opt/redpanda/libexec/redpanda-connect-fips" "$@"
