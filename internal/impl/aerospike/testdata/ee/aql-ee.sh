#!/bin/sh
# AQL from as-ee-tools onto the RF=2 cluster. --services-alternate is required
# because access-address is 127.0.0.1 on the host, not the Compose network.
exec aql --no-config-file --host as-ee-1 --port 3000 --services-alternate "$@"
