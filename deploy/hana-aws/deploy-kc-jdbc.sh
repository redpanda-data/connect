#!/usr/bin/env bash
# Add the generic Confluent JDBC Source connector benchmark harness (bulk read
# only, for comparison against the kafka-connect-sap connector) to an EC2 box
# that already has the native `rpcns-hana-*` bench dirs from deploy.sh.
#
# Does NOT copy ngdbc.jar from this machine — it's pulled straight from Maven
# Central (com.sap.cloud.db.jdbc:ngdbc, public) by a curl run on the EC2 box
# itself. Set NGDBC_VERSION to pin a specific release (default: latest).
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy-kc-jdbc.sh
#
# Requires locally: rsync. Requires remotely: the ~/rpcns-hana-bulk dir already
# created by deploy.sh, docker + compose plugin, the `task` CLI, and internet
# access on the box (kc:jdbc:build downloads the kafka-connect-jdbc plugin via
# confluent-hub).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_BULK_DIR="${REMOTE_BULK_DIR:-~/rpcns-hana-bulk}"
NGDBC_VERSION="${NGDBC_VERSION:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SAPHANA_BENCH_DIR="$REPO_ROOT/internal/impl/saphana/bench"
BULK_DIR="$SAPHANA_BENCH_DIR/saphana-read/bulk"

ssh_cmd() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$SSH_HOST" "$@"; }
rsync_cmd() { rsync -avz --partial --progress -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" "$@"; }

echo "== fetching ngdbc.jar on $SSH_HOST (from Maven Central) =="
ssh_cmd "mkdir -p ~/rpcns-hana-kc-shared"
ssh_cmd "bash -s" <<REMOTE
set -euo pipefail
VER="$NGDBC_VERSION"
if [ -z "\$VER" ]; then
  VER=\$(curl -sf https://repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/maven-metadata.xml \
    | grep -o '<release>[^<]*' | cut -d'>' -f2)
fi
if [ -z "\$VER" ]; then echo "ERROR: could not resolve latest ngdbc version"; exit 1; fi
echo "Downloading ngdbc \$VER..."
curl -sf -o ~/rpcns-hana-kc-shared/ngdbc.jar \
  "https://repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/\${VER}/ngdbc-\${VER}.jar"
test -s ~/rpcns-hana-kc-shared/ngdbc.jar || { echo "ERROR: download produced an empty file"; exit 1; }
echo "ngdbc \$VER staged at ~/rpcns-hana-kc-shared/ngdbc.jar"
REMOTE

echo "== uploading shared JDBC Kafka Connect Dockerfile =="
ssh_cmd "mkdir -p ~/rpcns-hana-kc-shared"
rsync_cmd "$SAPHANA_BENCH_DIR/Dockerfile.connect.jdbc" "$SSH_HOST:~/rpcns-hana-kc-shared/"

echo "== uploading bulk read bench JDBC Kafka Connect harness =="
ssh_cmd "mkdir -p $REMOTE_BULK_DIR"
rsync_cmd "$BULK_DIR/Taskfile.yaml" "$BULK_DIR/docker-compose.kc-jdbc.yaml" "$SSH_HOST:$REMOTE_BULK_DIR/"

cat <<EOF

Deployed. Next, on the box (every kc: task needs SAPHANA_BENCH_DIR pointing at the
staged jar/Dockerfile dir — export it once per shell):

  ssh -i $SSH_KEY $SSH_HOST
  export PATH="\$HOME/bin:\$PATH"
  export HANA_DSN="hdb://user:pass@your-hana-host:39015"
  export HANA_SCHEMA="YOUR_SCHEMA"
  export SAPHANA_BENCH_DIR=~/rpcns-hana-kc-shared

  # -- bulk read bench (generic Confluent JDBC Source connector) --
  cd $REMOTE_BULK_DIR
  task up                              # if not already running
  task kc:jdbc:build                   # downloads kafka-connect-jdbc plugin (needs internet)
  task kc:jdbc:up
  task bench:load COUNT=1000000
  task bench:kc:jdbc:run FETCH_SIZE=10000 BATCH_MAX_ROWS=1000 TOTAL=1000000
  task bench:kc:jdbc:matrix TOTAL=1000000 OUT=jdbc_bulk.txt
  task down

  task logs                            # tail the running benchmark (from within the dir)
EOF
