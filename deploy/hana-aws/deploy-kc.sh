#!/usr/bin/env bash
# Add the Kafka Connect (kafka-connect-sap JDBC source/sink) benchmark harness to an
# EC2 box that already has the native `rpcns-hana-*` bench dirs from deploy.sh.
#
# Does NOT copy ngdbc.jar from this machine — it's pulled straight from Maven
# Central (com.sap.cloud.db.jdbc:ngdbc, public) by a curl run on the EC2 box
# itself. Set NGDBC_VERSION to pin a specific release (default: latest).
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy-kc.sh
#
# Requires locally: rsync. Requires remotely: the dirs already created by
# deploy.sh (~/rpcns-hana-write, ~/rpcns-hana-bulk, ~/rpcns-hana-incrementing,
# ~/rpcns-hana-query), docker + compose plugin, the `task` CLI, and internet
# access on the box (kc:build downloads the kafka-connect-sap plugin jar).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_WRITE_DIR="${REMOTE_WRITE_DIR:-~/rpcns-hana-write}"
REMOTE_BULK_DIR="${REMOTE_BULK_DIR:-~/rpcns-hana-bulk}"
REMOTE_INC_DIR="${REMOTE_INC_DIR:-~/rpcns-hana-incrementing}"
REMOTE_QUERY_DIR="${REMOTE_QUERY_DIR:-~/rpcns-hana-query}"
NGDBC_VERSION="${NGDBC_VERSION:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SAPHANA_BENCH_DIR="$REPO_ROOT/internal/impl/saphana/bench"
WRITE_DIR="$SAPHANA_BENCH_DIR/saphana-write"
BULK_DIR="$SAPHANA_BENCH_DIR/saphana-read/bulk"
INC_DIR="$SAPHANA_BENCH_DIR/saphana-read/incrementing"
QUERY_DIR="$SAPHANA_BENCH_DIR/saphana-read/query"

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

echo "== uploading shared Kafka Connect Dockerfile =="
ssh_cmd "mkdir -p ~/rpcns-hana-kc-shared"
rsync_cmd "$SAPHANA_BENCH_DIR/Dockerfile.connect.sap" "$SSH_HOST:~/rpcns-hana-kc-shared/"

echo "== uploading write bench Kafka Connect harness =="
ssh_cmd "mkdir -p $REMOTE_WRITE_DIR"
rsync_cmd "$WRITE_DIR/Taskfile.yaml" "$WRITE_DIR/docker-compose.kc.yaml" "$SSH_HOST:$REMOTE_WRITE_DIR/"

echo "== uploading bulk read bench Kafka Connect harness =="
rsync_cmd "$BULK_DIR/Taskfile.yaml" "$BULK_DIR/docker-compose.kc.yaml" "$SSH_HOST:$REMOTE_BULK_DIR/"

echo "== uploading incrementing read bench Kafka Connect harness =="
rsync_cmd "$INC_DIR/Taskfile.yaml" "$INC_DIR/docker-compose.kc.yaml" "$SSH_HOST:$REMOTE_INC_DIR/"

echo "== uploading query read bench Kafka Connect harness =="
rsync_cmd "$QUERY_DIR/Taskfile.yaml" "$QUERY_DIR/docker-compose.kc.yaml" "$SSH_HOST:$REMOTE_QUERY_DIR/"

cat <<EOF

Deployed. Next, on the box (every kc: task needs SAPHANA_BENCH_DIR pointing at the
staged jar/Dockerfile dir — export it once per shell):

  ssh -i $SSH_KEY $SSH_HOST
  export PATH="\$HOME/bin:\$PATH"
  export HANA_DSN="hdb://user:pass@your-hana-host:39015"
  export HANA_SCHEMA="YOUR_SCHEMA"
  export SAPHANA_BENCH_DIR=~/rpcns-hana-kc-shared

  # -- write bench (Kafka Connect SAP HANA Sink) --
  cd $REMOTE_WRITE_DIR
  task up                              # if not already running
  task kc:build                        # downloads kafka-connect-sap plugin (needs internet)
  task kc:up
  task bench:setup
  task bench:load COUNT=100000
  task bench:kc:run TOTAL=100000 BATCH_SIZE=5000 TASKS=1
  task bench:kc:matrix TOTAL=100000 OUT=kc_write.txt
  task down

  # -- bulk read bench (Kafka Connect JDBC Source) --
  cd $REMOTE_BULK_DIR
  task up
  task kc:build
  task kc:up
  task bench:load COUNT=1000000
  task bench:kc:run FETCH_SIZE=10000 BATCH_MAX_ROWS=1000 TOTAL=1000000
  task bench:kc:matrix TOTAL=1000000 OUT=kc_bulk.txt
  task down

  # -- incrementing read bench (Kafka Connect JDBC Source) --
  cd $REMOTE_INC_DIR
  task up
  task kc:build
  task kc:up
  task bench:kc:run COUNT=500000 POLL=1000 BATCH_MAX_ROWS=10000
  task bench:kc:matrix COUNT=500000 OUT=kc_inc.txt
  task down

  # -- query read bench (Kafka Connect JDBC Source) --
  cd $REMOTE_QUERY_DIR
  task up
  task kc:build
  task kc:up
  task bench:kc:run FETCH_SIZE=10000 BATCH_MAX_ROWS=10000 TOTAL=2000000
  task bench:kc:matrix TOTAL=2000000 OUT=kc_query.txt
  task down

  task logs                            # tail the running benchmark (from within each dir)
EOF
