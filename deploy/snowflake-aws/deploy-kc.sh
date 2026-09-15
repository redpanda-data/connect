#!/usr/bin/env bash
# Add the Kafka Connect (official Snowflake Kafka Connector) comparison harness
# to an EC2 box that already has the native `rpcns-snowflake-*` bench dirs from
# deploy.sh. Unlike deploy.sh, this needs Docker on the box (to run a local
# Kafka + Kafka Connect) since it's benchmarking a Kafka-sourced sink, not a
# direct producer.
#
# Does NOT copy any jar from this machine -- the connector is a single
# self-contained fat jar (unlike SAP's ngdbc.jar, it isn't license-gated) that
# gets pulled from Maven Central during `task kc:build` on the box itself. Set
# SF_KC_VERSION to pin a specific release (default: 3.4.0).
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy-kc.sh
#
# Requires locally: rsync. Requires remotely: the dirs already created by
# deploy.sh (~/rpcns-snowflake-bulk, ~/rpcns-snowflake-streaming), and internet
# access on the box (kc:build downloads the connector jar from Maven Central).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_BULK_DIR="${REMOTE_BULK_DIR:-~/rpcns-snowflake-bulk}"
REMOTE_STREAMING_DIR="${REMOTE_STREAMING_DIR:-~/rpcns-snowflake-streaming}"
REMOTE_KC_SHARED_DIR="${REMOTE_KC_SHARED_DIR:-~/rpcns-snowflake-kc-shared}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SNOWFLAKE_BENCH_DIR="$REPO_ROOT/internal/impl/snowflake/bench"
BULK_DIR="$SNOWFLAKE_BENCH_DIR/write/bulk"
STREAMING_DIR="$SNOWFLAKE_BENCH_DIR/write/streaming"

ssh_cmd() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$SSH_HOST" "$@"; }
rsync_cmd() { rsync -avz --partial --progress -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" "$@"; }

echo "== checking native harness is already deployed =="
ssh_cmd "test -d $REMOTE_BULK_DIR && test -d $REMOTE_STREAMING_DIR" || {
  echo "ERROR: $REMOTE_BULK_DIR / $REMOTE_STREAMING_DIR not found on $SSH_HOST -- run deploy.sh first"
  exit 1
}

echo "== provisioning Docker on remote host ($SSH_HOST) =="
ssh_cmd 'bash -s' <<'REMOTE'
set -euo pipefail
if ! command -v docker >/dev/null; then
  if command -v dnf >/dev/null; then
    sudo dnf install -y docker
  else
    sudo yum install -y docker
  fi
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER"
fi
if ! docker compose version >/dev/null 2>&1; then
  sudo dnf install -y docker-compose-plugin 2>/dev/null || \
    sudo yum install -y docker-compose-plugin 2>/dev/null || {
      mkdir -p ~/.docker/cli-plugins
      curl -sSL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
        -o ~/.docker/cli-plugins/docker-compose
      chmod +x ~/.docker/cli-plugins/docker-compose
    }
fi
if ! command -v task >/dev/null; then
  sh -c "$(curl -sSL https://taskfile.dev/install.sh)" -- -d -b "$HOME/bin"
fi
REMOTE

echo "== uploading shared Kafka Connect Dockerfile =="
ssh_cmd "mkdir -p $REMOTE_KC_SHARED_DIR"
rsync_cmd "$SNOWFLAKE_BENCH_DIR/Dockerfile.connect.snowflake" "$SSH_HOST:$REMOTE_KC_SHARED_DIR/"

echo "== uploading bulk Kafka Connect harness =="
rsync_cmd "$BULK_DIR/docker-compose.yaml" "$BULK_DIR/docker-compose.kc.yaml" "$BULK_DIR/load_config.yaml" \
  "$SSH_HOST:$REMOTE_BULK_DIR/"

echo "== uploading streaming Kafka Connect harness =="
rsync_cmd "$STREAMING_DIR/docker-compose.yaml" "$STREAMING_DIR/docker-compose.kc.yaml" "$STREAMING_DIR/load_config.yaml" \
  "$SSH_HOST:$REMOTE_STREAMING_DIR/"

cat <<EOF

Deployed. Next, on the box (every kc: task needs SNOWFLAKE_BENCH_DIR pointing at
the staged Dockerfile dir -- export it once per shell):

  ssh -i $SSH_KEY $SSH_HOST
  export PATH="\$HOME/bin:\$PATH"
  export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
  export SNOWFLAKE_USER="bench_user"
  export SNOWFLAKE_DB="BENCH_DB"
  export SNOWFLAKE_PRIVATE_KEY="\$(cat /path/to/key.p8)"
  export SNOWFLAKE_ROLE="BENCH_ROLE"          # required for streaming, optional for bulk
  export SNOWFLAKE_SCHEMA="RAW"               # optional, default RAW
  export SNOWFLAKE_BENCH_DIR=$REMOTE_KC_SHARED_DIR

  # log out and back in once (docker group membership) if docker was just installed

  # -- bulk (Kafka Connect Snowpipe sink) --
  cd $REMOTE_BULK_DIR
  task up                              # start local Kafka
  task kc:build                        # downloads the connector jar (needs internet)
  task kc:up
  task bench:load COUNT=1000000        # produce test messages into Kafka
  task bench:kc:run BUFFER_COUNT=10000 TASKS=4
  task bench:kc:matrix OUT=kc_bulk.txt
  task down

  # -- streaming (Kafka Connect Snowpipe Streaming sink) --
  cd $REMOTE_STREAMING_DIR
  task up
  task kc:build
  task kc:up
  task bench:load COUNT=2000000
  task bench:kc:run BUFFER_COUNT=10000 MAX_CLIENT_LAG=1 TASKS=4
  task bench:kc:matrix OUT=kc_streaming.txt
  task down
EOF
