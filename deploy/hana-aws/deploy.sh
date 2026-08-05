#!/usr/bin/env bash
# Build the redpanda-connect + bench-load binaries (linux/amd64, no cgo — matches
# go-hdb which is pure Go) and ship them + the bench harness configs (write +
# read: bulk/incrementing/query) to the AWS box.
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy.sh
#
# Requires locally: rsync. Requires (on the AWS box, installed automatically if
# missing): docker, the docker compose plugin, and the `task` CLI (https://taskfile.dev).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_WRITE_DIR="${REMOTE_WRITE_DIR:-~/rpcns-hana-write}"
REMOTE_BULK_DIR="${REMOTE_BULK_DIR:-~/rpcns-hana-bulk}"
REMOTE_INC_DIR="${REMOTE_INC_DIR:-~/rpcns-hana-incrementing}"
REMOTE_QUERY_DIR="${REMOTE_QUERY_DIR:-~/rpcns-hana-query}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SAPHANA_BENCH_DIR="$REPO_ROOT/internal/impl/saphana/bench"
WRITE_DIR="$SAPHANA_BENCH_DIR/saphana-write"
BULK_DIR="$SAPHANA_BENCH_DIR/saphana-read/bulk"
INC_DIR="$SAPHANA_BENCH_DIR/saphana-read/incrementing"
QUERY_DIR="$SAPHANA_BENCH_DIR/saphana-read/query"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

ssh_cmd() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$SSH_HOST" "$@"; }
rsync_cmd() { rsync -avz --partial --progress -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" "$@"; }

echo "== building linux/amd64 binaries locally (stripped) =="
( cd "$REPO_ROOT" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/redpanda-connect-bin" ./cmd/redpanda-connect/ )
# Same binary, just named per-Taskfile expectation (write/bulk/inc/query each
# pkill/run against their own /tmp binary name so concurrent benches don't collide).
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-hana-write-bench"
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-hana-bench"
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-hana-inc-bench"
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-hana-query-bench"

( cd "$WRITE_DIR/load" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-write-bench-load" . )
( cd "$BULK_DIR/load" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-bench-load" . )
( cd "$INC_DIR/load" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-inc-bench-load" . )
( cd "$QUERY_DIR/load" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-query-bench-load" . )

echo "== provisioning remote host ($SSH_HOST) =="
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

echo "== uploading write bench harness =="
ssh_cmd "mkdir -p $REMOTE_WRITE_DIR"
rsync_cmd "$WRITE_DIR/Taskfile.yaml" "$WRITE_DIR/docker-compose.yaml" "$WRITE_DIR/benchmark_config.yaml" \
  "$SSH_HOST:$REMOTE_WRITE_DIR/"

echo "== uploading bulk read bench harness =="
ssh_cmd "mkdir -p $REMOTE_BULK_DIR"
rsync_cmd "$BULK_DIR/Taskfile.yaml" "$BULK_DIR/docker-compose.yaml" "$BULK_DIR/docker-compose.kc.yaml" \
  "$BULK_DIR/benchmark_config.yaml" "$SSH_HOST:$REMOTE_BULK_DIR/"

echo "== uploading incrementing read bench harness =="
ssh_cmd "mkdir -p $REMOTE_INC_DIR"
rsync_cmd "$INC_DIR/Taskfile.yaml" "$INC_DIR/docker-compose.yaml" "$INC_DIR/docker-compose.kc.yaml" \
  "$INC_DIR/benchmark_config.yaml" "$SSH_HOST:$REMOTE_INC_DIR/"

echo "== uploading query read bench harness =="
ssh_cmd "mkdir -p $REMOTE_QUERY_DIR"
rsync_cmd "$QUERY_DIR/Taskfile.yaml" "$QUERY_DIR/docker-compose.yaml" "$QUERY_DIR/docker-compose.kc.yaml" \
  "$QUERY_DIR/benchmark_config.yaml" "$SSH_HOST:$REMOTE_QUERY_DIR/"

echo "== uploading binaries (compressed, resumable) =="
rsync_cmd "$BUILD_DIR/rpcns-hana-write-bench" "$BUILD_DIR/rpcns-hana-write-bench-load" \
  "$BUILD_DIR/rpcns-hana-bench" "$BUILD_DIR/rpcns-hana-bench-load" \
  "$BUILD_DIR/rpcns-hana-inc-bench" "$BUILD_DIR/rpcns-hana-inc-bench-load" \
  "$BUILD_DIR/rpcns-hana-query-bench" "$BUILD_DIR/rpcns-hana-query-bench-load" \
  "$SSH_HOST:/tmp/"
ssh_cmd "chmod +x /tmp/rpcns-hana-write-bench /tmp/rpcns-hana-write-bench-load \
  /tmp/rpcns-hana-bench /tmp/rpcns-hana-bench-load \
  /tmp/rpcns-hana-inc-bench /tmp/rpcns-hana-inc-bench-load \
  /tmp/rpcns-hana-query-bench /tmp/rpcns-hana-query-bench-load"

cat <<EOF

Deployed. Next, on the box:

  ssh -i $SSH_KEY $SSH_HOST
  export PATH="\$HOME/bin:\$PATH"
  export HANA_DSN="hdb://user:pass@your-hana-host:39015"
  export HANA_SCHEMA="YOUR_SCHEMA"

  # -- write bench --
  cd $REMOTE_WRITE_DIR
  task up                              # start local Kafka (docker)
  task bench:setup                     # create BENCH_WRITES table in HANA
  task bench:load COUNT=100000         # produce test messages into Kafka
  task bench:run TOTAL=100000 BATCH_COUNT=10000 MAX_IN_FLIGHT=8
  task bench:matrix TOTAL=100000 OUT=results.txt
  task down

  # -- bulk read bench --
  cd $REMOTE_BULK_DIR
  task up
  task bench:load COUNT=1000000
  task bench:run FETCH_SIZE=10000 BATCH_COUNT=1000 MAX_IN_FLIGHT=10
  task bench:matrix OUT=rpcn_bulk.txt
  task down

  # -- incrementing read bench --
  cd $REMOTE_INC_DIR
  task up
  task bench:run
  task bench:matrix COUNT=500000 OUT=rpcn_inc.txt
  task down

  # -- query read bench --
  cd $REMOTE_QUERY_DIR
  task up
  task bench:run
  task bench:matrix OUT=rpcn_query.txt
  task down

  task logs                            # tail the running benchmark (from within each dir)
EOF
