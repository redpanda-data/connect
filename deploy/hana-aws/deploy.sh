#!/usr/bin/env bash
# Build the redpanda-connect + bench-load binaries (linux/amd64, no cgo — matches
# go-hdb which is pure Go) and ship them + the bench harness config to the AWS box.
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy.sh
#
# Requires locally: rsync. Requires (on the AWS box, installed automatically if
# missing): docker, the docker compose plugin, and the `task` CLI (https://taskfile.dev).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_DIR="${REMOTE_DIR:-~/rpcns-hana-write}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$REPO_ROOT/internal/impl/saphana/bench/saphana-write"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

ssh_cmd() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$SSH_HOST" "$@"; }
rsync_cmd() { rsync -avz --partial --progress -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" "$@"; }

echo "== building linux/amd64 binaries locally (stripped) =="
( cd "$REPO_ROOT" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-write-bench" ./cmd/redpanda-connect/ )
( cd "$BENCH_DIR/load" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/rpcns-hana-write-bench-load" . )

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

echo "== uploading bench harness + binaries (compressed, resumable) =="
ssh_cmd "mkdir -p $REMOTE_DIR"
rsync_cmd "$BENCH_DIR/Taskfile.yaml" "$BENCH_DIR/docker-compose.yaml" "$BENCH_DIR/benchmark_config.yaml" \
  "$SSH_HOST:$REMOTE_DIR/"
rsync_cmd "$BUILD_DIR/rpcns-hana-write-bench" "$BUILD_DIR/rpcns-hana-write-bench-load" \
  "$SSH_HOST:/tmp/"
ssh_cmd "chmod +x /tmp/rpcns-hana-write-bench /tmp/rpcns-hana-write-bench-load"

cat <<EOF

Deployed. Next, on the box:

  ssh -i $SSH_KEY $SSH_HOST
  cd $REMOTE_DIR
  export PATH="\$HOME/bin:\$PATH"
  export HANA_DSN="hdb://user:pass@your-hana-host:39015"
  export HANA_SCHEMA="YOUR_SCHEMA"

  task up                              # start local Kafka (docker)
  task bench:setup                     # create BENCH_WRITES table in HANA
  task bench:load COUNT=100000         # produce test messages into Kafka
  task bench:run TOTAL=100000 BATCH_COUNT=10000 MAX_IN_FLIGHT=8
  # or sweep configs:
  task bench:matrix TOTAL=100000 OUT=results.txt

  task logs                            # tail the running benchmark
  task down                            # tear down Kafka when done
EOF
