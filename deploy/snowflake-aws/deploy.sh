#!/usr/bin/env bash
# Build the redpanda-connect binary (linux/amd64, no cgo -- the Snowflake Go
# driver and streaming ingest SDK are pure Go) and ship it + the native bench
# harness (write/bulk, write/streaming) to the AWS box. No Docker needed here:
# unlike the SAP HANA bench, the native Snowflake benches produce synthetic
# records in-process (input.generate) and talk to Snowflake directly, with no
# local Kafka in the loop.
#
# Usage:
#   SSH_KEY=~/Downloads/atul_ed25519 SSH_HOST=ec2-user@44.220.172.22 ./deploy.sh
#
# Requires locally: rsync. Requires (on the AWS box, installed automatically if
# missing): the `task` CLI (https://taskfile.dev).
#
# task setup/teardown (creating BENCH_EVENTS/BENCH_EVENTS_JSON/BENCH_STAGE/
# BENCH_PIPE) need `snowsql` and aren't run by this script -- run them from
# wherever you already have snowsql in PATH (doesn't need to be the EC2 box,
# it only needs network access to your Snowflake account).
set -euo pipefail

SSH_KEY="${SSH_KEY:?set SSH_KEY to your private key path}"
SSH_HOST="${SSH_HOST:?set SSH_HOST to user@host}"
REMOTE_BULK_DIR="${REMOTE_BULK_DIR:-~/rpcns-snowflake-bulk}"
REMOTE_STREAMING_DIR="${REMOTE_STREAMING_DIR:-~/rpcns-snowflake-streaming}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SNOWFLAKE_BENCH_DIR="$REPO_ROOT/internal/impl/snowflake/bench"
BULK_DIR="$SNOWFLAKE_BENCH_DIR/write/bulk"
STREAMING_DIR="$SNOWFLAKE_BENCH_DIR/write/streaming"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

ssh_cmd() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$SSH_HOST" "$@"; }
rsync_cmd() { rsync -avz --partial --progress -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" "$@"; }

echo "== building linux/amd64 binary locally (stripped) =="
( cd "$REPO_ROOT" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o "$BUILD_DIR/redpanda-connect-bin" ./cmd/redpanda-connect/ )
# Same binary, copied per-dir so `RPCN=./<name>` stays self-contained and a
# redeploy of one mode can't clobber a binary the other mode has mid-run.
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-snowflake-bulk-bench"
cp "$BUILD_DIR/redpanda-connect-bin" "$BUILD_DIR/rpcns-snowflake-streaming-bench"

echo "== provisioning remote host ($SSH_HOST) =="
ssh_cmd 'bash -s' <<'REMOTE'
set -euo pipefail
if ! command -v task >/dev/null; then
  sh -c "$(curl -sSL https://taskfile.dev/install.sh)" -- -d -b "$HOME/bin"
fi
REMOTE

echo "== uploading bulk (PUT + Snowpipe) bench harness =="
ssh_cmd "mkdir -p $REMOTE_BULK_DIR"
rsync_cmd "$BULK_DIR/Taskfile.yaml" "$BULK_DIR/benchmark_config.yaml" \
  "$BUILD_DIR/rpcns-snowflake-bulk-bench" "$SSH_HOST:$REMOTE_BULK_DIR/"

echo "== uploading streaming (Snowpipe Streaming) bench harness =="
ssh_cmd "mkdir -p $REMOTE_STREAMING_DIR"
rsync_cmd "$STREAMING_DIR/Taskfile.yaml" "$STREAMING_DIR/benchmark_config.yaml" \
  "$BUILD_DIR/rpcns-snowflake-streaming-bench" "$SSH_HOST:$REMOTE_STREAMING_DIR/"

ssh_cmd "chmod +x $REMOTE_BULK_DIR/rpcns-snowflake-bulk-bench $REMOTE_STREAMING_DIR/rpcns-snowflake-streaming-bench"

cat <<EOF

Deployed. Next, on the box:

  ssh -i $SSH_KEY $SSH_HOST
  export PATH="\$HOME/bin:\$PATH"
  export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
  export SNOWFLAKE_USER="bench_user"
  export SNOWFLAKE_DB="BENCH_DB"
  export SNOWFLAKE_PRIVATE_KEY="\$(cat /path/to/key.p8)"
  export SNOWFLAKE_ROLE="BENCH_ROLE"          # optional
  export SNOWFLAKE_SCHEMA="RAW"               # optional, default RAW

  # -- run 'task setup' once first, from wherever snowsql is in PATH --
  #    (doesn't have to be this box -- see header comment in this script)

  # -- bulk (PUT + Snowpipe) bench --
  cd $REMOTE_BULK_DIR
  export SNOWFLAKE_WAREHOUSE="BENCH_WH"       # bulk only
  task bench:run RPCN=./rpcns-snowflake-bulk-bench COUNT=500000 BATCH=5000 UPLOAD_THREADS=8 MAX_IN_FLIGHT=16
  task bench:matrix RPCN=./rpcns-snowflake-bulk-bench OUT=results_bulk.txt

  # -- streaming (Snowpipe Streaming) bench --
  cd $REMOTE_STREAMING_DIR
  task bench:run RPCN=./rpcns-snowflake-streaming-bench COUNT=1000000 BATCH=5000 PARALLELISM=4
  task bench:matrix RPCN=./rpcns-snowflake-streaming-bench OUT=results_streaming.txt
EOF
