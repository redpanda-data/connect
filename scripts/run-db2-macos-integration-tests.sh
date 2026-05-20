#!/usr/bin/env bash
# Run DB2 CDC integration tests locally.
#
# On macOS: runs the test binary NATIVELY on the host using IBM's clidriver
# (libdb2.dylib). No docker-exec, no cross-compilation.
#
# On Linux (CI): cross-compiles for linux/amd64, copies the binary into the
# DB2 container, and runs it there with DB2_USE_LOCAL=1 (unchanged behaviour).
#
# Usage:
#   ./scripts/run-db2-macos-integration-tests.sh
#   ./scripts/run-db2-macos-integration-tests.sh -test.run TestIntegrationDB2CDCDriver
#
# Flags:
#   -timeout-minutes N          Override test timeout (default 40).
#   -test.run=<regex>           Override which tests to run.
#   --keep                      Keep the DB2 container alive after the run.
#   --reset                     Remove the named persistent container and volume,
#                               then start fresh.
#
# macOS — install the IBM clidriver (one of):
#   pip3 install --user ibm_db   # bundles libdb2.dylib in the Python package
#   export DB2_DYLIB_PATH=/path/to/libdb2.dylib
#
# See: https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OS="$(uname -s)"
BIN="$(mktemp -t db2test.XXXXXX)"
DB2_CTR_NAME="${DB2_CONTAINER_NAME:-db2-integration-test-persistent}"
DB2_VOLUME="db2-testdb-data"
SETUP_LOG="$(mktemp -t db2setup.XXXXXX)"
LOG_PID=""
KEEP=false
RESET=false

cleanup() {
  [[ -n "$LOG_PID" ]] && kill "$LOG_PID" 2>/dev/null || true
  rm -f "$BIN" "$SETUP_LOG"
  if [[ "$KEEP" == "false" ]]; then
    echo "==> Cleaning up container: $DB2_CTR_NAME"
    docker rm -f "$DB2_CTR_NAME" 2>/dev/null || true
  else
    echo "==> Keeping container $DB2_CTR_NAME alive for reuse (--keep was set)"
  fi
}
trap cleanup EXIT

# --------------------------------------------------------------------------
# Parse flags
# --------------------------------------------------------------------------
TEST_TIMEOUT="40m"
TEST_RUN="^TestIntegration"
TEST_EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -timeout-minutes)
      shift; TEST_TIMEOUT="${1}m"; shift ;;
    -test.run=*)
      TEST_RUN="${1#-test.run=}"; shift ;;
    --keep)
      KEEP=true; shift ;;
    --reset)
      RESET=true; shift ;;
    *)
      TEST_EXTRA_ARGS+=("$1"); shift ;;
  esac
done

# --------------------------------------------------------------------------
# macOS: locate libdb2.dylib (IBM clidriver — TCP-only, no SysV IPC)
# --------------------------------------------------------------------------
find_darwin_lib() {
  # 1. Explicit env var override
  if [[ -n "${DB2_DYLIB_PATH:-}" ]]; then
    if [[ -f "$DB2_DYLIB_PATH" ]]; then
      echo "$DB2_DYLIB_PATH"
      return 0
    fi
    echo "ERROR: DB2_DYLIB_PATH='$DB2_DYLIB_PATH' does not exist" >&2
    return 1
  fi

  # 2. ibm_db Python package bundles the clidriver
  local ibm_db_dir
  ibm_db_dir=$(python3 -c 'import ibm_db, os; print(os.path.dirname(ibm_db.__file__))' 2>/dev/null || true)
  if [[ -n "$ibm_db_dir" ]]; then
    local candidate="$ibm_db_dir/clidriver/lib/libdb2.dylib"
    if [[ -f "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  fi

  # 3. Standard IBM installation paths
  for p in \
    "/Library/IBM/SQLLIB/lib64/libdb2.dylib" \
    "/Library/IBM/SQLLIB/lib/libdb2.dylib" \
    "$HOME/sqllib/lib64/libdb2.dylib"; do
    if [[ -f "$p" ]]; then
      echo "$p"
      return 0
    fi
  done

  cat >&2 <<'EOF'
ERROR: IBM DB2 ODBC CLI Driver (libdb2.dylib) not found.

Install options (choose one):
  pip3 install --user ibm_db     # pip bundles the clidriver; --user avoids sudo
  export DB2_DYLIB_PATH=/path/to/libdb2.dylib

IBM official download:
  https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information
EOF
  return 1
}

# --------------------------------------------------------------------------
# Reset: destroy container + volume so next run starts fresh.
# --------------------------------------------------------------------------
if [[ "$RESET" == "true" ]]; then
  echo "==> --reset: removing container $DB2_CTR_NAME and volume $DB2_VOLUME"
  docker rm -f "$DB2_CTR_NAME" 2>/dev/null || true
  docker volume rm "$DB2_VOLUME" 2>/dev/null || true
  echo "==> Reset complete. Re-run without --reset to start fresh."
  exit 0
fi

# --------------------------------------------------------------------------
# Reuse: if the container already exists and is running, skip setup entirely.
# --------------------------------------------------------------------------
CONTAINER_REUSED=false
if docker ps -q --filter "name=^${DB2_CTR_NAME}$" | grep -q .; then
  echo "==> Reusing existing container $DB2_CTR_NAME (skipping ~8-min first-time setup)"
  CONTAINER_REUSED=true
elif docker ps -aq --filter "name=^${DB2_CTR_NAME}$" | grep -q .; then
  echo "==> Starting stopped container $DB2_CTR_NAME"
  docker start "$DB2_CTR_NAME"
  CONTAINER_REUSED=true
fi

# --------------------------------------------------------------------------
# First run: pull image, create volume, start container.
# --------------------------------------------------------------------------
if [[ "$CONTAINER_REUSED" == "false" ]]; then
  echo "==> Pulling DB2 image (no-op if already cached)"
  docker pull --platform linux/amd64 icr.io/db2_community/db2:latest

  echo "==> Creating named volume $DB2_VOLUME (no-op if already exists)"
  docker volume create "$DB2_VOLUME" >/dev/null

  echo "==> Starting DB2 container $DB2_CTR_NAME"
  if [[ "$OS" == "Darwin" ]]; then
    # macOS: clidriver connects via TCP/IP — no SysV IPC, so --ipc=host is not needed.
    docker run --detach \
      --name "$DB2_CTR_NAME" \
      --platform linux/amd64 \
      --privileged \
      --shm-size 512m \
      --volume "${DB2_VOLUME}:/database" \
      -p 50000:50000 \
      -e LICENSE=accept \
      -e DB2INST1_PASSWORD=password \
      -e DBNAME=TESTDB \
      -e ARCHIVE_LOGS=true \
      -e AUTOCONFIG=false \
      -e SAMPLEDB=false \
      -e REPODB=false \
      -e HADR_ENABLED=NO \
      -e UPDATEAVAIL=NO \
      icr.io/db2_community/db2:latest
  else
    docker run --detach \
      --name "$DB2_CTR_NAME" \
      --platform linux/amd64 \
      --privileged \
      --ipc=host \
      --shm-size 512m \
      --volume "${DB2_VOLUME}:/database" \
      -p 50000:50000 \
      -e LICENSE=accept \
      -e DB2INST1_PASSWORD=password \
      -e DBNAME=TESTDB \
      -e ARCHIVE_LOGS=true \
      -e AUTOCONFIG=false \
      -e SAMPLEDB=false \
      -e REPODB=false \
      -e HADR_ENABLED=NO \
      -e UPDATEAVAIL=NO \
      icr.io/db2_community/db2:latest
  fi

  docker logs -f "$DB2_CTR_NAME" 2>&1 | tee "$SETUP_LOG" | sed 's/^/[db2] /' &
  LOG_PID=$!

  echo "==> Waiting for DB2 first-time setup (up to 25 min)..."
  SETUP_DEADLINE=$(( $(date +%s) + 1500 ))
  while true; do
    if grep -q "Setup has completed" "$SETUP_LOG" 2>/dev/null; then
      echo "==> DB2 setup complete"
      break
    fi
    if [[ $(date +%s) -ge $SETUP_DEADLINE ]]; then
      echo "ERROR: DB2 setup did not complete within 25 min" >&2
      exit 1
    fi
    if ! docker ps -q --filter "name=^${DB2_CTR_NAME}$" | grep -q . 2>/dev/null; then
      echo "ERROR: DB2 container exited before setup completed" >&2
      exit 1
    fi
    sleep 5
  done

  kill "$LOG_PID" 2>/dev/null || true
  LOG_PID=""
fi

# --------------------------------------------------------------------------
# Build and run tests.
# --------------------------------------------------------------------------
cd "$REPO_ROOT"

if [[ "$OS" == "Darwin" ]]; then
  # Locate clidriver before building so we fail fast with a helpful message.
  FOUND_DYLIB=$(find_darwin_lib)

  echo "==> Building test binary for macOS host (native, no cross-compilation)"
  go test -c -o "$BIN" ./internal/impl/db2/

  echo "==> Running integration tests natively on macOS (clidriver: $FOUND_DYLIB)"
  DB2_DYLIB_PATH="$FOUND_DYLIB" \
  DB2_DARWIN_HOST=localhost \
  DB2_DARWIN_PORT=50000 \
  DB2_DARWIN_CONTAINER="$DB2_CTR_NAME" \
    "$BIN" -test.v -test.timeout "$TEST_TIMEOUT" "-test.run=${TEST_RUN}" "${TEST_EXTRA_ARGS[@]+"${TEST_EXTRA_ARGS[@]}"}"
else
  echo "==> Building linux/amd64 test binary"
  GOOS=linux GOARCH=amd64 go test -c -o "$BIN" ./internal/impl/db2/

  echo "==> Copying test binary into $DB2_CTR_NAME"
  docker cp "$BIN" "$DB2_CTR_NAME:/tmp/db2test"
  docker exec "$DB2_CTR_NAME" chmod 755 /tmp/db2test

  echo "==> Running integration tests inside DB2 container (timeout: $TEST_TIMEOUT, run: $TEST_RUN)"
  docker exec \
    -e DB2_USE_LOCAL=1 \
    -e DB2DIR=/opt/ibm/db2/V12.1 \
    -e DB2INSTANCE=db2inst1 \
    -e DB2MSGPATH=/opt/ibm/db2/V12.1/msg \
    -e LANG=en_US.iso88591 \
    "$DB2_CTR_NAME" \
    /tmp/db2test -test.v -test.timeout "$TEST_TIMEOUT" "-test.run=${TEST_RUN}" "${TEST_EXTRA_ARGS[@]+"${TEST_EXTRA_ARGS[@]}"}"
fi
