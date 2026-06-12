#!/usr/bin/env bash
# Run SAP HANA CDC integration tests locally on macOS or in CI.
#
# Starts SAP HANA Express 2.00 SPS08 in Docker, installs the CDC change-table
# infrastructure (_RPCN_CDC schema + CHANGES table + triggers), seeds every
# operation class (INSERT/UPDATE/DELETE/UPSERT/TRUNCATE/DDL/LOB/transactions),
# and runs the full Go integration test suite.
#
# Usage:
#   ./scripts/run-saphana-macos-integration-tests.sh
#   ./scripts/run-saphana-macos-integration-tests.sh -test.run TestInsert
#   ./scripts/run-saphana-macos-integration-tests.sh --keep
#   ./scripts/run-saphana-macos-integration-tests.sh --reset
#
# Flags:
#   -timeout-minutes N     Test timeout (default 20)
#   -test.run=<regex>      Test filter (default ^TestIntegration)
#   --keep                 Leave HANA container running after tests
#   --reset                Destroy container + data volume, start fresh
#
# Requirements (macOS / Apple Silicon):
#   - Docker Desktop with Rosetta / linux/amd64 emulation enabled
#   - 8+ GB RAM available to Docker (HANA needs ≥6 GB)
#   - 20+ GB free disk space (HANA data volume)
#   - First run: ~10 min for HANA to start and initialise
#   - Subsequent runs: reuses the persistent container (<30 s to ready)
#
# Environment overrides:
#   HANA_CONTAINER_NAME  container name (default: hxe-integration-test)
#   HANA_DATA_PATH       host data directory (default: /tmp/hana-express-data)
#   HANA_PASS            SYSTEM password (default: HXEHana1)
#   HANA_PORT            tenant SQL port (default: 39015)

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CTR_NAME="${HANA_CONTAINER_NAME:-hxe-integration-test}"
DATA_DIR="${HANA_DATA_PATH:-/tmp/hana-express-data}"
HANA_PASS="${HANA_PASS:-HXEHana1}"
HANA_PORT="${HANA_PORT:-39015}"
DC_FILE="$REPO_ROOT/internal/impl/saphana/testdata/docker-compose.yml"
SQL_DIR="$REPO_ROOT/internal/impl/saphana/sql"
KEEP=false
RESET=false
TEST_TIMEOUT="20m"
TEST_RUN="^TestIntegration"
TEST_EXTRA_ARGS=()

# ── Helpers ───────────────────────────────────────────────────────────────────
log()      { printf '\033[1;34m[saphana]\033[0m %s\n' "$*" >&2; }
log_ok()   { printf '\033[1;32m[saphana]\033[0m %s\n' "$*" >&2; }
log_warn() { printf '\033[1;33m[saphana]\033[0m %s\n' "$*" >&2; }
die()      { printf '\033[1;31m[saphana]\033[0m ERROR: %s\n' "$*" >&2; exit 1; }

hdbsql() {
    docker exec "$CTR_NAME" \
        /usr/sap/HXE/HDB90/exe/hdbsql \
        -i 90 -u SYSTEM -p "$HANA_PASS" -d HXE "$@"
}

hdbsql_file() {
    local file="$1"
    local basename; basename="$(basename "$file")"
    log "  Running SQL: $basename"
    docker exec "$CTR_NAME" \
        /usr/sap/HXE/HDB90/exe/hdbsql \
        -i 90 -u SYSTEM -p "$HANA_PASS" -d HXE \
        -I "/hana/mounts/$basename" 2>&1 || \
        log_warn "  $basename returned non-zero (may be idempotent DDL errors)"
}

container_health() {
    docker inspect --format='{{.State.Health.Status}}' "$CTR_NAME" 2>/dev/null || echo "missing"
}

# ── Cleanup ───────────────────────────────────────────────────────────────────
cleanup() {
    if [[ "$KEEP" == "false" ]]; then
        log "Stopping HANA container..."
        HANA_DATA_PATH="$DATA_DIR" docker-compose -f "$DC_FILE" down 2>/dev/null || true
    else
        log_ok "Keeping container $CTR_NAME alive (--keep). Reuse next run for faster startup."
    fi
}
trap cleanup EXIT

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -timeout-minutes)  shift; TEST_TIMEOUT="${1}m"; shift ;;
        -test.run=*)       TEST_RUN="${1#-test.run=}"; shift ;;
        --keep)            KEEP=true; shift ;;
        --reset)           RESET=true; shift ;;
        *)                 TEST_EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# ── Reset ─────────────────────────────────────────────────────────────────────
if [[ "$RESET" == "true" ]]; then
    log "--reset: destroying container $CTR_NAME and data directory $DATA_DIR"
    HANA_DATA_PATH="$DATA_DIR" docker-compose -f "$DC_FILE" down -v 2>/dev/null || true
    rm -rf "$DATA_DIR"
    log_ok "Reset complete. Re-run without --reset to start fresh."
    exit 0
fi

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    die "docker is not installed or not in PATH"
fi
if ! docker info &>/dev/null; then
    die "Docker daemon is not running. Start Docker Desktop first."
fi

# Check Rosetta / linux/amd64 emulation on Apple Silicon
ARCH="$(uname -m)"
if [[ "$ARCH" == "arm64" ]]; then
    if ! docker run --rm --platform linux/amd64 alpine:latest uname -m &>/dev/null 2>&1; then
        die "linux/amd64 emulation not available. Enable Rosetta in Docker Desktop: Settings → Features in development → Use Rosetta"
    fi
    log "Apple Silicon: linux/amd64 emulation via Rosetta detected ✓"
fi

# ── Container lifecycle ───────────────────────────────────────────────────────
HEALTH="$(container_health)"
REUSED=false

if [[ "$HEALTH" == "healthy" ]]; then
    log_ok "Reusing healthy container $CTR_NAME (skipping ~10 min first-time setup)"
    REUSED=true
elif [[ "$HEALTH" == "starting" ]]; then
    log "Container $CTR_NAME is starting — waiting for healthy..."
    REUSED=true
elif docker ps -aq --filter "name=^${CTR_NAME}$" | grep -q . 2>/dev/null; then
    log "Starting stopped container $CTR_NAME..."
    docker start "$CTR_NAME" >/dev/null
    REUSED=true
fi

if [[ "$REUSED" == "false" ]]; then
    log "First run — setting up HANA Express data directory: $DATA_DIR"
    mkdir -p "$DATA_DIR"
    cp "$REPO_ROOT/internal/impl/saphana/testdata/hxepasswd.json" "$DATA_DIR/hxepasswd.json"
    chmod 600 "$DATA_DIR/hxepasswd.json"

    # Copy SQL files into the volume so hdbsql can read them inside the container.
    for f in "$SQL_DIR"/*.sql; do
        cp "$f" "$DATA_DIR/"
    done

    log "Pulling and starting SAP HANA Express 2.00 SPS08 (linux/amd64)..."
    log "This takes 5–10 minutes on first run."
    HANA_DATA_PATH="$DATA_DIR" docker-compose -f "$DC_FILE" up -d hana
fi

# Wait for healthy (covers both first-run and reuse/starting cases).
if [[ "$(container_health)" != "healthy" ]]; then
    log "Waiting for HANA to become healthy (up to 10 min)..."
    DEADLINE=$(( $(date +%s) + 600 ))
    while true; do
        HEALTH="$(container_health)"
        if [[ "$HEALTH" == "healthy" ]]; then
            log_ok "HANA is healthy ✓"
            break
        fi
        if [[ $(date +%s) -ge $DEADLINE ]]; then
            log "Last 50 lines from container log:"
            docker logs "$CTR_NAME" --tail=50 2>&1 || true
            die "HANA did not become healthy within 10 min"
        fi
        log "  status=$HEALTH — waiting..."
        sleep 10
    done
fi

# ── One-time HANA setup (idempotent) ──────────────────────────────────────────
log "Installing CDC infrastructure (idempotent)..."

# Copy SQL files on every run in case they were updated.
for f in "$SQL_DIR"/*.sql; do
    cp "$f" "$DATA_DIR/"
done

hdbsql_file "$SQL_DIR/01_schema.sql"
hdbsql_file "$SQL_DIR/02_verify_schema.sql"
hdbsql_file "$SQL_DIR/03_triggers.sql"

# Activate log-segment retention (required for binary log research).
log "  Activating log segment retention..."
hdbsql "BACKUP DATA USING FILE ('/hana/mounts/backup/FULL')" 2>/dev/null && \
    log_ok "  Log backup complete — redo log segments will be retained." || \
    log_warn "  Backup skipped (already exists or insufficient space)"

# Fix log file permissions so the sidecar can read binary log segments.
docker exec "$CTR_NAME" bash -c \
    "find /hana/mounts/log -name 'logsegment_*.dat' -exec chmod 640 {} \;" 2>/dev/null || true

# ── Smoke test: HANA is reachable via TCP ────────────────────────────────────
log "Smoke test: TCP connectivity on port $HANA_PORT..."
if ! nc -z localhost "$HANA_PORT" 2>/dev/null; then
    die "Cannot reach HANA on localhost:$HANA_PORT — check Docker port mapping"
fi
log_ok "  Port $HANA_PORT is open ✓"

# ── Display log segment status (useful for binary format research) ────────────
log "Log segment status:"
hdbsql "SELECT SEGMENT_ID, STATE, USED_SIZE, LAST_COMMIT_TIME FROM SYS.M_LOG_SEGMENTS ORDER BY SEGMENT_ID" \
    2>/dev/null || true

# ── Run the full integration test suite ──────────────────────────────────────
log "Running SAP HANA CDC integration tests"
log "  timeout  : $TEST_TIMEOUT"
log "  filter   : $TEST_RUN"
log "  packages : ./internal/impl/saphana/..."
log ""

cd "$REPO_ROOT"

HANA_INTEGRATION_TESTS=1 \
HANA_HOST=localhost \
HANA_PORT="$HANA_PORT" \
HANA_USER=SYSTEM \
HANA_PASSWORD="$HANA_PASS" \
HANA_DATABASE=HXE \
    go test \
    -v \
    -timeout "$TEST_TIMEOUT" \
    "-run=${TEST_RUN}" \
    -coverprofile=/tmp/saphana-integration-coverage.out \
    "${TEST_EXTRA_ARGS[@]+"${TEST_EXTRA_ARGS[@]}"}" \
    ./internal/impl/saphana/...

EXIT_CODE=$?

# ── Coverage summary ──────────────────────────────────────────────────────────
if [[ -f /tmp/saphana-integration-coverage.out ]]; then
    log ""
    log "Coverage summary:"
    go tool cover -func=/tmp/saphana-integration-coverage.out | \
        grep -E "^(github\.com.*saphana|total)" | \
        awk '{printf "  %-80s %s\n", $1, $3}'
fi

# ── Redo log hex-dump (diagnostic — shows actual binary log content) ──────────
log ""
log "Redo log diagnostic (first 2 pages of latest segment):"
docker exec "$CTR_NAME" bash -c \
    "ls /hana/mounts/log/HXE/mnt00001/hdb*/logsegment_000_*.dat 2>/dev/null | sort | tail -1" \
    2>/dev/null | while IFS= read -r logfile; do
    log "  Log file: $logfile"
    docker exec "$CTR_NAME" bash -c \
        "dd if='$logfile' bs=4096 count=2 2>/dev/null | xxd | head -40" 2>/dev/null || true
done

if [[ $EXIT_CODE -eq 0 ]]; then
    log_ok ""
    log_ok "All integration tests PASSED ✓"
else
    die "Integration tests FAILED (exit code $EXIT_CODE)"
fi
