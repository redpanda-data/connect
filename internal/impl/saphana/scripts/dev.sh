#!/usr/bin/env bash
# dev.sh — SAP HANA CDC end-to-end dev script for Mac
#
# Starts HANA Express in Docker, installs CDC schemas, seeds test data,
# and streams Debezium-format JSON events to stdout.
#
# Usage:
#   bash dev.sh               # full demo: start → setup → seed → stream
#   bash dev.sh start         # start HANA only
#   bash dev.sh setup         # install CDC schemas (HANA must be running)
#   bash dev.sh seed          # insert test rows
#   bash dev.sh stream        # run connector (streams JSON to stdout)
#   bash dev.sh hexdump       # hex-dump first 3 pages of latest redo log
#   bash dev.sh stop          # stop containers
#
# Requirements:
#   - Docker Desktop running
#   - linux/amd64 emulation (Rosetta on Apple Silicon)
#   - 6+ GB RAM available to Docker
#   - ~20 GB free disk for HANA data volume

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DC_FILE="$REPO_ROOT/internal/impl/saphana/testdata/docker-compose.yml"
HANA_DATA="${HANA_DATA_PATH:-/tmp/hana-express-data}"
HANA_USER="${HANA_USER:-SYSTEM}"
HANA_PASS="${HANA_PASS:-HXEHana1}"
HANA_HOST="${HANA_HOST:-localhost}"
HANA_PORT="${HANA_PORT:-39015}"
HANA_DB="${HANA_DB:-HXE}"

log()  { printf '[dev.sh] %s\n' "$*" >&2; }
die()  { log "ERROR: $*"; exit 1; }

hdbsql() {
    docker exec hxe /usr/sap/HXE/HDB90/exe/hdbsql \
        -i 90 -u "$HANA_USER" -p "$HANA_PASS" -d "$HANA_DB" "$@"
}

wait_healthy() {
    log "Waiting for HANA to be healthy (up to 10 min)..."
    for i in $(seq 1 60); do
        status=$(docker inspect --format='{{.State.Health.Status}}' hxe 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            log "HANA is healthy."
            return 0
        fi
        log "  ($i/60) status=$status ..."
        sleep 10
    done
    die "HANA did not become healthy. Check: docker logs hxe"
}

cmd_start() {
    log "Creating data directory: $HANA_DATA"
    mkdir -p "$HANA_DATA"

    log "Copying password file..."
    cp "$SCRIPT_DIR/../testdata/hxepasswd.json" "$HANA_DATA/hxepasswd.json"
    chmod 600 "$HANA_DATA/hxepasswd.json"

    # Copy SQL files so hdbsql can read them from inside the container.
    for f in "$SCRIPT_DIR/../sql/"*.sql; do
        cp "$f" "$HANA_DATA/"
    done

    log "Starting HANA Express (linux/amd64, Rosetta on Apple Silicon)..."
    HANA_DATA_PATH="$HANA_DATA" docker-compose -f "$DC_FILE" up -d hana
    wait_healthy
}

cmd_setup() {
    log "Installing test schemas and CDC verification triggers..."
    hdbsql -I /hana/mounts/01_schema.sql
    hdbsql -I /hana/mounts/02_verify_schema.sql
    hdbsql -I /hana/mounts/03_triggers.sql

    log "Taking initial backup to activate log segment retention..."
    hdbsql "BACKUP DATA USING FILE ('/hana/mounts/backup/FULL')" 2>/dev/null || \
        log "Backup failed or already exists — continuing."

    log "Fixing log file permissions (HANA sets 600; sidecar needs 640)..."
    docker exec hxe bash -c \
        "find /hana/mounts/log -name 'logsegment_*.dat' -exec chmod 640 {} \;" 2>/dev/null || true

    log "CDC setup complete. Log files:"
    docker exec hxe bash -c "ls -la /hana/mounts/log/HXE/mnt00001/hdb*/logsegment_000_*.dat 2>/dev/null | head -5" || true
}

cmd_seed() {
    log "Seeding test data to generate CDC events..."
    hdbsql "INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, INT_VAL) VALUES (1, 'Alice', 100)"
    hdbsql "INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, INT_VAL) VALUES (2, 'Bob', 200)"
    hdbsql "UPDATE CDC_TEST.BASIC SET STR_VAL = 'Alice Smith' WHERE ID = 1"
    hdbsql "DELETE FROM CDC_TEST.BASIC WHERE ID = 2"
    hdbsql "UPSERT CDC_TEST.BASIC (ID, STR_VAL, INT_VAL) VALUES (3, 'Carol', 300) WITH PRIMARY KEY"

    log "Raw CDC events in _RPCN_CDC.CHANGES (if triggers installed):"
    hdbsql "SELECT ID, OP, SCHEMA_NAME, TABLE_NAME FROM _RPCN_CDC.CHANGES ORDER BY ID" 2>/dev/null || \
        log "Note: run 'setup' first to install the CDC change table."

    log "Raw verification events in _CDC_VERIFY.CHANGES:"
    hdbsql "SELECT ID, OP, SCHEMA_NAME, TABLE_NAME FROM _CDC_VERIFY.CHANGES ORDER BY ID" 2>/dev/null || true
}

cmd_stream() {
    log "Building connector binary..."
    (cd "$REPO_ROOT" && go build -o /tmp/saphana-connect ./cmd/redpanda-connect)

    log "Streaming CDC events as JSON (Ctrl-C to stop)..."
    cat > /tmp/saphana-cdc-config.yaml <<EOF
input:
  saphana_cdc:
    dsn: "hdb://${HANA_USER}:${HANA_PASS}@${HANA_HOST}:${HANA_PORT}?databaseName=${HANA_DB}"
    tables:
      - "CDC_TEST.BASIC"
    snapshot_mode: initial

output:
  stdout:
    codec: lines
EOF
    /tmp/saphana-connect run /tmp/saphana-cdc-config.yaml
}

cmd_hexdump() {
    log "Hex-dumping first 3 pages of the most recent redo log segment..."
    go run "$REPO_ROOT/internal/impl/saphana/scripts/hexdump_main.go" \
        --dir="$HANA_DATA/log/HXE/mnt00001" \
        --pages=3
}

cmd_stop() {
    HANA_DATA_PATH="$HANA_DATA" docker-compose -f "$DC_FILE" down
    log "HANA stopped."
}

ACTION="${1:-all}"
case "$ACTION" in
    start)   cmd_start ;;
    setup)   cmd_setup ;;
    seed)    cmd_seed ;;
    stream)  cmd_stream ;;
    hexdump) cmd_hexdump ;;
    stop)    cmd_stop ;;
    all)
        cmd_start
        cmd_setup
        cmd_seed
        cmd_stream
        ;;
    *)
        die "Unknown action '$ACTION'. Valid: start|setup|seed|stream|hexdump|stop|all"
        ;;
esac
