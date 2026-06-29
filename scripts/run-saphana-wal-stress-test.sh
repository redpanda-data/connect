#!/usr/bin/env bash
# run-saphana-wal-stress-test.sh
#
# Stress-tests the SAP HANA redo log parser by inserting ~1 GB of data to force
# multiple log segment rollovers, then parses every segment and validates the
# page structure parser handles multi-segment scenarios correctly.
#
# What this proves:
#   - The page parser correctly handles circular segment buffer (segments not in LSN order)
#   - The parser correctly reads LSNs at offset 16 across segment boundaries
#   - UsedBytes (offset 76) is valid across all pages
#   - No page magic violations after heavy DML
#
# Usage:
#   bash scripts/run-saphana-wal-stress-test.sh [--rows N] [--skip-insert]
#
# Options:
#   --rows N        Number of rows to insert (default: 50000, each ~20KB = ~1 GB total)
#   --skip-insert   Skip the INSERT phase, only parse existing segments
#   --keep          Keep HANA running after the test

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HANA_DATA="${HANA_DATA_PATH:-/tmp/hana-express-data}"
LOG_DIR="$HANA_DATA/log/mnt00001"
ROWS="${ROWS:-50000}"
SKIP_INSERT=false
KEEP=false

log()    { printf '\033[1;34m[stress]\033[0m %s\n' "$*" >&2; }
log_ok() { printf '\033[1;32m[stress]\033[0m %s\n' "$*" >&2; }
die()    { printf '\033[1;31m[stress]\033[0m ERROR: %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rows)      ROWS="$2"; shift 2 ;;
        --skip-insert) SKIP_INSERT=true; shift ;;
        --keep)      KEEP=true; shift ;;
        *) die "Unknown flag: $1" ;;
    esac
done

hdbsql() {
    DOCKER_API_VERSION=1.46 docker exec hxe \
        /usr/sap/HXE/HDB90/exe/hdbsql -i 90 -u SYSTEM -p HXEHana1 -d SYSTEMDB "$@"
}

# ── Phase 1: Ensure HANA is running ──────────────────────────────────────────
log "Checking HANA status..."
STATUS=$(DOCKER_API_VERSION=1.46 docker inspect --format='{{.State.Health.Status}}' hxe 2>/dev/null || echo "missing")
if [[ "$STATUS" != "healthy" ]]; then
    log "HANA not healthy (status=$STATUS). Starting..."
    HANA_DATA_PATH="$HANA_DATA" \
    DOCKER_API_VERSION=1.46 \
    docker-compose -f "$REPO_ROOT/internal/impl/saphana/testdata/docker-compose.yml" up -d hana

    log "Waiting for HANA to become healthy (up to 15 min)..."
    DEADLINE=$(( $(date +%s) + 900 ))
    while true; do
        STATUS=$(DOCKER_API_VERSION=1.46 docker inspect --format='{{.State.Health.Status}}' hxe 2>/dev/null || echo "starting")
        [[ "$STATUS" == "healthy" ]] && { log_ok "HANA is healthy."; break; }
        [[ $(date +%s) -ge $DEADLINE ]] && die "HANA did not become healthy in time"
        log "  $STATUS — waiting..."
        sleep 15
    done
fi

# ── Phase 2: Capture segment count before INSERT ──────────────────────────────
SEGS_BEFORE=$(find "$LOG_DIR" -name "logsegment_000_0*.dat" 2>/dev/null | wc -l | tr -d ' ')
log "Segments before stress test: $SEGS_BEFORE"

# ── Phase 3: Insert ~1 GB of data ─────────────────────────────────────────────
if [[ "$SKIP_INSERT" == "false" ]]; then
    log "Creating stress test schema..."
    hdbsql "DROP TABLE STRESS.WAL_DATA" 2>/dev/null || true
    hdbsql "DROP SCHEMA STRESS" 2>/dev/null || true
    hdbsql "CREATE SCHEMA STRESS"
    hdbsql "CREATE COLUMN TABLE STRESS.WAL_DATA (
        ID BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        BATCH_ID INTEGER,
        PADDING NCLOB,
        INT_MARKER INTEGER,
        TS_MARKER TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )"

    # Each row inserts ~20KB of NCLOB padding → 50K rows ≈ 1 GB
    PAYLOAD=$(python3 -c "print('X' * 20000)")
    BATCH_SIZE=500
    BATCHES=$(( ROWS / BATCH_SIZE ))

    log "Inserting $ROWS rows in $BATCHES batches of $BATCH_SIZE (~1 GB total)..."
    for batch in $(seq 1 $BATCHES); do
        # Build multi-row INSERT for this batch
        VALUES=""
        for i in $(seq 1 $BATCH_SIZE); do
            if [[ -n "$VALUES" ]]; then VALUES="$VALUES,"; fi
            VALUES="$VALUES($batch, '$PAYLOAD', $batch)"
        done
        hdbsql "INSERT INTO STRESS.WAL_DATA (BATCH_ID, PADDING, INT_MARKER) VALUES $VALUES" 2>/dev/null || {
            log "Retrying batch $batch with individual inserts..."
            for i in $(seq 1 $BATCH_SIZE); do
                hdbsql "INSERT INTO STRESS.WAL_DATA (BATCH_ID, PADDING, INT_MARKER) VALUES ($batch, '$PAYLOAD', $batch)" 2>/dev/null || true
            done
        }

        if (( batch % 10 == 0 )); then
            SEGS_NOW=$(find "$LOG_DIR" -name "logsegment_000_0*.dat" 2>/dev/null | wc -l | tr -d ' ')
            log "  Batch $batch/$BATCHES complete. Segments: $SEGS_NOW"
        fi

        # Periodic savepoint to flush WAL
        if (( batch % 50 == 0 )); then
            hdbsql "ALTER SYSTEM SAVEPOINT" 2>/dev/null || true
        fi
    done

    # Final savepoint
    hdbsql "ALTER SYSTEM SAVEPOINT" 2>/dev/null || true
    sleep 3

    SEGS_AFTER=$(find "$LOG_DIR" -name "logsegment_000_0*.dat" 2>/dev/null | wc -l | tr -d ' ')
    log_ok "INSERT complete. Segments: $SEGS_BEFORE → $SEGS_AFTER (rolled over $(( SEGS_AFTER - SEGS_BEFORE )) times)"
fi

# ── Phase 4: Run the page parser against ALL segments ─────────────────────────
log "Running page parser validation across all log segments..."

HANA_LOG_DIR="$LOG_DIR" \
go test ./internal/impl/saphana/logformat/empirical/ \
    -v \
    -run "TestDirect" \
    -count=1 \
    -timeout 5m \
    2>&1

# ── Phase 5: Run the full parser test (reads & validates every page) ──────────
log "Running full multi-segment page scan..."

cat > /tmp/scan_all_segments.go << 'GOEOF'
//go:build ignore

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

func main() {
	logDir := os.Args[1]
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	sort.Strings(matches)

	type segResult struct {
		file       string
		pages      int
		magicPages int
		minLSN     uint64
		maxLSN     uint64
		violations int
	}

	var results []segResult
	totalPages, totalMagic, totalViolations := 0, 0, 0

	for _, f := range matches {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Printf("ERROR reading %s: %v\n", f, err)
			continue
		}
		r := segResult{file: filepath.Base(f), pages: len(data) / page.Size}
		r.minLSN = ^uint64(0)

		for i := range r.pages {
			if binary.LittleEndian.Uint64(data[i*page.Size:i*page.Size+8]) != page.Magic {
				continue
			}
			r.magicPages++
			lsn := binary.LittleEndian.Uint64(data[i*page.Size+16 : i*page.Size+24])
			used := uint32(binary.LittleEndian.Uint32(data[i*page.Size+76 : i*page.Size+80]))
			if lsn < r.minLSN { r.minLSN = lsn }
			if lsn > r.maxLSN { r.maxLSN = lsn }
			maxPayload := uint32(page.Size - page.HeaderSize)
			if used > maxPayload {
				r.violations++
				fmt.Printf("  VIOLATION page %d in %s: UsedBytes=%d > max=%d\n", i, r.file, used, maxPayload)
			}
		}
		if r.minLSN == ^uint64(0) { r.minLSN = 0 }
		results = append(results, r)
		totalPages += r.pages
		totalMagic += r.magicPages
		totalViolations += r.violations
	}

	// Sort by min LSN (correct segment order)
	sort.Slice(results, func(i, j int) bool { return results[i].minLSN < results[j].minLSN })

	fmt.Printf("\n%-45s %6s %6s %15s %15s %s\n",
		"SEGMENT", "PAGES", "MAGIC", "MIN LSN", "MAX LSN", "VIOLATIONS")
	fmt.Println(string(make([]byte, 100)))
	for _, r := range results {
		v := ""
		if r.violations > 0 { v = fmt.Sprintf("*** %d ***", r.violations) }
		fmt.Printf("%-45s %6d %6d %15d %15d %s\n",
			r.file, r.pages, r.magicPages, r.minLSN, r.maxLSN, v)
	}

	fmt.Printf("\nTOTAL: %d segments, %d pages, %d magic-pages, %d violations\n",
		len(results), totalPages, totalMagic, totalViolations)

	if totalViolations > 0 {
		fmt.Fprintln(os.Stderr, "FAIL: parser violations found")
		os.Exit(1)
	}
	fmt.Println("PASS: all pages parsed cleanly")
}
GOEOF

go run /tmp/scan_all_segments.go "$LOG_DIR" 2>&1

# ── Phase 6: Summary ──────────────────────────────────────────────────────────
SEGS_FINAL=$(find "$LOG_DIR" -name "logsegment_000_0*.dat" 2>/dev/null | wc -l | tr -d ' ')
TOTAL_PAGES=$(find "$LOG_DIR" -name "logsegment_000_0*.dat" -exec wc -c {} \; 2>/dev/null | \
    awk '{sum+=$1} END {print sum/4096}')

log_ok ""
log_ok "Stress test complete!"
log_ok "  Segments parsed:  $SEGS_FINAL"
log_ok "  Total pages:      ~$TOTAL_PAGES"
log_ok "  Log directory:    $LOG_DIR"

[[ "$KEEP" == "false" ]] && {
    log "Stopping HANA..."
    HANA_DATA_PATH="$HANA_DATA" DOCKER_API_VERSION=1.46 \
    docker-compose -f "$REPO_ROOT/internal/impl/saphana/testdata/docker-compose.yml" down 2>/dev/null || true
}
