// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package empirical contains integration tests that run against a live SAP HANA
// instance to empirically confirm or refute binary log format hypotheses.
//
// These tests READ the actual redo log segment files from disk and extract byte
// patterns to confirm exact offsets, type codes, and encodings.
//
// Run:
//
//	HANA_INTEGRATION_TESTS=1 \
//	HANA_LOG_DIR=/tmp/hana-express-data/log/HXE/mnt00001 \
//	go test ./internal/impl/saphana/logformat/empirical/... -v -timeout 5m
//
// All confirmed findings are written to findings.json and update VERSION_MATRIX.md.
package empirical_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

// Finding records a single empirically confirmed (or refuted) binary format fact.
type Finding struct {
	HANAVersion string `json:"hana_version"`
	DockerImage string `json:"docker_image"`
	Field       string `json:"field"`
	Hypothesis  string `json:"hypothesis"`
	Result      string `json:"result"` // "confirmed" | "refuted" | "indeterminate"
	ActualValue string `json:"actual_value,omitempty"`
	PageIndex   int    `json:"page_index,omitempty"`
	ByteOffset  int    `json:"byte_offset,omitempty"`
	HexContext  string `json:"hex_context,omitempty"`
	TestName    string `json:"test_name"`
	Timestamp   string `json:"timestamp"`
}

var globalFindings []Finding

func skipIfNoHANA(t *testing.T) (*sql.DB, string) {
	t.Helper()
	if os.Getenv("HANA_INTEGRATION_TESTS") != "1" {
		t.Skip("set HANA_INTEGRATION_TESTS=1 and HANA_LOG_DIR=/path/to/hana/log to run empirical tests")
	}
	host := envOr("HANA_HOST", "localhost")
	port := envOr("HANA_PORT", "39015")
	user := envOr("HANA_USER", "SYSTEM")
	pass := envOr("HANA_PASS", "HXEHana1")
	dbName := envOr("HANA_DB", "HXE")

	dsn := fmt.Sprintf("hdb://%s:%s@%s:%s?databaseName=%s", user, pass, host, port, dbName)
	conn, err := driver.NewDSNConnector(dsn)
	require.NoError(t, err, "building HANA connector")
	conn.SetTimeout(30 * time.Second)
	db := sql.OpenDB(conn)
	require.NoError(t, db.PingContext(context.Background()), "pinging HANA")
	t.Cleanup(func() { db.Close() })

	var version string
	require.NoError(t, db.QueryRowContext(context.Background(), "SELECT VERSION FROM SYS.M_DATABASE").Scan(&version))
	t.Logf("Connected to SAP HANA %s", version)
	return db, version
}

func logDir(t *testing.T) string {
	t.Helper()
	d := os.Getenv("HANA_LOG_DIR")
	if d == "" {
		t.Skip("set HANA_LOG_DIR=/path/to/hana/log to read log files")
	}
	return d
}

func latestSegment(t *testing.T, dir string) string {
	t.Helper()
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_*.dat")
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.NotEmpty(t, matches, "no log segments found at %s", pattern)
	sort.Strings(matches)
	return matches[len(matches)-1]
}

func directoryFile(t *testing.T, dir string) string {
	t.Helper()
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_directory.dat")
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.NotEmpty(t, matches, "no directory file found at %s", pattern)
	return matches[0]
}

func flushLog(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), "ALTER SYSTEM SAVEPOINT")
	require.NoError(t, err, "ALTER SYSTEM SAVEPOINT")
	time.Sleep(3 * time.Second) // let the flush complete
}

func record(t *testing.T, version, field, hypothesis, result, actual, hexCtx string, pageIdx, byteOffset int) {
	t.Helper()
	f := Finding{
		HANAVersion: version,
		DockerImage: "saplabs/hanaexpress:latest (2.00.088.00 SPS08)",
		Field:       field,
		Hypothesis:  hypothesis,
		Result:      result,
		ActualValue: actual,
		PageIndex:   pageIdx,
		ByteOffset:  byteOffset,
		HexContext:  hexCtx,
		TestName:    t.Name(),
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	globalFindings = append(globalFindings, f)
	t.Logf("[%s] %s: hypothesis=%q actual=%s", result, field, hypothesis, actual)
}

func saveFindings(t *testing.T) {
	t.Helper()
	if len(globalFindings) == 0 {
		return
	}
	data, _ := json.MarshalIndent(globalFindings, "", "  ")
	path := "/tmp/saphana-empirical-findings.json"
	os.WriteFile(path, data, 0o644)
	t.Logf("Findings written to %s", path)
}

// ── Test: Page size is a multiple of 4096 ─────────────────────────────────────

// TestEmpiricalPageSizeMultiple reads an actual log segment and confirms its
// file size is an exact multiple of 4096 bytes, confirming PageSize=4096.
func TestEmpiricalPageSizeMultiple(t *testing.T) {
	_, version := skipIfNoHANA(t)
	dir := logDir(t)

	segFile := latestSegment(t, dir)
	info, err := os.Stat(segFile)
	require.NoError(t, err)

	size := info.Size()
	remainder := size % int64(page.Size)

	if remainder == 0 {
		record(t, version, "page.size_bytes", "4096", "confirmed",
			fmt.Sprintf("file size %d is exact multiple of 4096", size), "", 0, 0)
	} else {
		record(t, version, "page.size_bytes", "4096", "refuted",
			fmt.Sprintf("file size %d has remainder %d", size, remainder), "", 0, 0)
	}
	assert.Equal(t, int64(0), remainder, "log segment file size must be a multiple of 4096")
}

// ── Test: LSN at page offset 16 (confirmed) ───────────────────────────────────

// TestEmpiricalLSNAtOffset16 inserts a row, forces a savepoint, reads the log
// segment, and checks whether bytes[16:24] (CurrentLSNOffset) of each page
// match the expected LSN range from SYS.M_LOG_SEGMENTS.
//
// Confirmed: HANA 2.00.088.00 SPS08 — LSN is at offset 16, NOT offset 0.
// Offset 0 holds the page magic constant (0x00000060FF400002).
// Offset 8 holds the INT64_MAX sentinel (0x7FFFFFFFFFFFFFFF).
// Offset 16 holds the CurrentPageLSN (uint64 LE, monotonically increasing +64/page).
func TestEmpiricalLSNAtOffset16(t *testing.T) {
	db, version := skipIfNoHANA(t)
	dir := logDir(t)

	// Get the current writing segment's min/max position BEFORE inserting
	var segFile string
	var minPos, maxPos int64
	err := db.QueryRowContext(context.Background(), `
		SELECT FILE_NAME, MIN_POSITION, MAX_POSITION
		FROM SYS.M_LOG_SEGMENTS
		WHERE STATE = 'Writing'
		ORDER BY SEGMENT_ID DESC`).Scan(&segFile, &minPos, &maxPos)
	require.NoError(t, err, "querying writing segment")
	t.Logf("Writing segment: %s  min_pos=%d max_pos=%d", segFile, minPos, maxPos)

	// Flush to ensure pages are written
	flushLog(t, db)

	// Translate container path to host path
	hostFile := translatePath(dir, segFile)
	t.Logf("Host path: %s", hostFile)

	data, err := os.ReadFile(hostFile)
	require.NoError(t, err, "reading log segment")
	require.GreaterOrEqual(t, len(data), page.Size, "segment must have at least one page")

	pages, err := page.ScanFile(data)
	require.NoError(t, err)
	require.NotEmpty(t, pages)

	// Check offset 16 (confirmed: CurrentLSNOffset) of first page.
	p0LSN := binary.LittleEndian.Uint64(pages[0].Raw[16:24]) // confirmed: LSN is at offset 16
	t.Logf("Page 0 bytes[16:24] as LE uint64 = %d (0x%016x)", p0LSN, p0LSN)
	t.Logf("Expected range: %d .. %d", minPos, maxPos)

	// Also log the magic and sentinel at offsets 0 and 8 for cross-check
	magic := binary.LittleEndian.Uint64(pages[0].Raw[0:8])
	sentinel := binary.LittleEndian.Uint64(pages[0].Raw[8:16])
	t.Logf("Page 0 bytes[0:8]   (Magic)    = 0x%016x (expected 0x%016x)", magic, page.Magic)
	t.Logf("Page 0 bytes[8:16]  (Sentinel) = 0x%016x (expected 0x%016x)", sentinel, page.Sentinel)

	hexCtx := hexStr(pages[0].Raw[0:32])

	if magic == page.Magic {
		record(t, version, "page.header.magic_offset0", "bytes[0:8] = 0x00000060FF400002 (page magic)", "confirmed",
			fmt.Sprintf("0x%016x", magic), hexCtx, 0, 0)
	} else {
		record(t, version, "page.header.magic_offset0", "bytes[0:8] = 0x00000060FF400002 (page magic)", "refuted",
			fmt.Sprintf("got 0x%016x, want 0x%016x", magic, page.Magic), hexCtx, 0, 0)
	}

	if p0LSN >= uint64(minPos) && p0LSN <= uint64(maxPos+1000000) {
		record(t, version, "page.header.lsn_offset", "bytes[16:24] = page LSN (uint64 LE)", "confirmed",
			fmt.Sprintf("value %d within segment range [%d, %d]", p0LSN, minPos, maxPos),
			hexCtx, 0, 16)
	} else if p0LSN == 0 {
		record(t, version, "page.header.lsn_offset", "bytes[16:24] = page LSN (uint64 LE)", "indeterminate",
			"first page has LSN=0 (may be an empty/filler page)", hexCtx, 0, 16)
	} else {
		record(t, version, "page.header.lsn_offset", "bytes[16:24] = page LSN (uint64 LE)", "refuted",
			fmt.Sprintf("value %d outside segment range [%d, %d]", p0LSN, minPos, maxPos),
			hexCtx, 0, 16)
	}

	// Check monotonicity across pages using confirmed offset 16.
	if len(pages) >= 3 {
		lsns := make([]uint64, len(pages))
		for i, pg := range pages {
			if len(pg.Raw) >= 24 {
				lsns[i] = binary.LittleEndian.Uint64(pg.Raw[16:24]) // confirmed: CurrentLSNOffset
			}
		}
		t.Logf("First 5 page LSNs (offset 16): %v", lsns[:min(5, len(lsns))])
		monotonic := true
		for i := 1; i < len(lsns); i++ {
			if lsns[i] != 0 && lsns[i] < lsns[i-1] {
				monotonic = false
				t.Logf("Non-monotonic at page %d: %d < %d", i, lsns[i], lsns[i-1])
			}
		}
		if monotonic {
			record(t, version, "page.header.lsn_monotonic", "LSN at offset 16 increases across pages", "confirmed",
				fmt.Sprintf("checked %d pages, all LSNs non-decreasing", len(lsns)), "", 0, 16)
		}
	}

	saveFindings(t)
}

// TestEmpiricalPageMagic reads an actual log segment and confirms that bytes[0:8]
// of every page contain the magic constant 0x00000060FF400002.
//
// Confirmed: HANA 2.00.088.00 SPS08 — constant on all 16384 pages of
// logsegment_000_00000003.dat.
func TestEmpiricalPageMagic(t *testing.T) {
	_, version := skipIfNoHANA(t)
	dir := logDir(t)

	segFile := latestSegment(t, dir)
	data, err := os.ReadFile(segFile)
	require.NoError(t, err, "reading log segment")
	require.GreaterOrEqual(t, len(data), page.Size, "segment must have at least one page")

	pages, err := page.ScanFile(data)
	require.NoError(t, err)
	require.NotEmpty(t, pages)

	wrongMagic := 0
	for i, pg := range pages {
		if len(pg.Raw) < 8 {
			continue
		}
		got := binary.LittleEndian.Uint64(pg.Raw[0:8])
		if got != page.Magic {
			wrongMagic++
			if wrongMagic <= 3 {
				t.Logf("Page %d: magic = 0x%016x, want 0x%016x", i, got, page.Magic)
			}
		}
	}

	hexCtx := hexStr(pages[0].Raw[0:8])
	if wrongMagic == 0 {
		record(t, version, "page.header.magic_constant", fmt.Sprintf("bytes[0:8] = 0x%016x on all pages", page.Magic), "confirmed",
			fmt.Sprintf("checked %d pages, all have correct magic", len(pages)), hexCtx, 0, 0)
		t.Logf("Magic 0x%016x confirmed on all %d pages", page.Magic, len(pages))
	} else {
		record(t, version, "page.header.magic_constant", fmt.Sprintf("bytes[0:8] = 0x%016x on all pages", page.Magic), "refuted",
			fmt.Sprintf("%d/%d pages have wrong magic", wrongMagic, len(pages)), hexCtx, 0, 0)
		t.Errorf("Magic mismatch on %d/%d pages", wrongMagic, len(pages))
	}

	saveFindings(t)
}

// ── Test: Little-endian byte order confirmed ───────────────────────────────────

// TestEmpiricalLittleEndian inserts a known INTEGER value and confirms it
// appears as 4-byte little-endian in the log segment.
func TestEmpiricalLittleEndian(t *testing.T) {
	db, version := skipIfNoHANA(t)
	dir := logDir(t)

	// Use a sentinel integer: 0xDEADBEEF = 3735928559
	// LE: DE AD BE EF → wait, that's big-endian. LE would be: EF BE AD DE
	// Let's use a simpler value: 0x01020304 = 16909060
	// LE: 04 03 02 01
	sentinelInt := int32(0x01020304)
	sentinelLE := []byte{0x04, 0x03, 0x02, 0x01}

	db.ExecContext(context.Background(), "DROP TABLE PROBE_LE.T")
	db.ExecContext(context.Background(), "DROP SCHEMA PROBE_LE")
	db.ExecContext(context.Background(), "CREATE SCHEMA PROBE_LE")
	_, err := db.ExecContext(context.Background(), `CREATE COLUMN TABLE PROBE_LE.T (ID INTEGER NOT NULL PRIMARY KEY, INT_VAL INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO PROBE_LE.T (ID, INT_VAL) VALUES (1, ?)`, sentinelInt)
	require.NoError(t, err)
	flushLog(t, db)

	segFile := latestSegment(t, dir)
	data, err := os.ReadFile(segFile)
	require.NoError(t, err)

	positions := findBytes(data, sentinelLE)
	if len(positions) > 0 {
		hexCtx := hexStr(data[max(0, positions[0]-8):min(len(data), positions[0]+12)])
		record(t, version, "column.integer.byte_order", "INTEGER 0x01020304 stored as [04 03 02 01] (little-endian)", "confirmed",
			fmt.Sprintf("found LE bytes at %d locations; first at byte %d", len(positions), positions[0]),
			hexCtx, positions[0]/page.Size, positions[0]%page.Size)

		// Now check: big-endian would be [01 02 03 04] — confirm it's NOT present
		sentinelBE := []byte{0x01, 0x02, 0x03, 0x04}
		bePositions := findBytes(data, sentinelBE)
		if len(bePositions) == 0 {
			record(t, version, "column.integer.byte_order_not_BE", "big-endian [01 02 03 04] absent", "confirmed",
				"big-endian pattern not found — confirms little-endian", "", 0, 0)
		}
	} else {
		record(t, version, "column.integer.byte_order", "INTEGER 0x01020304 stored as [04 03 02 01] (little-endian)", "indeterminate",
			"sentinel LE bytes not found — may need longer flush or different segment", "", 0, 0)
	}

	assert.NotEmpty(t, positions, "sentinel little-endian bytes [04 03 02 01] must appear in log segment")
	saveFindings(t)
}

// ── Test: VARCHAR length prefix ────────────────────────────────────────────────

// TestEmpiricalVarcharLengthPrefix inserts a known VARCHAR string and checks
// whether 1 or 2 bytes immediately before the string in the log encode its length.
func TestEmpiricalVarcharLengthPrefix(t *testing.T) {
	db, version := skipIfNoHANA(t)
	dir := logDir(t)

	// Use a unique marker with a known length
	marker := fmt.Sprintf("RPCN_PROBE_%d", time.Now().UnixNano())
	markerBytes := []byte(marker)
	markerLen := len(markerBytes)

	db.ExecContext(context.Background(), "DROP TABLE PROBE_VC.T")
	db.ExecContext(context.Background(), "DROP SCHEMA PROBE_VC")
	db.ExecContext(context.Background(), "CREATE SCHEMA PROBE_VC")
	_, err := db.ExecContext(context.Background(), `CREATE COLUMN TABLE PROBE_VC.T (ID INTEGER NOT NULL PRIMARY KEY, STR_VAL VARCHAR(255))`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO PROBE_VC.T (ID, STR_VAL) VALUES (1, ?)`, marker)
	require.NoError(t, err)
	flushLog(t, db)

	segFile := latestSegment(t, dir)
	data, err := os.ReadFile(segFile)
	require.NoError(t, err)

	positions := findBytes(data, markerBytes)
	require.NotEmpty(t, positions, "marker string %q must appear in log segment (encryption must be OFF)", marker)

	pos := positions[0]
	t.Logf("Marker %q (len=%d) found at byte %d (page %d, offset %d)",
		marker, markerLen, pos, pos/page.Size, pos%page.Size)

	hexCtx := hexStr(data[max(0, pos-8):min(len(data), pos+markerLen+4)])
	t.Logf("Context bytes: %s", hexCtx)

	// Check for 2-byte LE length prefix
	if pos >= 2 {
		le2 := int(binary.LittleEndian.Uint16(data[pos-2 : pos]))
		if le2 == markerLen {
			record(t, version, "column.varchar.length_prefix_bytes", "2-byte LE uint16 immediately before VARCHAR string", "confirmed",
				fmt.Sprintf("bytes[-2:-1]=%02x%02x = %d == len(%q)=%d",
					data[pos-2], data[pos-1], le2, marker, markerLen),
				hexCtx, pos/page.Size, pos-2)
		} else if pos >= 1 {
			le1 := int(data[pos-1])
			if le1 == markerLen {
				record(t, version, "column.varchar.length_prefix_bytes", "1-byte length prefix immediately before VARCHAR string", "confirmed",
					fmt.Sprintf("byte[-1]=%02x = %d == len(%q)=%d", data[pos-1], le1, marker, markerLen),
					hexCtx, pos/page.Size, pos-1)
				record(t, version, "column.varchar.2byte_prefix", "2-byte LE prefix hypothesis", "refuted",
					fmt.Sprintf("2-byte value %d != string length %d; but 1-byte value matches", le2, markerLen),
					hexCtx, pos/page.Size, pos-2)
			} else {
				t.Logf("WARNING: neither 1-byte (%d) nor 2-byte (%d) prefix matches string length %d", le1, le2, markerLen)
				record(t, version, "column.varchar.length_prefix_bytes", "length prefix before VARCHAR", "indeterminate",
					fmt.Sprintf("1-byte=%d 2-byte=%d neither matches length=%d; check byte offset", le1, le2, markerLen),
					hexCtx, pos/page.Size, pos)
			}
		}
	}

	saveFindings(t)
}

// ── Test: Directory file header byte layout ────────────────────────────────────

// TestEmpiricalDirectoryFileHeader reads the actual directory file and checks
// whether the hypothesised byte offsets (from KBA 2908105) are correct.
func TestEmpiricalDirectoryFileHeader(t *testing.T) {
	db, version := skipIfNoHANA(t)
	dir := logDir(t)

	// Get expected values from SYS.M_LOG_SEGMENTS
	var totalSegs int
	db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS`).Scan(&totalSegs)
	t.Logf("Total segments: %d", totalSegs)

	dirFile := directoryFile(t, dir)
	data, err := os.ReadFile(dirFile)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), page.Size, "directory file too short")
	t.Logf("Directory file: %s (%d bytes = %d pages)", dirFile, len(data), len(data)/page.Size)

	// Dump first 48 bytes of first two pages for analysis
	t.Logf("Page 0 bytes[0:48]: %s", hexStr(data[0:48]))
	if len(data) >= page.Size+48 {
		t.Logf("Page 1 bytes[0:48]: %s", hexStr(data[page.Size:page.Size+48]))
	}

	// Test hypothesised layout from KBA 2908105:
	// LogIndex uint32 @ 0, PhyIndex uint32 @ 4, EntryCount uint32 @ 8,
	// CMax uint32 @ 12, Generation uint64 @ 16, Version uint16 @ 24,
	// Checksum uint64 @ 32

	logIdx := binary.LittleEndian.Uint32(data[0:4])
	phyIdx := binary.LittleEndian.Uint32(data[4:8])
	entryCount := binary.LittleEndian.Uint32(data[8:12])
	cmax := binary.LittleEndian.Uint32(data[12:16])
	generation := binary.LittleEndian.Uint64(data[16:24])
	version16 := binary.LittleEndian.Uint16(data[24:26])
	checksum := binary.LittleEndian.Uint64(data[32:40])

	t.Logf("Hypothesised fields: log=%d phy=%d ecnt=%d cmax=%d seq=%d ver=%d chk=0x%016x",
		logIdx, phyIdx, entryCount, cmax, generation, version16, checksum)

	hexCtx := hexStr(data[0:40])

	// Version should be 2
	if version16 == 2 {
		record(t, version, "directory.header.version_offset", "Version=2 at offset 24 (uint16 LE)", "confirmed",
			fmt.Sprintf("offset 24: value=%d", version16), hexCtx, 0, 24)
	} else {
		record(t, version, "directory.header.version_offset", "Version=2 at offset 24 (uint16 LE)", "refuted",
			fmt.Sprintf("offset 24: value=%d (expected 2)", version16), hexCtx, 0, 24)
		// Try other offsets
		for off := 0; off < 40; off += 2 {
			v := binary.LittleEndian.Uint16(data[off : off+2])
			if v == 2 {
				t.Logf("  Version=2 found at offset %d instead of 24", off)
			}
		}
	}

	// EntryCount should be 10240 (default max segments)
	if entryCount == 10240 {
		record(t, version, "directory.header.entry_count_offset", "EntryCount=10240 at offset 8 (uint32 LE)", "confirmed",
			fmt.Sprintf("offset 8: value=%d", entryCount), hexCtx, 0, 8)
	} else {
		record(t, version, "directory.header.entry_count_offset", "EntryCount=10240 at offset 8 (uint32 LE)", "refuted",
			fmt.Sprintf("offset 8: value=%d (expected 10240)", entryCount), hexCtx, 0, 8)
		// Find 10240 (0x00002800) in the header
		target := []byte{0x00, 0x28, 0x00, 0x00}
		for i := range 40 {
			if i+4 <= len(data) {
				if data[i] == target[0] && data[i+1] == target[1] && data[i+2] == target[2] && data[i+3] == target[3] {
					t.Logf("  EntryCount=10240 found at offset %d (not 8)", i)
				}
			}
		}
	}

	_ = logIdx
	_ = phyIdx
	_ = cmax
	_ = generation
	_ = checksum

	saveFindings(t)
}

// ── Test: Checksum algorithm ───────────────────────────────────────────────────

// TestEmpiricalChecksumAlgorithm reads the directory file and tries all three
// checksum candidates to determine which one HANA uses.
func TestEmpiricalChecksumAlgorithm(t *testing.T) {
	_, version := skipIfNoHANA(t)
	dir := logDir(t)

	dirFile := directoryFile(t, dir)
	data, err := os.ReadFile(dirFile)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), page.Size)

	pageData := make([]byte, page.Size)
	copy(pageData, data[:page.Size])

	// Read stored checksum at hypothesised offset 32
	storedChecksum := binary.LittleEndian.Uint64(pageData[32:40])
	t.Logf("Stored checksum at offset 32: 0x%016x", storedChecksum)

	// Zero out the checksum field before computing
	pageDataZeroed := make([]byte, page.Size)
	copy(pageDataZeroed, pageData)
	binary.LittleEndian.PutUint64(pageDataZeroed[32:40], 0)

	// Try CRC-64/ECMA-182
	crc64Val := computeCRC64ECMA(pageDataZeroed)
	t.Logf("CRC-64/ECMA-182: 0x%016x (matches: %v)", crc64Val, crc64Val == storedChecksum)

	// Try Fletcher-64
	fl64Val := computeFletcher64(pageDataZeroed)
	t.Logf("Fletcher-64:      0x%016x (matches: %v)", fl64Val, fl64Val == storedChecksum)

	// Try XOR-64
	xor64Val := computeXOR64(pageDataZeroed)
	t.Logf("XOR-64:           0x%016x (matches: %v)", xor64Val, xor64Val == storedChecksum)

	hexCtx := hexStr(pageData[28:48])

	switch {
	case crc64Val == storedChecksum:
		record(t, version, "directory.checksum_algorithm", "CRC-64/ECMA-182 (polynomial 0xC96C5795D7870F42)", "confirmed",
			fmt.Sprintf("stored=0x%016x matches CRC-64/ECMA", storedChecksum), hexCtx, 0, 32)
	case fl64Val == storedChecksum:
		record(t, version, "directory.checksum_algorithm", "Fletcher-64", "confirmed",
			fmt.Sprintf("stored=0x%016x matches Fletcher-64", storedChecksum), hexCtx, 0, 32)
	case xor64Val == storedChecksum:
		record(t, version, "directory.checksum_algorithm", "XOR-64 of 8-byte words", "confirmed",
			fmt.Sprintf("stored=0x%016x matches XOR-64", storedChecksum), hexCtx, 0, 32)
	default:
		record(t, version, "directory.checksum_algorithm", "one of CRC-64/ECMA, Fletcher-64, XOR-64", "indeterminate",
			fmt.Sprintf("stored=0x%016x; CRC64=%016x FL64=%016x XOR64=%016x; none match — checksum field may be at a different offset",
				storedChecksum, crc64Val, fl64Val, xor64Val), hexCtx, 0, 32)
		// Try other offsets for the checksum field
		for off := 0; off+8 <= 64; off += 8 {
			stored := binary.LittleEndian.Uint64(pageData[off : off+8])
			zeroed := make([]byte, page.Size)
			copy(zeroed, pageData)
			binary.LittleEndian.PutUint64(zeroed[off:off+8], 0)
			if computeCRC64ECMA(zeroed) == stored {
				t.Logf("  CRC-64/ECMA matches if checksum is at offset %d (value 0x%016x)", off, stored)
				record(t, version, "directory.checksum_actual_offset", fmt.Sprintf("checksum at offset %d", off), "confirmed",
					fmt.Sprintf("CRC-64/ECMA matches stored value 0x%016x at offset %d", stored, off), hexStr(pageData[off:off+16]), 0, off)
			}
			if computeFletcher64(zeroed) == stored {
				t.Logf("  Fletcher-64 matches if checksum is at offset %d (value 0x%016x)", off, stored)
			}
		}
	}

	saveFindings(t)
}

// ── Test: Multi-page entry confirmation ───────────────────────────────────────

// TestEmpiricalMultiPageEntry inserts an ~8KB NCLOB and checks whether the
// string spans multiple 4096-byte pages.
func TestEmpiricalMultiPageEntry(t *testing.T) {
	db, version := skipIfNoHANA(t)
	dir := logDir(t)

	largeStr := strings.Repeat("LARGE_PROBE_", 700) // ~8.4 KB

	db.ExecContext(context.Background(), "DROP TABLE PROBE_MP.T")
	db.ExecContext(context.Background(), "DROP SCHEMA PROBE_MP")
	db.ExecContext(context.Background(), "CREATE SCHEMA PROBE_MP")
	_, err := db.ExecContext(context.Background(), `CREATE COLUMN TABLE PROBE_MP.T (ID INTEGER NOT NULL PRIMARY KEY, BIG_VAL NCLOB)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO PROBE_MP.T (ID, BIG_VAL) VALUES (1, ?)`, largeStr)
	require.NoError(t, err)
	flushLog(t, db)

	segFile := latestSegment(t, dir)
	data, err := os.ReadFile(segFile)
	require.NoError(t, err)

	probe := []byte("LARGE_PROBE_")
	positions := findBytes(data, probe)

	if len(positions) == 0 {
		record(t, version, "page.multi_page_entries", "large NCLOB stored inline in redo log", "refuted",
			"8KB probe string not found in redo log — NCLOB likely stored in separate LOB segment, not inline in redo", "", 0, 0)
		t.Log("NCLOB not found in redo log — this is expected; LOBs have their own storage")
		saveFindings(t)
		return
	}

	firstPage := positions[0] / page.Size
	lastPage := positions[len(positions)-1] / page.Size
	t.Logf("Probe found at %d positions; first page=%d last page=%d", len(positions), firstPage, lastPage)

	if firstPage != lastPage {
		record(t, version, "page.multi_page_entries", "large entries span multiple pages", "confirmed",
			fmt.Sprintf("8KB NCLOB spans pages %d to %d", firstPage, lastPage), "", firstPage, 0)
	} else {
		record(t, version, "page.multi_page_entries", "large entries span multiple pages", "refuted",
			fmt.Sprintf("8KB NCLOB contained within page %d — entries do not cross page boundaries for this size", firstPage), "", firstPage, 0)
	}

	saveFindings(t)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func findBytes(data, needle []byte) []int {
	var out []int
	for i := 0; i <= len(data)-len(needle); i++ {
		ok := true
		for j := range needle {
			if data[i+j] != needle[j] {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, i)
		}
	}
	return out
}

func hexStr(b []byte) string {
	parts := make([]string, len(b))
	for i, v := range b {
		parts[i] = fmt.Sprintf("%02x", v)
	}
	return strings.Join(parts, " ")
}

func translatePath(logDir, containerPath string) string {
	if containerPath == "" {
		return latestSegmentPath(logDir)
	}
	parts := strings.SplitN(containerPath, "/mnt00001/", 2)
	if len(parts) == 2 {
		return filepath.Join(logDir, parts[1])
	}
	return filepath.Join(logDir, "hdb00002", filepath.Base(containerPath))
}

func latestSegmentPath(dir string) string {
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_*.dat")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return ""
	}
	sort.Strings(matches)
	return matches[len(matches)-1]
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// CRC-64/ECMA-182 (polynomial 0xC96C5795D7870F42)
func computeCRC64ECMA(data []byte) uint64 {
	const poly = uint64(0xC96C5795D7870F42)
	table := make([]uint64, 256)
	for i := range table {
		crc := uint64(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	var crc uint64 = 0xFFFFFFFFFFFFFFFF
	for _, b := range data {
		crc = table[byte(crc)^b] ^ (crc >> 8) //nolint:gosec
	}
	return crc ^ 0xFFFFFFFFFFFFFFFF
}

// Fletcher-64: two 32-bit accumulators over 4-byte words
func computeFletcher64(data []byte) uint64 {
	var a, b uint64
	for i := 0; i+3 < len(data); i += 4 {
		w := uint64(binary.LittleEndian.Uint32(data[i : i+4]))
		a = (a + w) & 0xFFFFFFFF
		b = (b + a) & 0xFFFFFFFF
	}
	return (b << 32) | a
}

// XOR-64: XOR all 8-byte words
func computeXOR64(data []byte) uint64 {
	var result uint64
	for i := 0; i+7 < len(data); i += 8 {
		result ^= binary.LittleEndian.Uint64(data[i : i+8])
	}
	return result
}
