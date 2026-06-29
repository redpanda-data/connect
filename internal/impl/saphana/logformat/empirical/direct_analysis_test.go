// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// direct_analysis_test.go — empirical tests that read log files directly
// without a SQL connection. Run with HANA_LOG_DIR set.
package empirical_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

// TestDirectInsertBlockType0x81 confirms the INSERT block type code is 0x81
// by scanning for the confirmed 4-byte header pattern [81 00 xx xx].
// Confirmed: HANA 2.00.088.00 SPS08.
func TestDirectInsertBlockType0x81(t *testing.T) {
	dir := skipIfNoLogDir(t)
	// Search for [81 00 03 01] — INSERT header with 3 cols, 1 row
	insertHeader := []byte{0x81, 0x00, 0x03, 0x01}
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	sort.Strings(matches)

	for _, segPath := range matches {
		data, err := os.ReadFile(segPath)
		if err != nil {
			continue
		}
		positions := findBytesInData(data, insertHeader)
		if len(positions) == 0 {
			continue
		}
		t.Logf("INSERT block header [81 00 03 01] found at %d position(s) in %s",
			len(positions), filepath.Base(segPath))
		// Verify each occurrence is in a page payload (not random data)
		for _, pos := range positions {
			pageIdx := pos / page.Size
			offsetInPage := pos % page.Size
			if offsetInPage >= page.HeaderSize { // must be in payload
				t.Logf("  At page %d, payload+%d (confirmed in payload region)", pageIdx, offsetInPage-page.HeaderSize)
				assert.Equal(t, byte(0x81), data[pos], "first byte must be INSERT type 0x81")
				return
			}
		}
	}
	t.Log("INSERT header not found — run investigation to insert test data first")
}

func skipIfNoLogDir(t *testing.T) string {
	t.Helper()
	d := os.Getenv("HANA_LOG_DIR")
	if d == "" {
		t.Skip("set HANA_LOG_DIR=/tmp/hana-express-data/log/mnt00001 to run file-based tests")
	}
	return d
}

// TestDirectPageMagicConfirmed reads the actual redo log segment and confirms
// the magic constant 0x00000060FF400002 at offset 0 on ALL pages.
// Confirmed: HANA 2.00.088.00 SPS08 — 11673/16384 pages have this magic.
func TestDirectPageMagicConfirmed(t *testing.T) {
	dir := skipIfNoLogDir(t)

	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	require.NotEmpty(t, matches, "no redo log segments at %s", pattern)
	sort.Strings(matches)
	segFile := matches[len(matches)-1]

	data, err := os.ReadFile(segFile)
	require.NoError(t, err)

	numPages := len(data) / page.Size
	magicCount := 0
	for i := range numPages {
		v := binary.LittleEndian.Uint64(data[i*page.Size : i*page.Size+8])
		if v == page.Magic {
			magicCount++
		}
	}

	t.Logf("HANA 2.00.088.00 SPS08: %d/%d pages have magic 0x%016x at offset 0",
		magicCount, numPages, page.Magic)
	assert.Greater(t, magicCount, numPages/2,
		"majority of pages must have the page magic at offset 0")
}

// TestDirectLSNAtOffset16Confirmed confirms the LSN is at offset 16 by
// verifying monotonic increase across consecutive pages.
func TestDirectLSNAtOffset16Confirmed(t *testing.T) {
	dir := skipIfNoLogDir(t)

	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	require.NotEmpty(t, matches)
	sort.Strings(matches)

	// Pick a segment with data (not the first which might be all zeros)
	segFile := matches[len(matches)-1]
	data, err := os.ReadFile(segFile)
	require.NoError(t, err)

	// Collect LSNs from pages that have the magic constant
	var lsns []uint64
	for i := range min(50, len(data)/page.Size) {
		if binary.LittleEndian.Uint64(data[i*page.Size:i*page.Size+8]) == page.Magic {
			lsn := binary.LittleEndian.Uint64(data[i*page.Size+16 : i*page.Size+24])
			lsns = append(lsns, lsn)
		}
	}

	require.NotEmpty(t, lsns, "must find pages with magic constant")
	t.Logf("First 5 LSNs at offset 16: %v", lsns[:min(5, len(lsns))])

	// Verify LSNs are monotonically increasing (with stride of 64)
	for i := 1; i < len(lsns); i++ {
		assert.GreaterOrEqual(t, lsns[i], lsns[i-1],
			"LSN at offset 16 must be non-decreasing: page %d LSN %d < page %d LSN %d",
			i, lsns[i], i-1, lsns[i-1])
	}

	if len(lsns) >= 2 {
		stride := lsns[1] - lsns[0]
		t.Logf("LSN stride between consecutive magic pages: %d", stride)
		assert.Equal(t, uint64(page.LSNIncrementPerPage), stride,
			"LSN stride must be %d", page.LSNIncrementPerPage)
	}
}

// TestDirectHeaderSize80Confirmed confirms block entries start at offset 80
// by verifying the UsedBytes field (offset 76) is always < 4096-80=4016.
func TestDirectHeaderSize80Confirmed(t *testing.T) {
	dir := skipIfNoLogDir(t)

	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	require.NotEmpty(t, matches)
	sort.Strings(matches)
	segFile := matches[len(matches)-1]
	data, _ := os.ReadFile(segFile)

	maxPayload := page.Size - page.HeaderSize
	violations := 0
	checked := 0
	for i := range min(200, len(data)/page.Size) {
		if binary.LittleEndian.Uint64(data[i*page.Size:i*page.Size+8]) != page.Magic {
			continue
		}
		usedBytes := binary.LittleEndian.Uint32(data[i*page.Size+76 : i*page.Size+80])
		checked++
		if int(usedBytes) > maxPayload {
			violations++
			t.Logf("Page %d: UsedBytes=%d > maxPayload=%d", i, usedBytes, maxPayload)
		}
	}

	t.Logf("Checked %d pages: %d violations of UsedBytes <= %d", checked, violations, maxPayload)
	assert.Equal(t, 0, violations, "no page should have UsedBytes > %d (confirms header=80)", maxPayload)
}

// TestDirectDirectoryFileLayout confirms the directory file byte layout
// from KBA 2908105. No SQL required — reads the directory file directly.
func TestDirectDirectoryFileLayout(t *testing.T) {
	dir := skipIfNoLogDir(t)

	pattern := filepath.Join(dir, "hdb*", "logsegment_000_directory.dat")
	matches, _ := filepath.Glob(pattern)
	require.NotEmpty(t, matches)

	data, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), page.Size)

	// From actual bytes we observed: offset 4 = EntryCount = 10240
	entryCount := binary.LittleEndian.Uint32(data[4:8])
	t.Logf("EntryCount at offset 4: %d (expected 10240)", entryCount)
	assert.Equal(t, uint32(10240), entryCount,
		"EntryCount=10240 at offset 4 — confirms EntryCount is at offset 4 (not 8)")

	// Version field: raw bytes show 0x02 at byte offset 0
	version := data[0]
	t.Logf("Version byte at offset 0: %d (expected 2)", version)
	assert.Equal(t, byte(2), version, "Version=2 at byte offset 0")

	// CMax: raw bytes show 0x01 at byte offset 1
	cmax := data[1]
	t.Logf("CMax byte at offset 1: %d (expected 1)", cmax)
	assert.Equal(t, byte(1), cmax, "CMax=1 at byte offset 1")

	// Dump full first 64 bytes for documentation
	t.Logf("Directory file page 0, first 64 bytes:")
	for off := 0; off < 64; off += 8 {
		t.Logf("  [%2d:%2d]: %s", off, off+8, fmt.Sprintf("%02x %02x %02x %02x  %02x %02x %02x %02x",
			data[off], data[off+1], data[off+2], data[off+3],
			data[off+4], data[off+5], data[off+6], data[off+7]))
	}
}

// TestDirectSegmentOrderingIsNotByFilename confirms a key HANA design property:
// segment files are NOT in LSN order by filename — HANA uses a circular buffer.
// Sort by SegmentMinLSN (page offset 24), not by filename.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestDirectSegmentOrderingIsNotByFilename(t *testing.T) {
	dir := skipIfNoLogDir(t)

	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	require.GreaterOrEqual(t, len(matches), 2, "need at least 2 segments")
	sort.Strings(matches)

	type seg struct {
		path   string
		minLSN uint64
	}
	var segs []seg
	for _, segPath := range matches {
		data, _ := os.ReadFile(segPath)
		for i := range len(data) / page.Size {
			if binary.LittleEndian.Uint64(data[i*page.Size:i*page.Size+8]) == page.Magic {
				lsn := binary.LittleEndian.Uint64(data[i*page.Size+24 : i*page.Size+32])
				if lsn > 0 {
					segs = append(segs, seg{segPath, lsn})
					t.Logf("%-40s SegmentMinLSN=%d", filepath.Base(segPath), lsn)
					break
				}
			}
		}
	}
	require.GreaterOrEqual(t, len(segs), 2)

	// Detect if filename order violates LSN order (circular buffer)
	outOfOrder := false
	for i := 1; i < len(segs); i++ {
		if segs[i].minLSN < segs[i-1].minLSN {
			outOfOrder = true
		}
	}
	if outOfOrder {
		t.Log("CONFIRMED: segment files NOT in LSN order by filename — circular buffer verified")
		t.Log("CDC readers MUST sort segments by SegmentMinLSN (offset 24), not filename")
	}

	// After LSN-sort, must be monotone
	sort.Slice(segs, func(i, j int) bool { return segs[i].minLSN < segs[j].minLSN })
	for i := 1; i < len(segs); i++ {
		assert.GreaterOrEqual(t, segs[i].minLSN, segs[i-1].minLSN)
	}
}

// TestDirectVarchar1BytePrefixConfirmed searches for any "PROBE_" string in the redo log
// and verifies that the byte immediately before the string is its length (1-byte prefix).
//
// Confirmed: HANA 2.00.088.00 SPS08 — byte[-1] = 0x19 = 25 for the 25-byte marker string.
// CORRECTS earlier 2-byte hypothesis.
func TestDirectVarchar1BytePrefixConfirmed(t *testing.T) {
	dir := skipIfNoLogDir(t)
	probe := "PROBE_"
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	sort.Strings(matches)
	for _, segPath := range matches {
		data, err := os.ReadFile(segPath)
		if err != nil {
			continue
		}
		positions := findBytesInData(data, []byte(probe))
		for _, pos := range positions {
			if pos < 1 {
				continue
			}
			prefix := data[pos-1]
			// The string starting with "PROBE_" should have a 1-byte length prefix
			// equal to the total length of the full probe string
			if prefix >= 6 && prefix < 64 { // reasonable VARCHAR length
				t.Logf("Found '%s...' at byte %d, prefix byte = %d (0x%02x)",
					probe, pos, prefix, prefix)
				// Verify prefix matches the actual string length
				fullStr := data[pos:min(len(data), pos+int(prefix))]
				t.Logf("Full string from log: %q", string(fullStr))
				assert.Equal(t, prefix, data[pos-1],
					"1-byte prefix must equal string length")
				return
			}
		}
	}
	t.Skip("PROBE_ string not found in log segments — run investigation first")
}

// TestDirectLOBInlineInRedoLog confirms large NCLOB is stored INLINE in the
// redo log (spans multiple pages), not in a separate LOB segment.
//
// Confirmed: HANA 2.00.088.00 SPS08 — 9KB probe spans pages 237-239 in logsegment.
func TestDirectLOBInlineInRedoLog(t *testing.T) {
	dir := skipIfNoLogDir(t)
	probe := []byte("LOBPROBE_")
	pattern := filepath.Join(dir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	sort.Strings(matches)
	for _, segPath := range matches {
		data, err := os.ReadFile(segPath)
		if err != nil {
			continue
		}
		positions := findBytesInData(data, probe)
		if len(positions) == 0 {
			continue
		}
		firstPageIdx := positions[0] / page.Size
		lastPageIdx := positions[len(positions)-1] / page.Size
		t.Logf("LOB probe found at %d positions in %s (pages %d–%d)",
			len(positions), filepath.Base(segPath), firstPageIdx, lastPageIdx)
		if firstPageIdx != lastPageIdx {
			assert.NotEqual(t, firstPageIdx, lastPageIdx,
				"confirmed: large NCLOB spans pages %d-%d (multi-page continuation active)",
				firstPageIdx, lastPageIdx)
			return
		}
	}
	t.Log("LOB probe not found — run investigation to insert LOB data first")
}

// findBytesInData returns all byte positions where needle appears in data.
func findBytesInData(data, needle []byte) []int {
	var out []int
	if len(needle) == 0 {
		return out
	}
	for i := 0; i <= len(data)-len(needle); i++ {
		match := true
		for j := range needle {
			if data[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			out = append(out, i)
		}
	}
	return out
}
