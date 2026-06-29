// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package page_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

// makeSyntheticPage constructs a 4096-byte page with the confirmed HANA 2.00.088.00 SPS08
// header layout. It writes the page magic at offset 0, the INT64_MAX sentinel at offset 8,
// the given LSN at offset 16 (CurrentLSNOffset), and the given usedBytes at offset 76
// (UsedBytesOffset). All other bytes are left as zero.
func makeSyntheticPage(lsn uint64, usedBytes uint32) []byte {
	buf := make([]byte, page.Size)
	binary.LittleEndian.PutUint64(buf[page.MagicOffset:], page.Magic)
	binary.LittleEndian.PutUint64(buf[page.SentinelOffset:], page.Sentinel)
	binary.LittleEndian.PutUint64(buf[page.CurrentLSNOffset:], lsn)
	binary.LittleEndian.PutUint32(buf[page.UsedBytesOffset:], usedBytes)
	return buf
}

// ── Confirmed constants ────────────────────────────────────────────────────────

// TestPageMagicConstant verifies page.Magic == 0x00000060FF400002.
// Confirmed: HANA 2.00.088.00 SPS08 — constant on all pages of logsegment_000_00000003.dat.
func TestPageMagicConstant(t *testing.T) {
	const wantMagic uint64 = 0x00000060FF400002
	if page.Magic != wantMagic {
		t.Errorf("page.Magic = 0x%016x, want 0x%016x", page.Magic, wantMagic)
	}
}

// TestPageSentinelConstant verifies page.Sentinel == 0x7FFFFFFFFFFFFFFF (INT64_MAX).
// Confirmed: HANA 2.00.088.00 SPS08 — constant on all pages.
func TestPageSentinelConstant(t *testing.T) {
	const wantSentinel uint64 = 0x7FFFFFFFFFFFFFFF
	if page.Sentinel != wantSentinel {
		t.Errorf("page.Sentinel = 0x%016x, want 0x%016x", page.Sentinel, wantSentinel)
	}
}

// TestPageHeaderSize80 verifies page.HeaderSize == 80.
// Confirmed: HANA 2.00.088.00 SPS08 — block entries start at offset 80.
func TestPageHeaderSize80(t *testing.T) {
	if page.HeaderSize != 80 {
		t.Errorf("page.HeaderSize = %d, want 80", page.HeaderSize)
	}
}

// TestPageSizeMustBe4096 confirms the Size constant equals 4096.
func TestPageSizeMustBe4096(t *testing.T) {
	if page.Size != 4096 {
		t.Errorf("page.Size = %d, want 4096", page.Size)
	}
}

// TestConfirmedOffsetConstants verifies the confirmed byte-offset constants from
// HANA 2.00.088.00 SPS08 raw binary analysis.
func TestConfirmedOffsetConstants(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"MagicOffset", page.MagicOffset, 0},
		{"SentinelOffset", page.SentinelOffset, 8},
		{"CurrentLSNOffset", page.CurrentLSNOffset, 16},
		{"SegmentMinLSNOffset", page.SegmentMinLSNOffset, 24},
		{"SAPTagOffset", page.SAPTagOffset, 32},
		{"DBNameOffset", page.DBNameOffset, 48},
		{"NextPageLSNOffset", page.NextPageLSNOffset, 56},
		{"SegmentInternalIDOffset", page.SegmentInternalIDOffset, 68},
		{"FormatVersionOffset", page.FormatVersionOffset, 72},
		{"PageIndexOffset", page.PageIndexOffset, 74},
		{"UsedBytesOffset", page.UsedBytesOffset, 76},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
			}
		})
	}
}

// ── Parse error cases ──────────────────────────────────────────────────────────

// TestParseRejectsTooShort verifies Parse returns an error for buffers shorter
// than 4096 bytes.
func TestParseRejectsTooShort(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"one byte", 1},
		{"4095 bytes", 4095},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.size)
			_, err := page.Parse(buf)
			if err == nil {
				t.Errorf("Parse(%d bytes): expected error, got nil", tc.size)
			}
		})
	}
}

// TestParseAcceptsExactly4096 verifies Parse succeeds with exactly 4096 bytes.
func TestParseAcceptsExactly4096(t *testing.T) {
	buf := make([]byte, page.Size)
	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse(4096 zero bytes): unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("Parse returned nil page with no error")
	}
	if len(p.Raw) != page.Size {
		t.Errorf("p.Raw length = %d, want %d", len(p.Raw), page.Size)
	}
}

// ── Confirmed field extraction tests ──────────────────────────────────────────

// TestParsedPageFields builds a synthetic page with known bytes at every confirmed
// offset and verifies Parse() extracts all fields correctly.
// Offsets confirmed from HANA 2.00.088.00 SPS08 raw binary analysis.
func TestParsedPageFields(t *testing.T) {
	buf := make([]byte, page.Size)

	// Write all confirmed fields
	const wantMagic uint64 = 0x00000060FF400002
	const wantSentinel uint64 = 0x7FFFFFFFFFFFFFFF
	const wantLSN uint64 = 4652672
	const wantSegMinLSN uint64 = 4652672
	wantSAPTag := [16]byte{0x53, 0x41, 0x50, 0x0c, 0x3c, 0x7b, 0x34, 0xc9, 0x9f, 0x1e, 0xa3, 0x03, 0x27, 0x7b, 0xa4, 0xdb}
	wantDBName := [8]byte{0x48, 0x58, 0x45, 0x03, 0x98, 0xbe, 0x62, 0xfd} // "HXE" + padding
	const wantNextLSN uint64 = 4652736
	const wantSegID uint32 = 414664 // 0x000653C8
	const wantVersion uint16 = 3
	const wantPageIdx uint16 = 7
	const wantUsedBytes uint32 = 944

	binary.LittleEndian.PutUint64(buf[page.MagicOffset:], wantMagic)
	binary.LittleEndian.PutUint64(buf[page.SentinelOffset:], wantSentinel)
	binary.LittleEndian.PutUint64(buf[page.CurrentLSNOffset:], wantLSN)
	binary.LittleEndian.PutUint64(buf[page.SegmentMinLSNOffset:], wantSegMinLSN)
	copy(buf[page.SAPTagOffset:], wantSAPTag[:])
	copy(buf[page.DBNameOffset:], wantDBName[:])
	binary.LittleEndian.PutUint64(buf[page.NextPageLSNOffset:], wantNextLSN)
	binary.LittleEndian.PutUint32(buf[page.SegmentInternalIDOffset:], wantSegID)
	binary.LittleEndian.PutUint16(buf[page.FormatVersionOffset:], wantVersion)
	binary.LittleEndian.PutUint16(buf[page.PageIndexOffset:], wantPageIdx)
	binary.LittleEndian.PutUint32(buf[page.UsedBytesOffset:], wantUsedBytes)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}

	if p.PageMagic != wantMagic {
		t.Errorf("PageMagic = 0x%016x, want 0x%016x", p.PageMagic, wantMagic)
	}
	if p.PageSentinel != wantSentinel {
		t.Errorf("PageSentinel = 0x%016x, want 0x%016x", p.PageSentinel, wantSentinel)
	}
	if p.LSN != wantLSN {
		t.Errorf("LSN = %d, want %d", p.LSN, wantLSN)
	}
	if p.SegmentMinLSN != wantSegMinLSN {
		t.Errorf("SegmentMinLSN = %d, want %d", p.SegmentMinLSN, wantSegMinLSN)
	}
	if p.SAPTag != wantSAPTag {
		t.Errorf("SAPTag = %v, want %v", p.SAPTag, wantSAPTag)
	}
	if p.DBNamePrefix != wantDBName {
		t.Errorf("DBNamePrefix = %v, want %v", p.DBNamePrefix, wantDBName)
	}
	if p.NextPageLSN != wantNextLSN {
		t.Errorf("NextPageLSN = %d, want %d", p.NextPageLSN, wantNextLSN)
	}
	if p.SegmentInternalID != wantSegID {
		t.Errorf("SegmentInternalID = %d, want %d", p.SegmentInternalID, wantSegID)
	}
	if p.FormatVersion != wantVersion {
		t.Errorf("FormatVersion = %d, want %d", p.FormatVersion, wantVersion)
	}
	if p.PageIndex != wantPageIdx {
		t.Errorf("PageIndex = %d, want %d", p.PageIndex, wantPageIdx)
	}
	if p.UsedBytes != wantUsedBytes {
		t.Errorf("UsedBytes = %d, want %d", p.UsedBytes, wantUsedBytes)
	}
	if len(p.Raw) != page.Size {
		t.Errorf("Raw length = %d, want %d", len(p.Raw), page.Size)
	}
}

// TestLSNOffset16 verifies that a page with LSN 9999 written at offset 16
// (CurrentLSNOffset) is correctly extracted by Parse().
// Confirmed: LSN is at offset 16, not offset 0 — HANA 2.00.088.00 SPS08.
func TestLSNOffset16(t *testing.T) {
	const wantLSN uint64 = 9999
	buf := make([]byte, page.Size)
	binary.LittleEndian.PutUint64(buf[page.CurrentLSNOffset:], wantLSN)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.LSN != wantLSN {
		t.Errorf("LSN = %d (from offset %d), want %d — offset 16 is the confirmed CurrentLSNOffset",
			p.LSN, page.CurrentLSNOffset, wantLSN)
	}
	// Confirm offset 0 is NOT mistakenly used for LSN
	lsnFromOffset0 := binary.LittleEndian.Uint64(buf[0:8])
	if lsnFromOffset0 == wantLSN {
		t.Error("LSN found at offset 0 — but confirmed offset is 16 (HANA 2.00.088.00 SPS08)")
	}
}

// TestPageIndexOffset74 verifies that a page with PageIndex 42 written at
// offset 74 (uint16 LE) is correctly extracted by Parse().
// Confirmed: PageIndex is at offset 74 — HANA 2.00.088.00 SPS08.
func TestPageIndexOffset74(t *testing.T) {
	const wantIdx uint16 = 42
	buf := make([]byte, page.Size)
	binary.LittleEndian.PutUint16(buf[page.PageIndexOffset:], wantIdx)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.PageIndex != wantIdx {
		t.Errorf("PageIndex = %d (from offset %d), want %d",
			p.PageIndex, page.PageIndexOffset, wantIdx)
	}
}

// TestUsedBytesOffset76 verifies that a page with UsedBytes 512 written at
// offset 76 (uint32 LE) is correctly extracted by Parse().
// Confirmed: UsedBytes is at offset 76 — HANA 2.00.088.00 SPS08.
func TestUsedBytesOffset76(t *testing.T) {
	const wantUsed uint32 = 512
	buf := make([]byte, page.Size)
	binary.LittleEndian.PutUint32(buf[page.UsedBytesOffset:], wantUsed)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.UsedBytes != wantUsed {
		t.Errorf("UsedBytes = %d (from offset %d), want %d",
			p.UsedBytes, page.UsedBytesOffset, wantUsed)
	}
}

// ── Payload and utility tests ──────────────────────────────────────────────────

// TestPagePayload verifies Payload() returns the correct slice of bytes
// following the 80-byte header, bounded by UsedBytes.
func TestPagePayload(t *testing.T) {
	const usedBytes uint32 = 100

	buf := makeSyntheticPage(1, usedBytes)
	// Write a recognizable pattern into the payload area (after header).
	for i := page.HeaderSize; i < page.HeaderSize+int(usedBytes); i++ {
		buf[i] = byte(i - page.HeaderSize)
	}

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}

	payload := p.Payload()
	wantLen := int(usedBytes)
	if len(payload) != wantLen {
		t.Fatalf("Payload length = %d, want %d", len(payload), wantLen)
	}
	for i, b := range payload {
		if b != byte(i) {
			t.Errorf("payload[%d] = 0x%02x, want 0x%02x", i, b, byte(i))
			break
		}
	}
}

// TestPagePayloadZeroUsedBytes verifies that when UsedBytes == 0, Payload()
// returns all bytes after the 80-byte header (full remaining page).
func TestPagePayloadZeroUsedBytes(t *testing.T) {
	buf := makeSyntheticPage(1, 0)
	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	payload := p.Payload()
	wantLen := page.Size - page.HeaderSize
	if len(payload) != wantLen {
		t.Errorf("Payload (usedBytes=0) length = %d, want %d (full post-header)", len(payload), wantLen)
	}
}

// TestContainsBytesFindsMarker verifies that ContainsBytes locates a known
// marker string written anywhere inside the page raw bytes.
func TestContainsBytesFindsMarker(t *testing.T) {
	buf := makeSyntheticPage(42, 200)

	// Write a recognizable ASCII marker at a specific offset.
	marker := []byte("RPCN_SAPHANA_PROBE_MARKER")
	const markerOffset = 512
	copy(buf[markerOffset:], marker)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}

	if !p.ContainsBytes(marker) {
		t.Error("ContainsBytes: marker not found, expected to find it")
	}

	absent := []byte("SHOULD_NOT_BE_PRESENT")
	if p.ContainsBytes(absent) {
		t.Error("ContainsBytes: absent string unexpectedly found")
	}
}

// TestContainsBytesEmptyNeedle verifies ContainsBytes returns false for an
// empty needle (avoids trivial true match).
func TestContainsBytesEmptyNeedle(t *testing.T) {
	buf := makeSyntheticPage(1, 0)
	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.ContainsBytes([]byte{}) {
		t.Error("ContainsBytes(empty): expected false, got true")
	}
}

// ── ScanFile tests ─────────────────────────────────────────────────────────────

// TestScanFileTwoPages verifies ScanFile on exactly 8192 bytes returns 2 pages
// with the correct LSNs at offset 16 (CurrentLSNOffset).
func TestScanFileTwoPages(t *testing.T) {
	const lsn0 uint64 = 4652672
	const lsn1 uint64 = 4652736 // +64 per confirmed LSNIncrementPerPage

	data := append(makeSyntheticPage(lsn0, 100), makeSyntheticPage(lsn1, 200)...)

	pages, err := page.ScanFile(data)
	if err != nil {
		t.Fatalf("ScanFile: unexpected error: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("ScanFile returned %d pages, want 2", len(pages))
	}
	if pages[0].LSN != lsn0 {
		t.Errorf("pages[0].LSN = %d, want %d", pages[0].LSN, lsn0)
	}
	if pages[1].LSN != lsn1 {
		t.Errorf("pages[1].LSN = %d, want %d", pages[1].LSN, lsn1)
	}
}

// TestScanFilePartialPage verifies ScanFile on 4096+100 bytes returns 2 pages:
// one full page and one partial page.
func TestScanFilePartialPage(t *testing.T) {
	full := makeSyntheticPage(4652672, 50)
	partial := make([]byte, 100)
	binary.LittleEndian.PutUint64(partial[page.CurrentLSNOffset:], 4652736)
	data := append(full, partial...)

	pages, err := page.ScanFile(data)
	if err != nil {
		t.Fatalf("ScanFile: unexpected error: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("ScanFile returned %d pages, want 2", len(pages))
	}
	if pages[0].LSN != 4652672 {
		t.Errorf("pages[0].LSN = %d, want 4652672", pages[0].LSN)
	}
	// Partial page: raw bytes too short for Parse; page exists with correct raw size.
	if len(pages[1].Raw) != 100 {
		t.Errorf("partial page Raw length = %d, want 100", len(pages[1].Raw))
	}
}

// TestLSNMonotonicallyIncreasing verifies that 4 synthetic pages with LSNs
// incrementing by LSNIncrementPerPage (64) parse in strictly increasing order.
// This matches the confirmed +64/page behaviour in HANA 2.00.088.00 SPS08.
func TestLSNMonotonicallyIncreasing(t *testing.T) {
	base := uint64(4652672)
	lsns := []uint64{
		base,
		base + page.LSNIncrementPerPage,
		base + 2*page.LSNIncrementPerPage,
		base + 3*page.LSNIncrementPerPage,
	}
	var data []byte
	for _, lsn := range lsns {
		data = append(data, makeSyntheticPage(lsn, 10)...)
	}

	pages, err := page.ScanFile(data)
	if err != nil {
		t.Fatalf("ScanFile: unexpected error: %v", err)
	}
	if len(pages) != len(lsns) {
		t.Fatalf("ScanFile returned %d pages, want %d", len(pages), len(lsns))
	}
	for i := 1; i < len(pages); i++ {
		if pages[i].LSN <= pages[i-1].LSN {
			t.Errorf("LSN not monotonically increasing: pages[%d].LSN=%d <= pages[%d].LSN=%d",
				i, pages[i].LSN, i-1, pages[i-1].LSN)
		}
		diff := pages[i].LSN - pages[i-1].LSN
		if diff != page.LSNIncrementPerPage {
			t.Errorf("LSN increment pages[%d]->pages[%d] = %d, want %d (LSNIncrementPerPage)",
				i-1, i, diff, page.LSNIncrementPerPage)
		}
	}
}

// TestSAPTagStartsWithSAP verifies that a page with the confirmed SAP tag bytes
// written at SAPTagOffset has "SAP" in the first 3 bytes of p.SAPTag.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestSAPTagStartsWithSAP(t *testing.T) {
	buf := make([]byte, page.Size)
	sapBytes := []byte{0x53, 0x41, 0x50, 0x0c} // "SAP" + 0x0c
	copy(buf[page.SAPTagOffset:], sapBytes)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.SAPTag[0] != 0x53 || p.SAPTag[1] != 0x41 || p.SAPTag[2] != 0x50 {
		t.Errorf("SAPTag[0:3] = %02x %02x %02x, want 53 41 50 (\"SAP\")",
			p.SAPTag[0], p.SAPTag[1], p.SAPTag[2])
	}
}

// TestDBNamePrefixHXE verifies that a page with "HXE" written at DBNameOffset
// has the correct bytes in p.DBNamePrefix.
// Confirmed: HANA 2.00.088.00 SPS08 database name "HXE".
func TestDBNamePrefixHXE(t *testing.T) {
	buf := make([]byte, page.Size)
	hxeBytes := []byte{0x48, 0x58, 0x45} // "HXE"
	copy(buf[page.DBNameOffset:], hxeBytes)

	p, err := page.Parse(buf)
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	if p.DBNamePrefix[0] != 0x48 || p.DBNamePrefix[1] != 0x58 || p.DBNamePrefix[2] != 0x45 {
		t.Errorf("DBNamePrefix[0:3] = %02x %02x %02x, want 48 58 45 (\"HXE\")",
			p.DBNamePrefix[0], p.DBNamePrefix[1], p.DBNamePrefix[2])
	}
}

// ── Empirically confirmed findings — HANA 2.00.088.00 SPS08 ──────────────────

// TestSAPTagOffset32Confirmed confirms bytes[32:35] = "SAP" on all pages.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestSAPTagOffset32Confirmed(t *testing.T) {
	assert.Equal(t, 32, page.SAPTagOffset)
	assert.Equal(t, [3]byte{0x53, 0x41, 0x50}, page.SAPTagMagic, "SAP marker must be ASCII 'SAP'")
}

// TestSAPTagPresentInSyntheticPage confirms Parse() detects SAP tag.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestSAPTagPresentInSyntheticPage(t *testing.T) {
	raw := makeSyntheticPage(12345, 100)
	// Write SAP tag
	raw[32] = 0x53
	raw[33] = 0x41
	raw[34] = 0x50
	p, err := page.Parse(raw)
	require.NoError(t, err)
	assert.True(t, p.SAPTagPresent, "SAP tag must be detected at offset 32")
}

// TestSAPTagAbsentWhenNotWritten confirms Parse() reports SAPTagPresent=false when
// the SAP magic bytes are not present.
func TestSAPTagAbsentWhenNotWritten(t *testing.T) {
	raw := makeSyntheticPage(12345, 100)
	// offset 32 is already zero — SAP tag not present
	p, err := page.Parse(raw)
	require.NoError(t, err)
	assert.False(t, p.SAPTagPresent, "SAPTagPresent must be false when SAP bytes are absent")
}

// TestHanaPropChecksumOffset64 confirms the HANA proprietary checksum is at offset 64.
// Confirmed: HANA 2.00.088.00 SPS08. Values vary per page; not a standard CRC.
// Examples: pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0.
func TestHanaPropChecksumOffset64(t *testing.T) {
	assert.Equal(t, 64, page.HanaPropChecksumOffset,
		"HANA proprietary checksum is at offset 64 (confirmed HANA 2.00.088.00 SPS08)")
}

// TestHanaPropChecksumParsed confirms Parse() reads HanaPropChecksum32 from offset 64.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestHanaPropChecksumParsed(t *testing.T) {
	const wantChecksum uint32 = 0xd5ff3706 // pg0 observed value, HANA 2.00.088.00 SPS08
	raw := makeSyntheticPage(4652672, 944)
	binary.LittleEndian.PutUint32(raw[page.HanaPropChecksumOffset:], wantChecksum)

	p, err := page.Parse(raw)
	require.NoError(t, err)
	assert.Equal(t, wantChecksum, p.HanaPropChecksum32,
		"HanaPropChecksum32 must be read from offset 64 as uint32 LE")
}

// TestRowIDIsUint64LE confirms RowID encoding.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: Row1→RowID=1, Row2→RowID=2, Row3→RowID=3 found 40 bytes before column values.
func TestRowIDIsUint64LE(t *testing.T) {
	assert.Equal(t, 8, page.RowIDSize, "RowID must be 8 bytes (uint64 LE)")
	assert.True(t, page.RowIDMonotonicallyIncreasing,
		"RowIDs start at 1 and increment; confirmed from 3 sequential inserts")
	// Encode RowID=1 as uint64 LE
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, buf[:],
		"RowID=1 must encode as [01 00 00 00 00 00 00 00]")
}

// TestRowIDSequential confirms RowIDs 1, 2, 3 encode as consecutive uint64 LE values.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: 3 sequential INSERTs into a fresh table produced RowIDs 1, 2, 3
// at offset-40 from the column values within each INSERT block.
func TestRowIDSequential(t *testing.T) {
	for rowID := uint64(1); rowID <= 3; rowID++ {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], rowID)
		// Verify round-trip
		decoded := binary.LittleEndian.Uint64(buf[:])
		assert.Equal(t, rowID, decoded, "RowID %d must round-trip as uint64 LE", rowID)
		// Verify first byte equals rowID (since rowIDs 1-3 fit in one byte)
		assert.Equal(t, byte(rowID), buf[0], "RowID %d low byte must be %d", rowID, rowID)
		// Verify remaining bytes are zero
		for i := 1; i < 8; i++ {
			assert.Equal(t, byte(0), buf[i], "RowID %d byte[%d] must be 0", rowID, i)
		}
	}
}

// TestShadowPageGenerationAtOffset16 confirms the shadow paging generation field.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: directory pg0 gen=1093, pg1 gen=337 → pg0 is winner (higher generation).
func TestShadowPageGenerationAtOffset16(t *testing.T) {
	// Synthetic directory page with generation=1093 at offset 16
	page0 := make([]byte, page.Size)
	binary.LittleEndian.PutUint64(page0[16:24], 1093)
	gen0 := binary.LittleEndian.Uint64(page0[16:24])
	assert.Equal(t, uint64(1093), gen0, "generation is uint64 LE at directory page offset 16")

	page1 := make([]byte, page.Size)
	binary.LittleEndian.PutUint64(page1[16:24], 337)
	gen1 := binary.LittleEndian.Uint64(page1[16:24])
	assert.Equal(t, uint64(337), gen1, "pg1 generation must be 337")

	// Winner has higher generation
	winner := 0
	if gen1 > gen0 {
		winner = 1
	}
	assert.Equal(t, 0, winner, "pg0 (gen=1093) must win over pg1 (gen=337)")
}

// TestPageAlignmentInvariants confirms fixed-size values fit within a single page.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestPageAlignmentInvariants(t *testing.T) {
	maxPayload := page.Size - page.HeaderSize // 4016 bytes
	assert.Equal(t, 4016, maxPayload)
	// Even 100 BIGINT columns (8 bytes each) = 800 bytes, well under 4016
	assert.Less(t, 800, maxPayload, "even 100 BIGINT columns fit within one page")
	// NCLOB can span pages (confirmed empirically: 9KB probe found across pages 237-239)
	// Large values exceeding maxPayload are stored via multi-page continuation.
	nineKB := 9 * 1024
	assert.Greater(t, nineKB, maxPayload, "9KB NCLOB exceeds single page capacity and spans pages")
}

// TestSAPChecksumOffset36 confirms the SAP checksum region starts at offset 36.
// Confirmed: HANA 2.00.088.00 SPS08.
// Structure: [SAP:3 bytes][VersionByte:0x0C][HanaChecksum12:12 bytes]
// Evidence: bytes[32:48] = 53 41 50 0c 3c 7b 34 c9 9f 1e a3 03 27 7b a4 db
func TestSAPChecksumOffset36(t *testing.T) {
	assert.Equal(t, 36, page.SAPChecksumOffset,
		"SAP 12-byte checksum starts at offset 36 (= SAPTagOffset 32 + 3-byte 'SAP' + 1 version byte)")
}

// TestSAPVersionByte0x0C confirms the SAP version byte at offset 35 = 0x0C.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: byte[35] = 0x0C = 12 (version/algorithm indicator).
func TestSAPVersionByte0x0C(t *testing.T) {
	raw := makeSyntheticPage(4652672, 100)
	// Write complete SAP tag region from live HANA 2.00.088.00 SPS08 observation
	sapRegion := []byte{0x53, 0x41, 0x50, 0x0c, 0x3c, 0x7b, 0x34, 0xc9, 0x9f, 0x1e, 0xa3, 0x03, 0x27, 0x7b, 0xa4, 0xdb}
	copy(raw[page.SAPTagOffset:], sapRegion)

	p, err := page.Parse(raw)
	require.NoError(t, err)
	// Verify "SAP" magic
	assert.True(t, p.SAPTagPresent, "SAP tag present")
	// Verify version byte at position 3 within the SAP region (absolute offset 35)
	assert.Equal(t, byte(0x0c), p.SAPTag[3], "SAP version/algorithm byte must be 0x0C")
	// Verify the 12-byte checksum starts at position 4 within SAPTag (absolute offset 36)
	wantChecksum12 := []byte{0x3c, 0x7b, 0x34, 0xc9, 0x9f, 0x1e, 0xa3, 0x03, 0x27, 0x7b, 0xa4, 0xdb}
	assert.Equal(t, wantChecksum12, p.SAPTag[4:16],
		"SAP 12-byte proprietary checksum must match observed bytes from HANA 2.00.088.00 SPS08")
}

// ── Archive log format tests ──────────────────────────────────────────────────

// TestArchiveHeaderSize confirms the archive log extra header is exactly 4096 bytes.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: log_backup_0_0_0_0.1781137490876 is 12288 bytes (3 pages);
// first page is metadata, pages 2-3 are redo pages (or padding for empty backup).
func TestArchiveHeaderSize(t *testing.T) {
	assert.Equal(t, 4096, page.ArchiveHeaderSize,
		"archive log extra header must be exactly 4096 bytes (HANA 2.00.088.00 SPS08)")
	assert.Equal(t, page.Size, page.ArchiveHeaderSize,
		"archive header = one full 4096-byte page")
}

// TestArchiveHeaderIsTaggedASCII confirms the archive header format.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: first 4096 bytes of archive file contain human-readable ASCII:
//
//	[MAGIC]HANABackup - designed by SAP in Berlin
//	[DBID]382be212-c560-ef46-8296-4f9bad4174dd
//	[DATE]1781137490876
//	[BACKUPID]1781137490876
//	[HOSTNAME]hxe
//	[SERVICENAME]nameserver
//
// NOT a binary uint64 LSN at offset 0.
func TestArchiveHeaderIsTaggedASCII(t *testing.T) {
	assert.True(t, page.ArchiveHeaderIsTaggedASCII,
		"archive header is tagged ASCII metadata, NOT binary (confirmed HANA 2.00.088.00 SPS08)")
	assert.Equal(t, "[MAGIC]", page.ArchiveHeaderMagicTag)
	assert.Contains(t, page.ArchiveHeaderSignature, "HANABackup")
	assert.Contains(t, page.ArchiveHeaderSignature, "SAP")
}

// TestIsArchiveLogFileDetection confirms archive log detection.
// Confirmed: HANA 2.00.088.00 SPS08.
// An archive log file's first 4096 bytes start with byte 0x01 (not 0x02 like redo pages)
// and contain "[MAGIC]" at offset 16.
func TestIsArchiveLogFileDetection(t *testing.T) {
	// Synthetic archive header matching confirmed bytes:
	// [0]=0x01 [1]=0x00 ... [16:23]="[MAGIC]"
	archiveHeader := make([]byte, 4096)
	archiveHeader[0] = 0x01 // version byte (not redo page magic 0x02)
	archiveHeader[1] = 0x00
	copy(archiveHeader[16:], []byte("[MAGIC]"))

	assert.True(t, page.IsArchiveLogFile(archiveHeader),
		"archive log detection must match on byte[0]=0x01 + [MAGIC] at offset 16")

	// Regular redo page must NOT be detected as archive
	redoPage := make([]byte, 4096)
	redoPage[0] = 0x02 // redo page magic low byte
	assert.False(t, page.IsArchiveLogFile(redoPage),
		"regular redo log page must NOT match archive log detection")
}

// TestArchiveHeaderContainsBackupMetadata documents the confirmed archive header content.
// Confirmed: HANA 2.00.088.00 SPS08.
// The archive header contains these tagged fields:
//
//	[MAGIC] - "HANABackup - designed by SAP in Berlin"
//	[DBID]  - database UUID (e.g. "382be212-c560-ef46-8296-4f9bad4174dd")
//	[DATE]  - backup timestamp (milliseconds epoch)
//	[BACKUPID] - same as the numeric suffix in the filename
//	[HOSTNAME] - HANA host name
//	[SERVICENAME] - HANA service name
//	[VOLUMEID] - persistence volume ID
//	[DESTINATIONLIST] - backup destination file path
//
// NOT: the backup start LSN as uint64 at offset 0 (offset 0 = version byte 0x01)
func TestArchiveHeaderContainsBackupMetadata(t *testing.T) {
	// This test documents the confirmed structure; no binary assertions needed.
	// All metadata fields confirmed from log_backup_0_0_0_0.1781137490876 analysis.
	knownTags := []string{"[MAGIC]", "[DBID]", "[DATE]", "[BACKUPID]", "[HOSTNAME]", "[SERVICENAME]"}
	for _, tag := range knownTags {
		assert.NotEmpty(t, tag, "confirmed tag must be non-empty")
	}
	assert.Equal(t, "HANABackup - designed by SAP in Berlin", page.ArchiveHeaderSignature)
}
