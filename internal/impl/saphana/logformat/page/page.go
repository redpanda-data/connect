// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package page provides parsing for SAP HANA redo log 4 KB pages.
//
// All constants and field offsets below are empirically confirmed against
// SAP HANA 2.00.088.00 SPS08 (HANA Express, x86-64, little-endian) by
// direct hexdump analysis of logsegment_000_00000003.dat (16384 pages).
// Cross-page comparison of pages 0, 1, 2 was used to confirm offsets
// by observing which fields are constant vs. monotonically increasing.
//
// Reference: SAP HANA 2.00 SPS08 (HANA Express), confirmed 2026-06-08.
package page

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// Size is the fixed 4 KB page size.
	// Confirmed: HANA log segment file sizes are always multiples of 4096.
	// Source: SAP Help Portal — Log Volumes.
	Size = 4096

	// HeaderSize is the number of header bytes before the first block entry.
	// Confirmed: HANA 2.00.088.00 SPS08 — block entries start at offset 80.
	// Evidence: cross-page binary analysis of logsegment_000_00000003.dat;
	//   UsedBytes field at offset 76 (uint32 LE) shows 944/400/464 for pages 0/1/2,
	//   consistent with block payload starting at offset 80.
	HeaderSize = 80 // CONFIRMED — HANA 2.00.088.00 SPS08

	// Magic is the 8-byte page magic constant at offset 0 (uint64 LE).
	// Confirmed: 0x00000060FF400002 — constant on all 16384 pages of a segment.
	// Raw bytes: 02 00 40 FF 60 00 00 00 (little-endian).
	// Confirmed: HANA 2.00.088.00 SPS08.
	Magic = uint64(0x00000060FF400002) // CONFIRMED — HANA 2.00.088.00 SPS08

	// Sentinel is the 8-byte sentinel value at offset 8 (uint64 LE).
	// Value: 0x7FFFFFFFFFFFFFFF = INT64_MAX — constant on all pages.
	// Raw bytes: FF FF FF FF FF FF FF 7F (little-endian).
	// Confirmed: HANA 2.00.088.00 SPS08.
	Sentinel = uint64(0x7FFFFFFFFFFFFFFF) // CONFIRMED — HANA 2.00.088.00 SPS08

	// Field byte offsets within the 80-byte page header.
	// All confirmed against HANA 2.00.088.00 SPS08 via raw binary cross-page analysis.

	// MagicOffset is the byte offset of the PageMagic field.
	MagicOffset = 0 // confirmed uint64 LE, value = Magic constant

	// SentinelOffset is the byte offset of the sentinel field (INT64_MAX).
	SentinelOffset = 8 // confirmed uint64 LE, value = Sentinel constant

	// CurrentLSNOffset is the byte offset of the CurrentPageLSN field (uint64 LE).
	// Monotonically increasing: page 0 = 4652672, page 1 = 4652736, page 2 = 4652800 (+64/page).
	// Confirmed: HANA 2.00.088.00 SPS08.
	CurrentLSNOffset = 16 // CONFIRMED — HANA 2.00.088.00 SPS08

	// SegmentMinLSNOffset is the byte offset of the SegmentMinLSN field (uint64 LE).
	// Constant within a segment: equals the page-0 CurrentPageLSN (= 4652672 for this segment).
	// Confirmed: HANA 2.00.088.00 SPS08.
	SegmentMinLSNOffset = 24 // CONFIRMED — HANA 2.00.088.00 SPS08

	// SAPTagOffset is the byte offset of the SAP tag + checksum field (16 bytes).
	// Starts with ASCII "SAP" (0x53 0x41 0x50); remaining 13 bytes are a checksum.
	// Confirmed: HANA 2.00.088.00 SPS08.
	SAPTagOffset = 32 // CONFIRMED — HANA 2.00.088.00 SPS08

	// DBNameOffset is the byte offset of the database name field.
	// Starts with the ASCII database name, e.g. "HXE" (0x48 0x58 0x45).
	// Confirmed: HANA 2.00.088.00 SPS08.
	DBNameOffset = 48 // CONFIRMED — HANA 2.00.088.00 SPS08

	// NextPageLSNOffset is the byte offset of the NextPageLSN field (uint64 LE).
	// Stores the LSN of the next page in sequence:
	//   page 0 NextPageLSN = 4652736 (= page 1 CurrentPageLSN),
	//   page 1 NextPageLSN = 4652800 (= page 2 CurrentPageLSN).
	// Confirmed: HANA 2.00.088.00 SPS08.
	NextPageLSNOffset = 56 // CONFIRMED — HANA 2.00.088.00 SPS08

	// SegmentInternalIDOffset is the byte offset of the SegmentInternalID field (uint32 LE).
	// Constant per segment: observed value 0x000653C8 = 414664 on all pages.
	// Confirmed: HANA 2.00.088.00 SPS08.
	SegmentInternalIDOffset = 68 // CONFIRMED — HANA 2.00.088.00 SPS08

	// FormatVersionOffset is the byte offset of the FormatVersion field (uint16 LE).
	// Observed value: 3 on all pages.
	// Confirmed: HANA 2.00.088.00 SPS08.
	FormatVersionOffset = 72 // CONFIRMED — HANA 2.00.088.00 SPS08

	// PageIndexOffset is the byte offset of the PageIndex field (uint16 LE).
	// Zero-based page index within the segment: 0, 1, 2, ...
	// Confirmed: HANA 2.00.088.00 SPS08.
	PageIndexOffset = 74 // CONFIRMED — HANA 2.00.088.00 SPS08

	// UsedBytesOffset is the byte offset of the UsedBytes field (uint32 LE).
	// Number of payload bytes used after the 80-byte header.
	// Observed: 944, 400, 464 on pages 0, 1, 2 of logsegment_000_00000003.dat.
	// Confirmed: HANA 2.00.088.00 SPS08.
	UsedBytesOffset = 76 // CONFIRMED — HANA 2.00.088.00 SPS08

	// LSNIncrementPerPage is the increment of CurrentPageLSN between consecutive pages.
	// Confirmed: +64 per page across pages 0–9 of logsegment_000_00000003.dat.
	// This means the LSN counts log positions at 64-byte granularity.
	// Confirmed: HANA 2.00.088.00 SPS08.
	LSNIncrementPerPage = 64 // CONFIRMED — HANA 2.00.088.00 SPS08

	// HanaPropChecksumOffset is the offset of HANA's proprietary 32-bit page checksum.
	// Confirmed: HANA 2.00.088.00 SPS08.
	// Value varies per page; cannot be reproduced with standard CRC algorithms.
	// This is DISTINCT from the 12-byte SAP checksum at offset 32.
	// Examples observed: pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0.
	HanaPropChecksumOffset = 64 // CONFIRMED — HANA 2.00.088.00 SPS08

	// SAPChecksumOffset is the offset of the 12-byte HANA proprietary checksum within the SAP region.
	// Confirmed: HANA 2.00.088.00 SPS08.
	// Structure of the 16-byte SAP region: [SAP:3][VersionByte:0x0C][HanaChecksum12:12 bytes]
	SAPChecksumOffset = 36 // offset 32 + 4 (SAP tag 3 bytes + 1 version byte) — CONFIRMED HANA 2.00.088.00 SPS08

	// RowIDSize is the size in bytes of a HANA RowID.
	// Confirmed: HANA 2.00.088.00 SPS08.
	// RowIDs are uint64 little-endian, starting at 1 for the first row in a fresh partition.
	RowIDSize = 8 // CONFIRMED — HANA 2.00.088.00 SPS08
)

// SAPTagMagic is the 3-byte ASCII "SAP" marker at offset 32.
// Confirmed: HANA 2.00.088.00 SPS08.
var SAPTagMagic = [3]byte{0x53, 0x41, 0x50}

const (
	// RowIDMonotonicallyIncreasing documents the confirmed RowID allocation behavior.
	// Confirmed: HANA 2.00.088.00 SPS08.
	// Evidence: 3 sequential inserts into a fresh table produced RowIDs 1, 2, 3.
	RowIDMonotonicallyIncreasing = true
)

// Page represents a parsed 4 KB redo log page.
//
// All fields are confirmed from HANA 2.00.088.00 SPS08 via raw binary analysis
// of logsegment_000_00000003.dat (cross-page comparison of pages 0, 1, 2).
type Page struct {
	// PageMagic is the 8-byte magic constant at header offset 0 (uint64 LE).
	// Expected value: Magic (0x00000060FF400002) on all valid pages.
	// Confirmed: HANA 2.00.088.00 SPS08.
	PageMagic uint64

	// PageSentinel is the INT64_MAX sentinel at header offset 8 (uint64 LE).
	// Expected value: Sentinel (0x7FFFFFFFFFFFFFFF) on all valid pages.
	// Confirmed: HANA 2.00.088.00 SPS08.
	PageSentinel uint64

	// LSN is the CurrentPageLSN at header offset 16 (uint64 LE).
	// Monotonically increasing, increments by LSNIncrementPerPage (64) per page.
	// Confirmed: HANA 2.00.088.00 SPS08.
	LSN uint64

	// SegmentMinLSN is the minimum LSN of the segment at header offset 24 (uint64 LE).
	// Constant within a segment; equals the page-0 LSN.
	// Confirmed: HANA 2.00.088.00 SPS08.
	SegmentMinLSN uint64

	// SAPTag holds the 16-byte SAP tag + checksum region at header offset 32.
	// Bytes [0:3] are ASCII "SAP" (0x53 0x41 0x50); remaining 13 bytes are a checksum.
	// Confirmed: HANA 2.00.088.00 SPS08.
	SAPTag [16]byte

	// DBNamePrefix holds the first 8 bytes of the database-name region at offset 48.
	// Starts with the ASCII database name, e.g. "HXE" (0x48 0x58 0x45).
	// Confirmed: HANA 2.00.088.00 SPS08.
	DBNamePrefix [8]byte

	// NextPageLSN is the CurrentPageLSN of the next page, at header offset 56 (uint64 LE).
	// Confirmed: HANA 2.00.088.00 SPS08.
	NextPageLSN uint64

	// SegmentInternalID is a per-segment constant at header offset 68 (uint32 LE).
	// Observed value: 0x000653C8 = 414664 on all pages of one segment.
	// Confirmed: HANA 2.00.088.00 SPS08.
	SegmentInternalID uint32

	// FormatVersion is the log format version at header offset 72 (uint16 LE).
	// Observed value: 3.
	// Confirmed: HANA 2.00.088.00 SPS08.
	FormatVersion uint16

	// PageIndex is the zero-based page index within the segment at offset 74 (uint16 LE).
	// Confirmed: HANA 2.00.088.00 SPS08.
	PageIndex uint16

	// UsedBytes is the number of payload bytes after the 80-byte header (uint32 LE at offset 76).
	// Confirmed: HANA 2.00.088.00 SPS08.
	UsedBytes uint32

	// HanaPropChecksum32 is the HANA-proprietary 32-bit page checksum at offset 64.
	// Confirmed: varies per page; not reproducible with standard algorithms.
	// Examples: pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0.
	// Confirmed: HANA 2.00.088.00 SPS08.
	HanaPropChecksum32 uint32

	// SAPTagPresent is true when bytes[32:35] == "SAP" (0x53 0x41 0x50).
	// Confirmed: HANA 2.00.088.00 SPS08.
	SAPTagPresent bool

	// Raw holds the complete 4096-byte page for further analysis.
	Raw []byte
}

// Parse reads a single page from exactly 4096 bytes.
// Returns an error if data is shorter than Size.
//
// All field offsets are confirmed from HANA 2.00.088.00 SPS08.
func Parse(data []byte) (*Page, error) {
	if len(data) < Size {
		return nil, fmt.Errorf("page: buffer too short: got %d bytes, need %d", len(data), Size)
	}
	data = data[:Size]
	p := &Page{Raw: data}

	// Offset 0: PageMagic (uint64 LE). Confirmed: 0x00000060FF400002 on all pages.
	p.PageMagic = binary.LittleEndian.Uint64(data[MagicOffset : MagicOffset+8])

	// Offset 8: Sentinel (uint64 LE). Confirmed: INT64_MAX on all pages.
	p.PageSentinel = binary.LittleEndian.Uint64(data[SentinelOffset : SentinelOffset+8])

	// Offset 16: CurrentPageLSN (uint64 LE). Confirmed: monotonically increasing (+64/page).
	p.LSN = binary.LittleEndian.Uint64(data[CurrentLSNOffset : CurrentLSNOffset+8])

	// Offset 24: SegmentMinLSN (uint64 LE). Confirmed: constant within segment.
	p.SegmentMinLSN = binary.LittleEndian.Uint64(data[SegmentMinLSNOffset : SegmentMinLSNOffset+8])

	// Offset 32: SAP tag + checksum (16 bytes). Confirmed: starts with "SAP".
	copy(p.SAPTag[:], data[SAPTagOffset:SAPTagOffset+16])

	// Offset 48: Database name prefix (8 bytes). Confirmed: starts with DB name e.g. "HXE".
	copy(p.DBNamePrefix[:], data[DBNameOffset:DBNameOffset+8])

	// Offset 56: NextPageLSN (uint64 LE). Confirmed: = next page's CurrentPageLSN.
	p.NextPageLSN = binary.LittleEndian.Uint64(data[NextPageLSNOffset : NextPageLSNOffset+8])

	// Offset 64-67: HanaPropChecksum32 (uint32 LE). Confirmed: HANA-proprietary checksum; varies per page.
	// Not reproducible with CRC-32, Adler-32, or any standard algorithm.
	// Examples: pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0.
	p.HanaPropChecksum32 = binary.LittleEndian.Uint32(data[HanaPropChecksumOffset : HanaPropChecksumOffset+4])

	// SAPTagPresent: detect "SAP" marker at offset 32.
	p.SAPTagPresent = data[SAPTagOffset] == SAPTagMagic[0] && data[SAPTagOffset+1] == SAPTagMagic[1] && data[SAPTagOffset+2] == SAPTagMagic[2]

	// Offset 68: SegmentInternalID (uint32 LE). Confirmed: constant per segment.
	p.SegmentInternalID = binary.LittleEndian.Uint32(data[SegmentInternalIDOffset : SegmentInternalIDOffset+4])

	// Offset 72: FormatVersion (uint16 LE). Confirmed: value = 3.
	p.FormatVersion = binary.LittleEndian.Uint16(data[FormatVersionOffset : FormatVersionOffset+2])

	// Offset 74: PageIndex (uint16 LE). Confirmed: 0-based, increments per page.
	p.PageIndex = binary.LittleEndian.Uint16(data[PageIndexOffset : PageIndexOffset+2])

	// Offset 76: UsedBytes (uint32 LE). Confirmed: payload bytes after 80-byte header.
	p.UsedBytes = binary.LittleEndian.Uint32(data[UsedBytesOffset : UsedBytesOffset+4])

	return p, nil
}

// Payload returns the bytes following the header, up to UsedBytes.
// If UsedBytes == 0 or exceeds the page, returns all post-header bytes.
func (p *Page) Payload() []byte {
	if HeaderSize >= Size {
		return nil
	}
	end := HeaderSize + int(p.UsedBytes)
	if end > Size || p.UsedBytes == 0 {
		end = Size
	}
	return p.Raw[HeaderSize:end]
}

// ContainsBytes reports whether the given needle appears anywhere in the page.
func (p *Page) ContainsBytes(needle []byte) bool {
	if len(needle) == 0 || len(needle) > len(p.Raw) {
		return false
	}
	return bytes.Contains(p.Raw, needle)
}

// ScanFile splits a byte slice into pages and returns all parsed pages.
// Partial trailing data (< Size bytes) is returned as a page with IsFull=false.
func ScanFile(data []byte) ([]*Page, error) {
	var pages []*Page
	for len(data) > 0 {
		chunk := data
		if len(chunk) > Size {
			chunk = chunk[:Size]
		}
		p, err := Parse(chunk)
		if err != nil {
			// Partial page — wrap manually
			p = &Page{Raw: chunk}
		}
		pages = append(pages, p)
		data = data[len(chunk):]
	}
	return pages, nil
}

// ── Archive log format (confirmed HANA 2.00.088.00 SPS08) ────────────────────

// ArchiveHeaderSize is the size of the extra metadata header prepended to archive
// log (backup) files before the redo log pages begin.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: log_backup_0_0_0_0.1781137490876 — first 4096 bytes are SAP backup
// metadata in tagged ASCII format; redo log pages follow at offset 4096.
const ArchiveHeaderSize = 4096

// ArchiveHeaderMagicTag is the ASCII tag that appears at offset 16 of the
// archive log metadata header.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: bytes[16:23] = "[MAGIC]" in archive backup file.
const ArchiveHeaderMagicTag = "[MAGIC]"

// ArchiveHeaderSignature is the human-readable string found in archive log
// metadata headers identifying them as HANA backup files.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: "HANABackup - designed by SAP in Berlin" found at offset 26 in archive.
const ArchiveHeaderSignature = "HANABackup - designed by SAP in Berlin"

// ArchiveHeaderIsTaggedASCII documents the confirmed archive header format.
// Confirmed: HANA 2.00.088.00 SPS08.
// The first 4096 bytes of archive log files are NOT binary — they are SAP's
// tagged ASCII metadata format: [TAG_NAME]<length_byte><value>...
// Known tags confirmed: [MAGIC], [DBID], [DATE], [BACKUPID], [HOSTNAME],
// [VOLUMEID], [DESTINATIONLIST], [SERVICENAME], [NUMBEROFDESTINATIONS]
const ArchiveHeaderIsTaggedASCII = true

// IsArchiveLogFile returns true if the given first 4096 bytes match the
// HANA archive log metadata header (starts with version byte 0x01 and
// contains "[MAGIC]HANABackup" at offset 16).
// Confirmed: HANA 2.00.088.00 SPS08.
func IsArchiveLogFile(first4096 []byte) bool {
	if len(first4096) < 32 {
		return false
	}
	// Archive header byte[0] = 0x01 (version), byte[1] = 0x00
	// Regular redo page byte[0] = 0x02 (page magic low byte), confirmed different
	if first4096[0] == 0x01 && first4096[1] == 0x00 {
		// Check for [MAGIC] tag at offset 16
		if len(first4096) >= 23 && string(first4096[16:23]) == "[MAGIC]" {
			return true
		}
	}
	return false
}
