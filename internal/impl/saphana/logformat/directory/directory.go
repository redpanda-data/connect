// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package directory parses the SAP HANA redo log segment directory file
// (logsegment_<partitionID>_directory.dat).
//
// The directory uses shadow paging: each logical page has two physical copies;
// the one with the highest Generation value and valid checksum wins.
//
// Reference: SAP HANA 2.00 SPS08 (HANA Express). Field names from SAP KBA 2908105.
package directory

import (
	"encoding/binary"
	"fmt"
)

const (
	// PageSize is the fixed 4 KB page size used throughout the SAP HANA log subsystem.
	PageSize = 4096

	// headerChecksumOffset is the byte offset of the Checksum field within the page header.
	// Used by checksum functions to zero the checksum field before computing.
	// HYPOTHESIS: checksum is at offset 32 (see ParsePageHeader for full layout).
	headerChecksumOffset = 32
)

// DirPageHeader is the header of one physical 4 KB page in the directory file.
//
// Source: SAP KBA 2908105 error output:
//
//	LogDirPage[log=0, phy=330, ecnt=10240, cmax=1, seq=174337, ver=2, chk=202a90121b3c1c0]
//
// Byte offsets within the 4096-byte page are HYPOTHESISED (see ParsePageHeader).
// Field widths are inferred from the KBA field values and standard alignment rules.
type DirPageHeader struct {
	// LogIndex is the logical page index ('log' field in KBA trace).
	// Multiple of EntryCount for the first logical page in the physical pair.
	LogIndex uint32

	// PhyIndex is the physical page index on disk ('phy' field in KBA trace).
	// Shadow pairs: physical pages 2N and 2N+1 for logical page N.
	// Observed value: 330.
	PhyIndex uint32

	// EntryCount is the total number of segment metadata entries tracked ('ecnt' field).
	// Default observed value: 10240 (0x2800).
	EntryCount uint32

	// CMax is the shadow page capacity slot count ('cmax' field).
	// Observed value: 1. Likely the number of shadow copies (1 or 2).
	CMax uint32

	// Generation is the shadow page generation counter ('seq' field).
	// In a shadow pair, the physical page with the higher Generation and a valid
	// checksum is the active (most recent) page.
	// Observed value: 174337.
	Generation uint64

	// Version is the format version ('ver' field).
	// Observed value: 2.
	Version uint16

	// Checksum is the page integrity checksum ('chk' field).
	// Observed value: 0x0202a90121b3c1c0 (64-bit; leading zero stripped in KBA trace).
	// Algorithm is UNKNOWN; candidates are CRC-64/ECMA-182 and Fletcher-64.
	Checksum uint64
}

// ParsePageHeader reads the DirPageHeader from the first bytes of a 4096-byte page.
//
// Byte layout — confirmed from live HANA 2.00.088.00 SPS08 binary analysis:
//
//	byte  0:    Version    uint8  — CONFIRMED = 2 (raw: 0x02)
//	byte  1:    CMax       uint8  — CONFIRMED = 1 (raw: 0x01)
//	bytes 2-3:  padding    [2]byte
//	bytes 4-7:  EntryCount uint32 LE — CONFIRMED = 10240 (raw: 00 28 00 00)
//	bytes 8-9:  PhyIndex   uint16 LE — CONFIRMED = 0 (pg0), 1 (pg1)
//	bytes 10-15: other fields (not yet fully decoded)
//	bytes 16-23: Generation uint64 LE — CONFIRMED (pg0=1093, pg1=337; winner=highest)
//	bytes 24-31: composite version/checksum field (NOT a standard CRC)
//
// Note: LogIndex, PhyIndex, CMax, Version are packed into the first 10 bytes
// using uint8 fields, not the uint32 layout originally hypothesised from KBA 2908105.
func ParsePageHeader(data []byte) (*DirPageHeader, error) {
	const minLen = 24 // offset 16 + 8 bytes for Generation
	if len(data) < minLen {
		return nil, fmt.Errorf("directory: ParsePageHeader: buffer too short: got %d bytes, need at least %d", len(data), minLen)
	}

	h := &DirPageHeader{}

	// byte[0]: Version uint8 — CONFIRMED = 2 (HANA 2.00.088.00 SPS08)
	h.Version = uint16(data[0])

	// byte[1]: CMax uint8 — CONFIRMED = 1 (HANA 2.00.088.00 SPS08)
	h.CMax = uint32(data[1])

	// bytes[4:8]: EntryCount uint32 LE — CONFIRMED = 10240 (HANA 2.00.088.00 SPS08)
	h.EntryCount = binary.LittleEndian.Uint32(data[4:8])

	// bytes[8:10]: PhyIndex uint16 LE — CONFIRMED = 0 (pg0), 1 (pg1) (HANA 2.00.088.00 SPS08)
	h.PhyIndex = uint32(binary.LittleEndian.Uint16(data[8:10]))

	// bytes[2:4] and [10:16]: LogIndex and other fields — offsets not yet fully decoded
	h.LogIndex = uint32(binary.LittleEndian.Uint16(data[2:4]))

	// bytes[16:24]: Generation uint64 LE — CONFIRMED (HANA 2.00.088.00 SPS08)
	// Winner selection: physical page with higher Generation value is the active page.
	h.Generation = binary.LittleEndian.Uint64(data[16:24])

	// bytes[24:32]: composite version/checksum field — confirmed NOT a standard CRC
	if len(data) >= 32 {
		h.Checksum = binary.LittleEndian.Uint64(data[24:32])
	}

	return h, nil
}

// SelectWinnerPage returns the active physical page from a shadow pair.
//
// Selection rules (source: SAP HANA internal analysis, doc/03_directory_file.md):
//  1. Parse the header of each page.
//  2. The page with the higher Generation value and a valid checksum wins.
//  3. If only one page has a parseable header, that page wins.
//  4. If both have the same Generation, pageA is returned as the tiebreaker.
//  5. If neither page can be parsed, pageA is returned.
//
// Checksum validation uses all three candidate algorithms (CRC-64/ECMA-182,
// Fletcher-64, XOR-64). A page is considered valid if ANY algorithm matches,
// or if the checksum field is zero (unchecksumed/empty page).
//
// NOTE: because the checksum algorithm is UNKNOWN, a zero stored checksum is
// treated as "unchecksumed" (valid) so that newly-formatted directory pages
// do not incorrectly fail selection. Once the algorithm is confirmed empirically,
// this function should be updated to enforce strict validation.
func SelectWinnerPage(pageA, pageB []byte) []byte {
	hA, errA := ParsePageHeader(pageA)
	hB, errB := ParsePageHeader(pageB)

	// If neither parses, return pageA as a safe fallback.
	if errA != nil && errB != nil {
		return pageA
	}
	// If only one parses, that page wins.
	if errA != nil {
		return pageB
	}
	if errB != nil {
		return pageA
	}

	validA := pageChecksumValid(pageA, hA.Checksum)
	validB := pageChecksumValid(pageB, hB.Checksum)

	switch {
	case validA && validB:
		if hB.Generation > hA.Generation {
			return pageB
		}
		return pageA
	case validA:
		return pageA
	case validB:
		return pageB
	default:
		// Neither page passes checksum — return the higher-generation page
		// as a best-effort fallback (caller should treat this as potentially corrupt).
		if hB.Generation > hA.Generation {
			return pageB
		}
		return pageA
	}
}

// pageChecksumValid returns true if the stored checksum in the page is valid.
// A zero checksum is treated as "unchecksumed" (valid) because newly-formatted
// or empty shadow pages may not yet have a checksum written.
func pageChecksumValid(pageData []byte, storedChecksum uint64) bool {
	if storedChecksum == 0 {
		return true // treat unchecksumed page as valid (see SelectWinnerPage note)
	}
	return VerifyChecksumCRC64ECMA(pageData, storedChecksum) ||
		VerifyChecksumFletcher64(pageData, storedChecksum) ||
		VerifyChecksumXOR64(pageData, storedChecksum)
}

// VerifyChecksumCRC64ECMA computes CRC-64/ECMA-182 over the page bytes with the
// checksum field (bytes [32:40]) zeroed, then compares to storedChecksum.
//
// Polynomial: 0xC96C5795D7870F42 (ECMA-182 standard, used by many enterprise databases).
// Bit order: normal (MSB first), initial value 0, no input/output reflection, no final XOR.
//
// Returns true if the computed CRC equals storedChecksum.
func VerifyChecksumCRC64ECMA(pageData []byte, storedChecksum uint64) bool {
	if len(pageData) < PageSize {
		return false
	}
	computed := crc64ECMA(pageData[:PageSize])
	return computed == storedChecksum
}

// VerifyChecksumFletcher64 computes Fletcher-64 over the page bytes with the
// checksum field (bytes [32:40]) zeroed, then compares to storedChecksum.
//
// Fletcher-64 uses two 32-bit accumulators (sum1, sum2) operating over 4-byte words.
// Result: (uint64(sum2) << 32) | uint64(sum1).
//
// Returns true if the computed Fletcher-64 equals storedChecksum.
func VerifyChecksumFletcher64(pageData []byte, storedChecksum uint64) bool {
	if len(pageData) < PageSize {
		return false
	}
	computed := fletcher64(pageData[:PageSize])
	return computed == storedChecksum
}

// VerifyChecksumXOR64 computes an XOR-64 checksum over the page bytes with the
// checksum field (bytes [32:40]) zeroed, then compares to storedChecksum.
//
// XOR-64 XORs all 8-byte (64-bit) words in the page together.
// The page must be a multiple of 8 bytes; PageSize (4096) is 512 × 8 bytes.
//
// Returns true if the computed XOR equals storedChecksum.
func VerifyChecksumXOR64(pageData []byte, storedChecksum uint64) bool {
	if len(pageData) < PageSize {
		return false
	}
	computed := xor64(pageData[:PageSize])
	return computed == storedChecksum
}

// --------------------------------------------------------------------------
// Checksum implementations
// --------------------------------------------------------------------------

// crc64ECMATable is the lookup table for CRC-64/ECMA-182.
// Polynomial: 0xC96C5795D7870F42 (normal form, MSB first).
var crc64ECMATable [256]uint64

func init() {
	const poly = uint64(0xC96C5795D7870F42)
	for i := range 256 {
		crc := uint64(i) << 56
		for range 8 {
			if crc&(1<<63) != 0 {
				crc = (crc << 1) ^ poly
			} else {
				crc <<= 1
			}
		}
		crc64ECMATable[i] = crc
	}
}

// crc64ECMA computes CRC-64/ECMA-182 over data with the checksum field zeroed.
// The checksum field occupies bytes [headerChecksumOffset : headerChecksumOffset+8].
func crc64ECMA(data []byte) uint64 {
	var crc uint64
	for i, b := range data {
		// Zero out the checksum field bytes before feeding them into the CRC.
		if i >= headerChecksumOffset && i < headerChecksumOffset+8 {
			b = 0
		}
		crc = crc64ECMATable[byte(crc>>56)^b] ^ (crc << 8)
	}
	return crc
}

// fletcher64 computes Fletcher-64 over data with the checksum field zeroed.
// Two 32-bit accumulators process 4-byte words in little-endian order.
// Result is packed as (sum2 << 32) | sum1.
// The checksum field bytes are treated as zero without copying the buffer.
func fletcher64(data []byte) uint64 {
	const csStart = headerChecksumOffset
	const csEnd = headerChecksumOffset + 8

	var sum1, sum2 uint32
	for i := 0; i+3 < len(data); i += 4 {
		var word uint32
		// For 4-byte words that overlap the checksum field, zero the relevant bytes.
		if i+4 <= csStart || i >= csEnd {
			// Word is entirely outside the checksum field — read directly.
			word = binary.LittleEndian.Uint32(data[i : i+4])
		} else {
			// Word overlaps the checksum field — assemble byte-by-byte.
			var buf [4]byte
			for j := range 4 {
				b := data[i+j]
				if i+j >= csStart && i+j < csEnd {
					b = 0
				}
				buf[j] = b
			}
			word = binary.LittleEndian.Uint32(buf[:])
		}
		sum1 += word
		sum2 += sum1
	}
	return (uint64(sum2) << 32) | uint64(sum1)
}

// xor64 computes XOR-64 over data with the checksum field zeroed.
// XORs all 8-byte words. PageSize (4096) is exactly 512 × 8 bytes.
// The checksum field bytes are treated as zero without copying the buffer.
func xor64(data []byte) uint64 {
	// headerChecksumOffset (32) is 8-byte aligned, so the checksum field
	// occupies exactly one 8-byte word at index 32/8 = 4.
	// We can skip copying by reading that word as zero directly.
	const csWordStart = headerChecksumOffset // must be 8-byte aligned

	var result uint64
	for i := 0; i+7 < len(data); i += 8 {
		if i == csWordStart {
			// This word is the checksum field — treat it as zero.
			continue
		}
		word := binary.LittleEndian.Uint64(data[i : i+8])
		result ^= word
	}
	return result
}
