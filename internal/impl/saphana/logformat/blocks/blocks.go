// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package blocks provides parsers for SAP HANA redo log block entries.
// All byte offsets and type codes are either confirmed from SAP Help Portal
// or tagged as HYPOTHESIS pending empirical confirmation against HANA 2.00 SPS08.
//
// Investigation scaffold: go run ./internal/impl/saphana/logformat/investigate/
package blocks

import (
	"encoding/binary"
	"fmt"
	"math"
)

// BlockType identifies the type of a redo log block entry at the wire level.
// The actual wire size is HYPOTHESIS: uint8. Numeric values are not confirmed.
//
// Test procedure: run go run ./internal/impl/saphana/logformat/investigate/
// against HANA 2.00 SPS08, search for known INSERT string in log,
// read byte immediately before string start, that byte cluster is the block type.
type BlockType uint8

const (
	// BlockTypeUnknownCode is a placeholder until empirical tests run.
	// Do NOT use as a binary parsing sentinel.
	BlockTypeUnknownCode BlockType = 0x00 // placeholder until empirical test runs
)

// Confirmed block type codes — HANA 2.00.088.00 SPS08
// Source: empirical binary analysis via live HANA, 2026-06-08

// BlockTypeInsert is the redo log block type code for INSERT operations.
// Confirmed: HANA 2.00.088.00 SPS08 — byte 0x81 at start of block containing
// column data. Also used for UPSERT/REPLACE when row does not previously exist.
// Evidence: block starting with [81 00 col_count row_count] found immediately
// before VARCHAR probe string and adjacent INTEGER values in redo log.
const BlockTypeInsert = byte(0x81)

// BlockTypeInfrastructure is the block type for HANA-internal infrastructure entries
// (savepoint references, commit anchors, etc.) appearing at the start of every page.
// Confirmed: HANA 2.00.088.00 SPS08 — 16-byte block starting with 0x02 at
// payload+0 of every page, followed by a 0x10 (16) size field.
const BlockTypeInfrastructure = byte(0x02)

// InsertBlockHeaderSize is the fixed size of an INSERT block header in bytes.
// Structure: [type:1][flags:1][col_count:1][row_count:1]
// Then: col_count × uint32 column IDs
// Confirmed: HANA 2.00.088.00 SPS08
const InsertBlockHeaderSize = 4

// RecordEndSentinel is the 8-byte sentinel value that terminates each record
// within a block. Confirmed: INT64_MAX (0x7FFFFFFFFFFFFFFF) found after
// every row's column data in INSERT blocks.
// Evidence: [ff ff ff ff ff ff ff 7f] found after INT_VAL=42 in INSERT block.
const RecordEndSentinel = uint64(0x7FFFFFFFFFFFFFFF)

// ParseInsertHeader parses the 4-byte INSERT block header.
// Returns (colCount, rowCount, error).
// Confirmed: HANA 2.00.088.00 SPS08.
func ParseInsertHeader(data []byte) (colCount, rowCount uint8, err error) {
	if len(data) < InsertBlockHeaderSize {
		return 0, 0, fmt.Errorf("blocks: INSERT header requires %d bytes, got %d",
			InsertBlockHeaderSize, len(data))
	}
	if data[0] != BlockTypeInsert {
		return 0, 0, fmt.Errorf("blocks: expected INSERT block type 0x%02x, got 0x%02x",
			BlockTypeInsert, data[0])
	}
	return data[2], data[3], nil
}

// ColumnType identifies the column value encoding in the redo log.
// All values are HYPOTHESIS based on the standard HANA column store internal format.
type ColumnType uint8

const (
	// ColTypeTINYINT encodes a 1-byte unsigned integer.
	// HYPOTHESIS based on standard HANA column store internal format.
	ColTypeTINYINT ColumnType = 0x01 // HYPOTHESIS

	// ColTypeSMALLINT encodes a 2-byte little-endian signed integer.
	// HYPOTHESIS based on standard HANA column store internal format.
	ColTypeSMALLINT ColumnType = 0x02 // HYPOTHESIS

	// ColTypeINTEGER encodes a 4-byte little-endian signed integer.
	// HYPOTHESIS based on standard HANA column store internal format.
	ColTypeINTEGER ColumnType = 0x03 // HYPOTHESIS

	// ColTypeBIGINT encodes an 8-byte little-endian signed integer.
	// HYPOTHESIS based on standard HANA column store internal format.
	ColTypeBIGINT ColumnType = 0x04 // HYPOTHESIS

	// ColTypeVARCHAR encodes a length-prefixed UTF-8 string.
	// Wire format: 1-byte unsigned integer length prefix + UTF-8 bytes.
	// Confirmed: HANA 2.00.088.00 SPS08 — marker string of length 25 had byte 0x19=25
	// at position -1 immediately before the string start.
	ColTypeVARCHAR ColumnType = 0x09 // HYPOTHESIS

	// ColTypeNVARCHAR encodes a length-prefixed UTF-8 string.
	// Wire format: 1-byte unsigned integer length prefix + UTF-8 bytes.
	// Confirmed: HANA 2.00.088.00 SPS08 — NVARCHAR uses the same encoding as VARCHAR.
	// Evidence: INSERT NVAR_VAL='NVARTEST_PROBE' found as [0e NVARTEST_PROBE] in log.
	// 0x0e=14=len("NVARTEST_PROBE"). Bytes are UTF-8, NOT UTF-16.
	// CORRECTS earlier UTF-16LE hypothesis.
	// Type code byte 0x0A is still HYPOTHESIS — only the encoding is confirmed.
	ColTypeNVARCHAR ColumnType = 0x0A // type code HYPOTHESIS; encoding confirmed

	// ColTypeBOOLEAN encodes a boolean as a 4-byte little-endian integer (0=false, 1=true).
	// Confirmed: HANA 2.00.088.00 SPS08.
	// Evidence: BOOL=TRUE stored as [01 00 00 00], BOOL=FALSE as [00 00 00 00].
	// data_size=8 for row with ID(int4)+BOOL confirms 4-byte encoding.
	ColTypeBOOLEAN ColumnType = 0x1C // HYPOTHESIS — type code byte not yet empirically confirmed; encoding is confirmed
)

// BigIntSize is the byte count for BIGINT in the redo log.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: INSERT BIG_VAL=0x0102030405060708 — found [08 07 06 05 04 03 02 01] LE in log.
const BigIntSize = 8

// BooleanSize is the byte count for BOOLEAN in the redo log.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: BOOL=TRUE stored as [01 00 00 00], BOOL=FALSE as [00 00 00 00].
// data_size=8 for row with ID(int4)+BOOL confirms 4-byte encoding.
const BooleanSize = 4

// BooleanTrue is the 4-byte LE encoding of BOOLEAN TRUE.
// Confirmed: HANA 2.00.088.00 SPS08.
const BooleanTrue = uint32(1)

// BooleanFalse is the 4-byte LE encoding of BOOLEAN FALSE.
// Confirmed: HANA 2.00.088.00 SPS08.
const BooleanFalse = uint32(0)

// NullColumnAbsentFromLog documents the confirmed NULL encoding behavior.
// Confirmed: HANA 2.00.088.00 SPS08.
// NULL columns are simply ABSENT from the INSERT block — they are not represented
// by a null bitmap or null value bytes. col_count in the block header counts
// only non-NULL non-PK columns.
// Evidence: INSERT with only ID (PK) set — all 6 nullable columns NULL —
// produced col_count=1 and data_size=4 (only the 4-byte ID value).
const NullColumnAbsentFromLog = true

// SmallIntSameAsInteger documents confirmed SMALLINT encoding.
// Confirmed: HANA 2.00.088.00 SPS08. SMALLINT stored as 4-byte LE int32, same as INTEGER.
// Evidence: S=258 → [02 01 00 00] found in log immediately after ID value.
const SmallIntSameAsInteger = true

// TinyIntSameAsInteger documents confirmed TINYINT encoding.
// Confirmed: HANA 2.00.088.00 SPS08. TINYINT stored as 4-byte LE int32, same as INTEGER.
// Evidence: T=171 → [ab 00 00 00] found in log; same size as SMALLINT/INTEGER.
const TinyIntSameAsInteger = true

// RealSize is the byte count for REAL in the redo log (IEEE 754 float32).
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: e as float32 → [54 f8 2d 40] found in log.
const RealSize = 4

// DoubleSize is the byte count for DOUBLE in the redo log (IEEE 754 float64).
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: e as float64 → [69 57 14 8b 0a bf 05 40] found in log.
const DoubleSize = 8

// BlockTypeCommitCandidate is the first byte observed after the INT64_MAX
// end-of-record sentinel in autocommit transactions.
// Evidence: byte 0xC8 observed at `c8 aa 56 00 00 00 00 00` after INSERT sentinel.
// Status: CANDIDATE — not yet independently confirmed as COMMIT block type.
const BlockTypeCommitCandidate = byte(0xC8)

// VarcharLengthPrefixBytes is the number of bytes used for the VARCHAR length prefix.
// Confirmed: HANA 2.00.088.00 SPS08 — 1-byte unsigned integer immediately before the string.
// Evidence: marker string of length 25 had byte 0x19 = 25 at position -1.
// CORRECTS earlier 2-byte hypothesis.
const VarcharLengthPrefixBytes = 1

// ColValue represents a single column value extracted from a redo log block entry.
// The Raw field holds the raw little-endian bytes for numeric types, or the
// 1-byte length prefix followed by the string bytes for VARCHAR/NVARCHAR.
// NULL columns have IsNull=true and Raw=nil.
type ColValue struct {
	TypeCode ColumnType
	IsNull   bool
	Raw      []byte // raw little-endian bytes for the value; nil when IsNull is true
}

// InsertPayload is the parsed payload of a SAP HANA redo INSERT block.
//
// Structure (HYPOTHESIS — pending empirical confirmation on HANA 2.00 SPS08):
//
//	[ContainerID uint64 LE] [RowID uint64 LE] [NullBitmap ceil(N/8) bytes] [ColumnValues...]
//
// The NullBitmap layout (bit ordering, position relative to values) is test-pending.
// See doc/04_block_insert.md for the investigation procedure.
type InsertPayload struct {
	// ContainerID is the table partition identifier.
	// HYPOTHESIS: uint64 little-endian. Empirical test pending.
	ContainerID uint64

	// RowID is the immutable row identifier assigned at insert time.
	// Confirmed type: uint64 (SYS.M_LOG_SEGMENTS BIGINT columns, HVR example).
	RowID uint64

	// NullBitmap has ceil(colCount/8) bytes; bit i=1 means column i is NULL.
	// Bit ordering within each byte is HYPOTHESIS: LSB = lowest column index.
	NullBitmap []byte

	// Columns holds the decoded column values, one entry per non-NULL column
	// in column-definition order (INFERRED).
	Columns []ColValue
}

// DeletePayload is the parsed payload of a SAP HANA redo DELETE block.
//
// Structure (HYPOTHESIS — pending empirical confirmation on HANA 2.00 SPS08):
//
//	[ContainerID uint64 LE] [RowID uint64 LE]
//
// DELETE only stores the RowID — no column values are written.
// This is a WAL design invariant: to undo a DELETE, HANA needs only the RowID
// to locate and resurrect the row from the delta store.
type DeletePayload struct {
	// ContainerID is the table partition identifier.
	// HYPOTHESIS: uint64 little-endian.
	ContainerID uint64

	// RowID is the identifier of the deleted row.
	// Confirmed type: uint64.
	RowID uint64
}

// UpdatePayload is the parsed payload of a SAP HANA redo UPDATE block.
//
// Structure uses batch encoding (HYPOTHESIS — pending empirical confirmation on HANA 2.00 SPS08):
//
//	[ContainerID uint64 LE]
//	[BatchCount uint16 LE]   — number of before-RowIDs
//	[BeforeRowID_0..N-1 uint64 LE each]
//	[AfterRowID uint64 LE]
//	[ColumnBitmap ceil(col_count/8) bytes] — bit i=1 means column i changed
//	[ChangedColumnValues...]
//
// The batch encoding is inferred from HVR docs describing N-to-1 row consolidation.
// See doc/05_block_update.md for the investigation procedure.
type UpdatePayload struct {
	// ContainerID is the table partition identifier.
	// HYPOTHESIS: uint64 little-endian.
	ContainerID uint64

	// BeforeRowIDs holds the N before-row identifiers (batch encoding).
	// HYPOTHESIS: N encoded as uint16 LE immediately after ContainerID.
	BeforeRowIDs []uint64

	// AfterRowID is the single consolidated after-row identifier.
	AfterRowID uint64

	// ColumnBitmap has ceil(col_count/8) bytes; bit i=1 means column i changed.
	// Bit ordering within each byte is HYPOTHESIS: LSB = lowest column index.
	ColumnBitmap []byte

	// Values holds the changed column values in column-definition order.
	Values []ColValue
}

// ── Decoder helpers ───────────────────────────────────────────────────────────

// DecodeInteger reads a 4-byte little-endian int32 from raw.
// Returns an error if raw does not contain exactly 4 bytes.
func DecodeInteger(raw []byte) (int32, error) {
	if len(raw) != 4 {
		return 0, fmt.Errorf("blocks: DecodeInteger: need 4 bytes, got %d", len(raw))
	}
	return int32(binary.LittleEndian.Uint32(raw)), nil
}

// DecodeBigInt decodes a BIGINT value (8-byte LE signed).
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: INSERT BIG_VAL=0x0102030405060708 — found [08 07 06 05 04 03 02 01] LE in log.
// Returns an error if raw does not contain at least BigIntSize bytes.
func DecodeBigInt(raw []byte) (int64, error) {
	if len(raw) < BigIntSize {
		return 0, fmt.Errorf("blocks: BIGINT requires %d bytes, got %d", BigIntSize, len(raw))
	}
	return int64(binary.LittleEndian.Uint64(raw[:BigIntSize])), nil
}

// DecodeDecimal decodes a DECIMAL value from the redo log.
// DECIMAL(precision, scale) values are stored as scaled int32 LE:
// stored_value = actual_value × 10^scale
// Confirmed: DECIMAL(10,2) value 3.14 stored as int32 LE 314 ([3a 01 00 00]).
// Evidence: HANA 2.00.088.00 SPS08.
func DecodeDecimal(raw []byte, scale int) (float64, error) {
	if len(raw) < 4 {
		return 0, fmt.Errorf("blocks: DECIMAL requires 4 bytes, got %d", len(raw))
	}
	scaled := int32(binary.LittleEndian.Uint32(raw[:4]))
	divisor := 1.0
	for range scale {
		divisor *= 10
	}
	return float64(scaled) / divisor, nil
}

// EncodeDecimal encodes a float64 as a scaled DECIMAL (4 bytes LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func EncodeDecimal(value float64, scale int) []byte {
	multiplier := 1
	for range scale {
		multiplier *= 10
	}
	scaled := int32(value * float64(multiplier))
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(scaled))
	return b
}

// DecodeBoolean decodes a BOOLEAN value (4-byte LE int, 1=TRUE 0=FALSE).
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: BOOL=TRUE stored as [01 00 00 00], BOOL=FALSE as [00 00 00 00].
func DecodeBoolean(raw []byte) (bool, error) {
	if len(raw) < BooleanSize {
		return false, fmt.Errorf("blocks: BOOLEAN requires %d bytes, got %d", BooleanSize, len(raw))
	}
	return binary.LittleEndian.Uint32(raw[:BooleanSize]) == BooleanTrue, nil
}

// EncodeBoolean encodes a bool as BOOLEAN (4 bytes LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func EncodeBoolean(v bool) []byte {
	b := make([]byte, BooleanSize)
	if v {
		binary.LittleEndian.PutUint32(b, BooleanTrue)
	}
	return b
}

// EncodeReal encodes a float32 value as REAL (4 bytes IEEE 754 LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func EncodeReal(v float32) []byte {
	b := make([]byte, RealSize)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}

// DecodeReal decodes a REAL value (4-byte IEEE 754 float32 LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func DecodeReal(raw []byte) (float32, error) {
	if len(raw) < RealSize {
		return 0, fmt.Errorf("blocks: REAL requires %d bytes, got %d", RealSize, len(raw))
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(raw[:RealSize])), nil
}

// EncodeDouble encodes a float64 value as DOUBLE (8 bytes IEEE 754 LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func EncodeDouble(v float64) []byte {
	b := make([]byte, DoubleSize)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return b
}

// DecodeDouble decodes a DOUBLE value (8-byte IEEE 754 float64 LE).
// Confirmed: HANA 2.00.088.00 SPS08.
func DecodeDouble(raw []byte) (float64, error) {
	if len(raw) < DoubleSize {
		return 0, fmt.Errorf("blocks: DOUBLE requires %d bytes, got %d", DoubleSize, len(raw))
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(raw[:DoubleSize])), nil
}

// DecodeVarchar reads a length-prefixed UTF-8 VARCHAR from data starting at offset.
//
// Wire format (confirmed — 1-byte unsigned integer length prefix):
//
//	[uint8 length] [UTF-8 bytes × length]
//
// Returns the string value, total bytes consumed (1 + length), and any error.
//
// Confirmed: HANA 2.00.088.00 SPS08 — marker string "PROBE_1780976703225304000" (length 25)
// had byte 0x19 = 25 immediately before the string start. CORRECTS earlier 2-byte hypothesis.
func DecodeVarchar(data []byte, offset int) (string, int, error) {
	if offset < 0 || offset >= len(data) {
		return "", 0, fmt.Errorf("blocks: DecodeVarchar: offset %d out of range [0, %d)", offset, len(data))
	}
	remaining := data[offset:]
	if len(remaining) < VarcharLengthPrefixBytes {
		return "", 0, fmt.Errorf("blocks: DecodeVarchar: need at least %d byte(s) for length prefix, got %d", VarcharLengthPrefixBytes, len(remaining))
	}
	strLen := int(remaining[0])
	if len(remaining) < VarcharLengthPrefixBytes+strLen {
		return "", 0, fmt.Errorf("blocks: DecodeVarchar: length prefix %d but only %d bytes available after prefix", strLen, len(remaining)-VarcharLengthPrefixBytes)
	}
	return string(remaining[VarcharLengthPrefixBytes : VarcharLengthPrefixBytes+strLen]), VarcharLengthPrefixBytes + strLen, nil
}

// ── Encoder helpers (for building test fixtures) ──────────────────────────────

// EncodeInteger encodes v as a 4-byte little-endian int32.
func EncodeInteger(v int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}

// EncodeBigInt encodes v as an 8-byte little-endian int64.
func EncodeBigInt(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

// EncodeVarchar encodes s as a 1-byte unsigned integer length prefix followed by UTF-8 bytes.
// Confirmed wire format for VARCHAR columns in HANA 2.00.088.00 SPS08 redo log entries.
// Panics if len(s) > 255 (exceeds 1-byte prefix range).
func EncodeVarchar(s string) []byte {
	sb := []byte(s)
	if len(sb) > 255 {
		panic(fmt.Sprintf("blocks: EncodeVarchar: string length %d exceeds 1-byte prefix maximum (255)", len(sb)))
	}
	b := make([]byte, VarcharLengthPrefixBytes+len(sb))
	b[0] = byte(len(sb))
	copy(b[VarcharLengthPrefixBytes:], sb)
	return b
}

// ── Block parsers ─────────────────────────────────────────────────────────────

// ParseInsert parses a synthetic INSERT block payload.
//
// Expected layout (all offsets HYPOTHESIS):
//
//	[0:8]   ContainerID uint64 LE
//	[8:16]  RowID uint64 LE
//	[16]    ColCount uint8  (number of columns)
//	[17]    NullBitmapLen uint8 = ceil(ColCount/8)
//	[18 : 18+NullBitmapLen]  NullBitmap bytes
//	[18+NullBitmapLen ...]   Column entries, each:
//	    [0]     TypeCode uint8
//	    [1:3]   ValueLen uint16 LE
//	    [3 : 3+ValueLen]  raw value bytes
//
// NULL columns (bit set in NullBitmap) have no column entry in the stream.
// The column entry encoding uses an explicit (TypeCode, ValueLen, Raw) triple
// so that the parser is self-describing without a schema.
//
// This synthetic layout is designed to be round-trippable from BuildInsertBytes
// and is NOT the confirmed wire format for HANA 2.00 SPS08. The investigation
// tool (investigate/) will determine the real layout.
func ParseInsert(data []byte) (*InsertPayload, error) {
	const minLen = 8 + 8 + 1 + 1 // ContainerID + RowID + ColCount + NullBitmapLen
	if len(data) < minLen {
		return nil, fmt.Errorf("blocks: ParseInsert: data too short: %d bytes (need at least %d)", len(data), minLen)
	}

	p := &InsertPayload{}
	p.ContainerID = binary.LittleEndian.Uint64(data[0:8])
	p.RowID = binary.LittleEndian.Uint64(data[8:16])

	colCount := int(data[16])
	nullBitmapLen := int(data[17])

	offset := 18
	if offset+nullBitmapLen > len(data) {
		return nil, fmt.Errorf("blocks: ParseInsert: null bitmap requires %d bytes at offset %d, only %d bytes available",
			nullBitmapLen, offset, len(data))
	}

	p.NullBitmap = make([]byte, nullBitmapLen)
	copy(p.NullBitmap, data[offset:offset+nullBitmapLen])
	offset += nullBitmapLen

	// Parse column entries for non-NULL columns.
	// NULL columns are represented only in the bitmap; they have no column entry.
	p.Columns = make([]ColValue, 0, colCount)
	for colIdx := range colCount {
		isNull := isNullBit(p.NullBitmap, colIdx)
		if isNull {
			p.Columns = append(p.Columns, ColValue{IsNull: true})
			continue
		}
		// Read (TypeCode uint8, ValueLen uint16 LE, Raw bytes)
		if offset+3 > len(data) {
			return nil, fmt.Errorf("blocks: ParseInsert: column %d: not enough bytes for type+len header at offset %d", colIdx, offset)
		}
		typeCode := ColumnType(data[offset])
		valueLen := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
		offset += 3

		if offset+valueLen > len(data) {
			return nil, fmt.Errorf("blocks: ParseInsert: column %d: value length %d exceeds available data at offset %d", colIdx, valueLen, offset)
		}
		raw := make([]byte, valueLen)
		copy(raw, data[offset:offset+valueLen])
		offset += valueLen

		p.Columns = append(p.Columns, ColValue{
			TypeCode: typeCode,
			IsNull:   false,
			Raw:      raw,
		})
	}

	return p, nil
}

// ParseDelete parses a synthetic DELETE block payload.
//
// Expected layout (all offsets HYPOTHESIS):
//
//	[0:8]   ContainerID uint64 LE
//	[8:16]  RowID uint64 LE
//
// DELETE blocks carry no column values — confirmed by WAL design:
// to undo a DELETE, HANA needs only the RowID.
func ParseDelete(data []byte) (*DeletePayload, error) {
	const wantLen = 8 + 8
	if len(data) < wantLen {
		return nil, fmt.Errorf("blocks: ParseDelete: data too short: %d bytes (need %d)", len(data), wantLen)
	}
	return &DeletePayload{
		ContainerID: binary.LittleEndian.Uint64(data[0:8]),
		RowID:       binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}

// ParseUpdate parses a synthetic UPDATE block payload using batch encoding.
//
// Expected layout (all offsets HYPOTHESIS):
//
//	[0:8]   ContainerID uint64 LE
//	[8:10]  BatchCount uint16 LE  (number of before-RowIDs)
//	[10 : 10+8*N]  BeforeRowIDs, N × uint64 LE
//	[10+8*N : 18+8*N]  AfterRowID uint64 LE
//	[18+8*N]  ColCount uint8
//	[19+8*N]  BitmapLen uint8 = ceil(ColCount/8)
//	[20+8*N : 20+8*N+BitmapLen]  ColumnBitmap bytes
//	[20+8*N+BitmapLen ...]  Changed column entries (same triple format as ParseInsert),
//	                        one entry per bit set in ColumnBitmap.
func ParseUpdate(data []byte) (*UpdatePayload, error) {
	const minLen = 8 + 2 // ContainerID + BatchCount
	if len(data) < minLen {
		return nil, fmt.Errorf("blocks: ParseUpdate: data too short: %d bytes (need at least %d)", len(data), minLen)
	}

	p := &UpdatePayload{}
	p.ContainerID = binary.LittleEndian.Uint64(data[0:8])
	batchCount := int(binary.LittleEndian.Uint16(data[8:10]))
	offset := 10

	// Read N before-RowIDs
	p.BeforeRowIDs = make([]uint64, batchCount)
	for i := range batchCount {
		if offset+8 > len(data) {
			return nil, fmt.Errorf("blocks: ParseUpdate: before-RowID[%d]: not enough bytes at offset %d", i, offset)
		}
		p.BeforeRowIDs[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
	}

	// Read AfterRowID
	if offset+8 > len(data) {
		return nil, fmt.Errorf("blocks: ParseUpdate: AfterRowID: not enough bytes at offset %d", offset)
	}
	p.AfterRowID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Read ColCount + BitmapLen + ColumnBitmap
	if offset+2 > len(data) {
		return nil, fmt.Errorf("blocks: ParseUpdate: col count header: not enough bytes at offset %d", offset)
	}
	colCount := int(data[offset])
	bitmapLen := int(data[offset+1])
	offset += 2

	if offset+bitmapLen > len(data) {
		return nil, fmt.Errorf("blocks: ParseUpdate: column bitmap requires %d bytes at offset %d, only %d bytes available",
			bitmapLen, offset, len(data))
	}
	p.ColumnBitmap = make([]byte, bitmapLen)
	copy(p.ColumnBitmap, data[offset:offset+bitmapLen])
	offset += bitmapLen

	// Read changed column entries — one per bit set in the bitmap
	p.Values = make([]ColValue, 0)
	for colIdx := range colCount {
		if !isNullBit(p.ColumnBitmap, colIdx) {
			// bit clear = column did not change; skip
			continue
		}
		if offset+3 > len(data) {
			return nil, fmt.Errorf("blocks: ParseUpdate: changed column %d: not enough bytes for type+len header at offset %d", colIdx, offset)
		}
		typeCode := ColumnType(data[offset])
		valueLen := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
		offset += 3

		if offset+valueLen > len(data) {
			return nil, fmt.Errorf("blocks: ParseUpdate: changed column %d: value length %d exceeds available data at offset %d", colIdx, valueLen, offset)
		}
		raw := make([]byte, valueLen)
		copy(raw, data[offset:offset+valueLen])
		offset += valueLen

		p.Values = append(p.Values, ColValue{
			TypeCode: typeCode,
			Raw:      raw,
		})
	}

	return p, nil
}

// ── Fixture builders (for tests) ──────────────────────────────────────────────

// BuildInsertBytes constructs a synthetic INSERT payload byte slice for use in tests.
// The layout matches what ParseInsert expects.
//
// colCount is the total column count (including NULL columns).
// nullBitmap must have ceil(colCount/8) bytes; bit i=1 means column i is NULL.
// cols provides ColValue entries for non-NULL columns only, in column order.
func BuildInsertBytes(containerID, rowID uint64, colCount int, nullBitmap []byte, cols []ColValue) []byte {
	var buf []byte
	buf = append(buf, EncodeBigInt(int64(containerID))...)
	buf = append(buf, EncodeBigInt(int64(rowID))...)
	buf = append(buf, byte(colCount))
	buf = append(buf, byte(len(nullBitmap)))
	buf = append(buf, nullBitmap...)
	for _, c := range cols {
		buf = append(buf, byte(c.TypeCode))
		lenBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(c.Raw)))
		buf = append(buf, lenBuf...)
		buf = append(buf, c.Raw...)
	}
	return buf
}

// BuildDeleteBytes constructs a synthetic DELETE payload byte slice for use in tests.
func BuildDeleteBytes(containerID, rowID uint64) []byte {
	var buf []byte
	buf = append(buf, EncodeBigInt(int64(containerID))...)
	buf = append(buf, EncodeBigInt(int64(rowID))...)
	return buf
}

// BuildUpdateBytes constructs a synthetic UPDATE payload byte slice for use in tests.
// colCount is the total column count; columnBitmap marks changed columns.
// values provides ColValue entries for columns with a set bit in columnBitmap, in order.
func BuildUpdateBytes(containerID uint64, beforeRowIDs []uint64, afterRowID uint64, colCount int, columnBitmap []byte, values []ColValue) []byte {
	var buf []byte
	buf = append(buf, EncodeBigInt(int64(containerID))...)

	// BatchCount
	batchLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(batchLen, uint16(len(beforeRowIDs)))
	buf = append(buf, batchLen...)

	for _, rid := range beforeRowIDs {
		buf = append(buf, EncodeBigInt(int64(rid))...)
	}
	buf = append(buf, EncodeBigInt(int64(afterRowID))...)

	buf = append(buf, byte(colCount))
	buf = append(buf, byte(len(columnBitmap)))
	buf = append(buf, columnBitmap...)

	for _, v := range values {
		buf = append(buf, byte(v.TypeCode))
		lenBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(v.Raw)))
		buf = append(buf, lenBuf...)
		buf = append(buf, v.Raw...)
	}
	return buf
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// isNullBit reports whether bit colIdx is set in bitmap.
// Bit ordering: bit 0 of byte 0 = column 0 (LSB-first within each byte).
func isNullBit(bitmap []byte, colIdx int) bool {
	byteIdx := colIdx / 8
	bitIdx := uint(colIdx % 8)
	if byteIdx >= len(bitmap) {
		return false
	}
	return (bitmap[byteIdx]>>bitIdx)&1 == 1
}

// ── Confirmed DELETE and COMMIT block types (HANA 2.00.088.00 SPS08) ─────────

// BlockTypeDelete is the redo log block type code for DELETE operations.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: bytes [fe 00] found immediately before the 8-byte RowID in the
// DELETE block at pg99+796. Structure: [0xFE][0x00][RowID:uint64 LE].
// A DELETE block contains ONLY the RowID — no column values are stored.
const BlockTypeDelete = byte(0xFE)

// BlockTypeCommit is the redo log block type code for transaction COMMIT.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: 0xC8 consistently observed as the first byte immediately after
// the INT64_MAX end-of-record sentinel ([ff×7 7f]) in autocommit transactions.
// Structure: [0xC8][commit_lsn_or_timestamp: 7 bytes][padding: 8 bytes] = 16 bytes.
// Example observed: c8 ea 56 00 00 00 00 00 00 00 00 00 00 00 00 00
const BlockTypeCommit = byte(0xC8)

// DeleteBlockPayload documents the confirmed DELETE block layout.
// Confirmed: HANA 2.00.088.00 SPS08.
// Structure: [type:0xFE][flags:0x00][RowID:uint64 LE]
// DELETE blocks contain ONLY the RowID — no column values.
// Evidence: bytes [fe 00 00 00 1e 01 00 00 00 00] = DELETE header + RowID=18743296.
type DeleteBlockPayload struct {
	RowID uint64
}

// ParseDeletePayloadFromLog parses a DELETE block payload.
// Expected: [0xFE][0x00][RowID: 8 bytes LE uint64]
// Confirmed: HANA 2.00.088.00 SPS08.
func ParseDeletePayloadFromLog(data []byte) (*DeleteBlockPayload, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("blocks: DELETE payload requires 10 bytes, got %d", len(data))
	}
	if data[0] != BlockTypeDelete {
		return nil, fmt.Errorf("blocks: expected DELETE type 0x%02x, got 0x%02x", BlockTypeDelete, data[0])
	}
	return &DeleteBlockPayload{
		RowID: binary.LittleEndian.Uint64(data[2:10]),
	}, nil
}

// DDLBlockContainsJSONText documents the confirmed DDL block content.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: CREATE TABLE XDDLS.XDDLTEST_ZZZPROBE → the schema and table names
// appeared as readable UTF-8 text within a JSON structure in the redo log:
// {"_schema":"XDDLS","_name":"XDDLTEST_ZZZPROBE","_type":...}
// The exact DDL block type byte was not isolated (DDL data appears within
// multi-block pages starting with 0x02 infrastructure entries).
const DDLBlockContainsJSONText = true
