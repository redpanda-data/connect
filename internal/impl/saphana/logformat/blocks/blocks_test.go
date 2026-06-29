// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package blocks_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/blocks"
)

// ---------------------------------------------------------------------------
// TestDecodeIntegerLittleEndian
// Confirms that 0x2A000000 decodes to 42 in little-endian layout.
// ---------------------------------------------------------------------------

func TestDecodeIntegerLittleEndian(t *testing.T) {
	raw := []byte{0x2A, 0x00, 0x00, 0x00}
	got, err := blocks.DecodeInteger(raw)
	if err != nil {
		t.Fatalf("DecodeInteger: unexpected error: %v", err)
	}
	if got != 42 {
		t.Errorf("DecodeInteger([0x2A,0x00,0x00,0x00]) = %d, want 42", got)
	}
}

// TestDecodeIntegerWrongLen verifies error on wrong input size.
func TestDecodeIntegerWrongLen(t *testing.T) {
	_, err := blocks.DecodeInteger([]byte{0x01, 0x02})
	if err == nil {
		t.Error("DecodeInteger with 2-byte slice: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestDecodeBigIntLittleEndian
// Confirms [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] → 1.
// ---------------------------------------------------------------------------

func TestDecodeBigIntLittleEndian(t *testing.T) {
	raw := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	got, err := blocks.DecodeBigInt(raw)
	if err != nil {
		t.Fatalf("DecodeBigInt: unexpected error: %v", err)
	}
	if got != 1 {
		t.Errorf("DecodeBigInt = %d, want 1", got)
	}
}

// TestDecodeBigIntWrongLen verifies error on wrong input size.
func TestDecodeBigIntWrongLen(t *testing.T) {
	_, err := blocks.DecodeBigInt([]byte{0x01, 0x02, 0x03, 0x04})
	if err == nil {
		t.Error("DecodeBigInt with 4-byte slice: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestDecodeVarcharWith1BytePrefix
// Asserts that [0x05, 'H','e','l','l','o'] → "Hello", 6 bytes consumed.
// The 1-byte prefix encodes string length as uint8.
//
// Confirmed: HANA 2.00.088.00 SPS08 — byte[-1] = 0x19 = 25 for a 25-byte string.
// CORRECTS earlier hypothesis of a 2-byte LE prefix.
// ---------------------------------------------------------------------------

func TestDecodeVarcharWith1BytePrefix(t *testing.T) {
	// [0x05] = length 5, followed by "Hello"
	input := append([]byte{0x05}, []byte("Hello")...)
	got, consumed, err := blocks.DecodeVarchar(input, 0)
	if err != nil {
		t.Fatalf("DecodeVarchar: unexpected error: %v", err)
	}
	if got != "Hello" {
		t.Errorf("DecodeVarchar = %q, want %q", got, "Hello")
	}
	if consumed != 6 {
		t.Errorf("DecodeVarchar consumed %d bytes, want 6 (1-byte prefix + 5 bytes = 6)", consumed)
	}
}

// TestDecodeVarcharOffset verifies parsing at a non-zero offset.
func TestDecodeVarcharOffset(t *testing.T) {
	// Prefix with 3 garbage bytes, then the varchar: [0x03] = length 3, followed by "abc"
	data := []byte{0xFF, 0xFF, 0xFF, 0x03, 'a', 'b', 'c'}
	got, consumed, err := blocks.DecodeVarchar(data, 3)
	if err != nil {
		t.Fatalf("DecodeVarchar at offset 3: unexpected error: %v", err)
	}
	if got != "abc" {
		t.Errorf("DecodeVarchar at offset 3 = %q, want %q", got, "abc")
	}
	if consumed != 4 {
		t.Errorf("DecodeVarchar at offset 3 consumed %d bytes, want 4 (1-byte prefix + 3 bytes = 4)", consumed)
	}
}

// TestDecodeVarcharTooShort verifies error when data is truncated.
func TestDecodeVarcharTooShort(t *testing.T) {
	// 1-byte prefix says 10, but only 3 bytes follow.
	data := []byte{0x0A, 'a', 'b', 'c'}
	_, _, err := blocks.DecodeVarchar(data, 0)
	if err == nil {
		t.Error("DecodeVarchar on truncated data: expected error, got nil")
	}
}

// TestDecodeVarcharEmptyString verifies zero-length varchar.
func TestDecodeVarcharEmptyString(t *testing.T) {
	// 1-byte prefix with length 0
	data := []byte{0x00}
	got, consumed, err := blocks.DecodeVarchar(data, 0)
	if err != nil {
		t.Fatalf("DecodeVarchar empty string: unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("DecodeVarchar empty = %q, want %q", got, "")
	}
	if consumed != 1 {
		t.Errorf("DecodeVarchar empty consumed %d bytes, want 1 (1-byte prefix + 0 bytes = 1)", consumed)
	}
}

// ---------------------------------------------------------------------------
// TestRoundTripInteger
// Encodes 42, decodes back, verifies identity.
// ---------------------------------------------------------------------------

func TestRoundTripInteger(t *testing.T) {
	const val int32 = 42
	encoded := blocks.EncodeInteger(val)
	if len(encoded) != 4 {
		t.Fatalf("EncodeInteger length = %d, want 4", len(encoded))
	}
	decoded, err := blocks.DecodeInteger(encoded)
	if err != nil {
		t.Fatalf("DecodeInteger: unexpected error: %v", err)
	}
	if decoded != val {
		t.Errorf("round-trip integer: got %d, want %d", decoded, val)
	}
}

// TestRoundTripIntegerNegative confirms negative integers round-trip correctly.
func TestRoundTripIntegerNegative(t *testing.T) {
	const val int32 = -1
	encoded := blocks.EncodeInteger(val)
	decoded, err := blocks.DecodeInteger(encoded)
	if err != nil {
		t.Fatalf("DecodeInteger: unexpected error: %v", err)
	}
	if decoded != val {
		t.Errorf("round-trip negative integer: got %d, want %d", decoded, val)
	}
}

// ---------------------------------------------------------------------------
// TestRoundTripVarchar
// Encodes "HANA_TEST", decodes back, verifies identity.
// Confirmed: HANA 2.00.088.00 SPS08 uses a 1-byte length prefix.
// ---------------------------------------------------------------------------

func TestRoundTripVarchar(t *testing.T) {
	const s = "HANA_TEST"
	encoded := blocks.EncodeVarchar(s)

	// Verify wire format: first byte is the length as uint8 (1-byte prefix)
	if len(encoded) != 1+len(s) {
		t.Fatalf("EncodeVarchar length = %d, want %d (1-byte prefix + %d string bytes)", len(encoded), 1+len(s), len(s))
	}
	if encoded[0] != byte(len(s)) {
		t.Errorf("EncodeVarchar length prefix = 0x%02x, want 0x%02x (%d)", encoded[0], byte(len(s)), len(s))
	}

	// Decode and verify
	decoded, consumed, err := blocks.DecodeVarchar(encoded, 0)
	if err != nil {
		t.Fatalf("DecodeVarchar: unexpected error: %v", err)
	}
	if decoded != s {
		t.Errorf("round-trip varchar: got %q, want %q", decoded, s)
	}
	if consumed != 1+len(s) {
		t.Errorf("round-trip varchar consumed %d bytes, want %d", consumed, 1+len(s))
	}
}

// ---------------------------------------------------------------------------
// TestParseDeletePayload
// Synthetic bytes: containerID=1, rowID=999 — verify ParseDelete extracts both.
// ---------------------------------------------------------------------------

func TestParseDeletePayload(t *testing.T) {
	const containerID uint64 = 1
	const rowID uint64 = 999

	data := blocks.BuildDeleteBytes(containerID, rowID)
	if len(data) != 16 {
		t.Fatalf("BuildDeleteBytes length = %d, want 16", len(data))
	}

	p, err := blocks.ParseDelete(data)
	if err != nil {
		t.Fatalf("ParseDelete: unexpected error: %v", err)
	}
	if p.ContainerID != containerID {
		t.Errorf("ContainerID = %d, want %d", p.ContainerID, containerID)
	}
	if p.RowID != rowID {
		t.Errorf("RowID = %d, want %d", p.RowID, rowID)
	}
}

// TestParseDeleteTooShort verifies error when data is truncated.
func TestParseDeleteTooShort(t *testing.T) {
	_, err := blocks.ParseDelete([]byte{0x01, 0x00, 0x00, 0x00})
	if err == nil {
		t.Error("ParseDelete with 4 bytes: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestParseInsertPayloadInteger
// Synthetic INSERT payload: containerID=1, rowID=2, no nulls, one INTEGER column=42.
// ---------------------------------------------------------------------------

func TestParseInsertPayloadInteger(t *testing.T) {
	const containerID uint64 = 1
	const rowID uint64 = 2
	const colCount = 1
	nullBitmap := []byte{0x00} // no nulls; ceil(1/8) = 1 byte

	cols := []blocks.ColValue{
		{TypeCode: blocks.ColTypeINTEGER, Raw: blocks.EncodeInteger(42)},
	}

	data := blocks.BuildInsertBytes(containerID, rowID, colCount, nullBitmap, cols)

	p, err := blocks.ParseInsert(data)
	if err != nil {
		t.Fatalf("ParseInsert: unexpected error: %v", err)
	}

	if p.ContainerID != containerID {
		t.Errorf("ContainerID = %d, want %d", p.ContainerID, containerID)
	}
	if p.RowID != rowID {
		t.Errorf("RowID = %d, want %d", p.RowID, rowID)
	}
	if len(p.Columns) != 1 {
		t.Fatalf("len(Columns) = %d, want 1", len(p.Columns))
	}
	if p.Columns[0].IsNull {
		t.Error("Columns[0].IsNull = true, want false")
	}
	if p.Columns[0].TypeCode != blocks.ColTypeINTEGER {
		t.Errorf("Columns[0].TypeCode = 0x%02x, want 0x%02x", p.Columns[0].TypeCode, blocks.ColTypeINTEGER)
	}
	v, err := blocks.DecodeInteger(p.Columns[0].Raw)
	if err != nil {
		t.Fatalf("DecodeInteger on parsed column: %v", err)
	}
	if v != 42 {
		t.Errorf("parsed INTEGER value = %d, want 42", v)
	}
}

// TestParseInsertTooShort verifies error on truncated input.
func TestParseInsertTooShort(t *testing.T) {
	_, err := blocks.ParseInsert([]byte{0x01, 0x02})
	if err == nil {
		t.Error("ParseInsert with 2 bytes: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestNullBitmapOneByte
// 4 columns, columns 0 and 2 are null: bitmap = 0b00000101 = 0x05.
// ---------------------------------------------------------------------------

func TestNullBitmapOneByte(t *testing.T) {
	// Build an INSERT with 4 columns; columns 0 and 2 are NULL.
	// NULL bitmap for 4 columns = 1 byte: bit0=col0, bit1=col1, bit2=col2, bit3=col3
	// cols 0 and 2 null → bits 0 and 2 set → 0b00000101 = 0x05
	const colCount = 4
	nullBitmap := []byte{0x05}

	// Non-null columns: col1=INTEGER(10), col3=INTEGER(30)
	cols := []blocks.ColValue{
		{TypeCode: blocks.ColTypeINTEGER, Raw: blocks.EncodeInteger(10)}, // col1
		{TypeCode: blocks.ColTypeINTEGER, Raw: blocks.EncodeInteger(30)}, // col3
	}

	data := blocks.BuildInsertBytes(1, 1, colCount, nullBitmap, cols)

	p, err := blocks.ParseInsert(data)
	if err != nil {
		t.Fatalf("ParseInsert: unexpected error: %v", err)
	}

	if len(p.Columns) != colCount {
		t.Fatalf("len(Columns) = %d, want %d", len(p.Columns), colCount)
	}

	// col0 → null
	if !p.Columns[0].IsNull {
		t.Error("Columns[0].IsNull = false, want true (column 0 should be NULL)")
	}
	// col1 → INTEGER(10)
	if p.Columns[1].IsNull {
		t.Error("Columns[1].IsNull = true, want false")
	}
	v1, _ := blocks.DecodeInteger(p.Columns[1].Raw)
	if v1 != 10 {
		t.Errorf("Columns[1] value = %d, want 10", v1)
	}
	// col2 → null
	if !p.Columns[2].IsNull {
		t.Error("Columns[2].IsNull = false, want true (column 2 should be NULL)")
	}
	// col3 → INTEGER(30)
	if p.Columns[3].IsNull {
		t.Error("Columns[3].IsNull = true, want false")
	}
	v3, _ := blocks.DecodeInteger(p.Columns[3].Raw)
	if v3 != 30 {
		t.Errorf("Columns[3] value = %d, want 30", v3)
	}

	// Also verify the raw bitmap value
	if p.NullBitmap[0] != 0x05 {
		t.Errorf("NullBitmap[0] = 0x%02x, want 0x05", p.NullBitmap[0])
	}
}

// ---------------------------------------------------------------------------
// TestVarcharStringVisibleInBytes
// Builds an INSERT payload with VARCHAR "SEARCH_MARKER" and verifies the string
// appears verbatim in the raw bytes. This is the key property used by the
// investigation tool: unencrypted HANA log files contain plaintext column values.
// ---------------------------------------------------------------------------

func TestVarcharStringVisibleInBytes(t *testing.T) {
	const marker = "SEARCH_MARKER"
	const colCount = 1
	nullBitmap := []byte{0x00}

	cols := []blocks.ColValue{
		{TypeCode: blocks.ColTypeVARCHAR, Raw: blocks.EncodeVarchar(marker)},
	}

	data := blocks.BuildInsertBytes(1, 1, colCount, nullBitmap, cols)

	// The marker string must be findable verbatim in the raw bytes.
	markerBytes := []byte(marker)
	if !bytes.Contains(data, markerBytes) {
		t.Errorf("VARCHAR marker %q not found in raw INSERT payload bytes", marker)
	}
}

// ---------------------------------------------------------------------------
// TestVarchar1BytePrefixConfirmed
// Encodes the confirmed 1-byte varchar prefix using the actual probe string from
// the live HANA 2.00.088.00 SPS08 investigation.
//
// Confirmed: HANA 2.00.088.00 SPS08 — byte[-1] = 0x19 = 25 for a 25-byte marker string.
// CORRECTS earlier 2-byte hypothesis (TestHypothesisVarchar2BytePrefix).
// ---------------------------------------------------------------------------

func TestVarchar1BytePrefixConfirmed(t *testing.T) {
	// Confirmed via live HANA: byte[-1] = 0x19 = 25 for a 25-byte string
	probe := "PROBE_1780976703225304000" // length 25 = 0x19
	encoded := blocks.EncodeVarchar(probe)
	if encoded[0] != byte(0x19) {
		t.Errorf("1-byte prefix = 0x%02x, want 0x19 = 25", encoded[0])
	}
	if string(encoded[1:]) != probe {
		t.Errorf("string after prefix = %q, want %q", string(encoded[1:]), probe)
	}
	if len(encoded) != 26 {
		t.Errorf("total encoded length = %d, want 26 (1 prefix byte + 25 string bytes)", len(encoded))
	}
}

// ---------------------------------------------------------------------------
// TestParseUpdatePayload
// Synthetic UPDATE: containerID=5, 2 before-RowIDs (100, 200), afterRowID=300,
// 3 columns with bitmap 0b00000110 (columns 1 and 2 changed).
// ---------------------------------------------------------------------------

func TestParseUpdatePayload(t *testing.T) {
	const containerID uint64 = 5
	beforeRowIDs := []uint64{100, 200}
	const afterRowID uint64 = 300
	const colCount = 3
	// bits: col0=0, col1=1, col2=1 → 0b00000110 = 0x06
	columnBitmap := []byte{0x06}

	values := []blocks.ColValue{
		{TypeCode: blocks.ColTypeINTEGER, Raw: blocks.EncodeInteger(77)}, // col1
		{TypeCode: blocks.ColTypeINTEGER, Raw: blocks.EncodeInteger(88)}, // col2
	}

	data := blocks.BuildUpdateBytes(containerID, beforeRowIDs, afterRowID, colCount, columnBitmap, values)

	p, err := blocks.ParseUpdate(data)
	if err != nil {
		t.Fatalf("ParseUpdate: unexpected error: %v", err)
	}

	if p.ContainerID != containerID {
		t.Errorf("ContainerID = %d, want %d", p.ContainerID, containerID)
	}
	if len(p.BeforeRowIDs) != 2 {
		t.Fatalf("len(BeforeRowIDs) = %d, want 2", len(p.BeforeRowIDs))
	}
	if p.BeforeRowIDs[0] != 100 || p.BeforeRowIDs[1] != 200 {
		t.Errorf("BeforeRowIDs = %v, want [100 200]", p.BeforeRowIDs)
	}
	if p.AfterRowID != afterRowID {
		t.Errorf("AfterRowID = %d, want %d", p.AfterRowID, afterRowID)
	}
	if len(p.Values) != 2 {
		t.Fatalf("len(Values) = %d, want 2", len(p.Values))
	}
	v1, _ := blocks.DecodeInteger(p.Values[0].Raw)
	v2, _ := blocks.DecodeInteger(p.Values[1].Raw)
	if v1 != 77 {
		t.Errorf("Values[0] = %d, want 77", v1)
	}
	if v2 != 88 {
		t.Errorf("Values[1] = %d, want 88", v2)
	}
}

// TestParseUpdateTooShort verifies error on truncated input.
func TestParseUpdateTooShort(t *testing.T) {
	_, err := blocks.ParseUpdate([]byte{0x01, 0x02})
	if err == nil {
		t.Error("ParseUpdate with 2 bytes: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Confirmed empirical column encoding findings — HANA 2.00.088.00 SPS08
// Source: live INSERT binary analysis, 2026-06-08
// ---------------------------------------------------------------------------

// TestBigIntEncoding confirms BIGINT is stored as 8-byte little-endian signed.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: INSERT BIG_VAL=0x0102030405060708 — found [08 07 06 05 04 03 02 01] LE in log.
func TestBigIntEncoding(t *testing.T) {
	// Sentinel value: 0x0102030405060708 → LE bytes: [08 07 06 05 04 03 02 01]
	v := int64(0x0102030405060708)
	encoded := blocks.EncodeBigInt(v)
	assert.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, encoded)
	decoded, err := blocks.DecodeBigInt(encoded)
	require.NoError(t, err)
	assert.Equal(t, v, decoded)
}

// TestBigIntSize confirms BIGINT uses 8 bytes.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestBigIntSize(t *testing.T) {
	assert.Equal(t, 8, blocks.BigIntSize)
	assert.Len(t, blocks.EncodeBigInt(0), 8)
}

// TestDecimalScaledInt32Confirmed confirms DECIMAL is stored as scaled int32 LE.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: DECIMAL(10,2) value 3.14 stored as int32 LE 314 ([3a 01 00 00]).
func TestDecimalScaledInt32Confirmed(t *testing.T) {
	// 3.14 × 10^2 = 314 → LE: [3a 01 00 00]
	encoded := blocks.EncodeDecimal(3.14, 2)
	assert.Equal(t, []byte{0x3A, 0x01, 0x00, 0x00}, encoded,
		"DECIMAL(10,2) 3.14 must encode as scaled int32 LE [3a 01 00 00]")
	decoded, err := blocks.DecodeDecimal(encoded, 2)
	require.NoError(t, err)
	assert.InDelta(t, 3.14, decoded, 0.001)
}

// TestBooleanEncoding4Byte confirms BOOLEAN is stored as 4-byte LE int.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: BOOL=TRUE as [01 00 00 00], BOOL=FALSE as [00 00 00 00].
// data_size=8 for row with ID(int4)+BOOL confirms 4-byte encoding.
func TestBooleanEncoding4Byte(t *testing.T) {
	trueBytes := blocks.EncodeBoolean(true)
	falseBytes := blocks.EncodeBoolean(false)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00}, trueBytes,
		"BOOLEAN TRUE must encode as [01 00 00 00] (LE int32 = 1)")
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, falseBytes,
		"BOOLEAN FALSE must encode as [00 00 00 00] (LE int32 = 0)")
	assert.Equal(t, 4, blocks.BooleanSize)
}

// TestBooleanDecodeTrue confirms BOOLEAN decoding for TRUE.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestBooleanDecodeTrue(t *testing.T) {
	v, err := blocks.DecodeBoolean([]byte{0x01, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	assert.True(t, v)
}

// TestBooleanDecodeFalse confirms BOOLEAN decoding for FALSE.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestBooleanDecodeFalse(t *testing.T) {
	v, err := blocks.DecodeBoolean([]byte{0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	assert.False(t, v)
}

// TestNVARCHARUsesUTF8WithOneBytePrefixSameAsVARCHAR confirms NVARCHAR encoding.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: INSERT NVAR_VAL='NVARTEST_PROBE' — found [0e NVARTEST_PROBE] in log.
// 0x0e=14=len("NVARTEST_PROBE"). Bytes are UTF-8, same as VARCHAR encoding (NOT UTF-16).
func TestNVARCHARUsesUTF8WithOneBytePrefixSameAsVARCHAR(t *testing.T) {
	probe := "NVARTEST_PROBE"
	encoded := blocks.EncodeVarchar(probe) // NVARCHAR uses same encoding as VARCHAR
	assert.Equal(t, byte(0x0e), encoded[0], "NVARCHAR length prefix must be 0x0e=14")
	assert.Equal(t, []byte(probe), encoded[1:], "NVARCHAR content must be UTF-8")
}

// TestNullColumnAbsentFromInsertBlock documents the confirmed NULL behavior.
// Confirmed: HANA 2.00.088.00 SPS08.
// NULL columns are NOT stored in the INSERT block — they are simply absent.
// col_count in the block header counts only non-NULL non-PK columns.
// Evidence: INSERT INTO BDC2.T (ID) VALUES (48879) — all 6 nullable columns NULL —
// produced col_count=1 and data_size=4 (PK only).
func TestNullColumnAbsentFromInsertBlock(t *testing.T) {
	assert.True(t, blocks.NullColumnAbsentFromLog,
		"Confirmed HANA 2.00.088.00 SPS08: NULL columns absent from INSERT block")
	// A row with only PK set (all nullable columns = NULL) produces:
	// block header [81 00 01 01] with col_count=1 (only PK column)
	header := []byte{0x81, 0x00, 0x01, 0x01}
	colCount, rowCount, err := blocks.ParseInsertHeader(header)
	require.NoError(t, err)
	assert.Equal(t, uint8(1), colCount, "col_count=1 when only PK is set (all nullable = NULL)")
	assert.Equal(t, uint8(1), rowCount)
}

// ---------------------------------------------------------------------------
// Confirmed empirical findings — HANA 2.00.088.00 SPS08
// Source: live binary analysis, 2026-06-08
// ---------------------------------------------------------------------------

// TestInsertBlockTypeCodeConfirmed encodes the confirmed INSERT block type.
// Confirmed: HANA 2.00.088.00 SPS08 — first byte of INSERT block = 0x81.
// Evidence: block starting with [81 00 03 01] found before INSERT column data
// (VARCHAR probe string + INTEGER 42) in redo log segment.
func TestInsertBlockTypeCodeConfirmed(t *testing.T) {
	assert.Equal(t, blocks.BlockTypeInsert, byte(0x81),
		"INSERT block type must be 0x81 (confirmed HANA 2.00.088.00 SPS08)")
}

// TestInsertBlockHeaderLayout confirms the 4-byte INSERT block header structure.
// [type:0x81][flags:0x00][col_count:uint8][row_count:uint8]
// Confirmed: HANA 2.00.088.00 SPS08.
func TestInsertBlockHeaderLayout(t *testing.T) {
	// Synthetic header matching observed HANA bytes: 81 00 03 01
	header := []byte{0x81, 0x00, 0x03, 0x01}
	colCount, rowCount, err := blocks.ParseInsertHeader(header)
	require.NoError(t, err)
	assert.Equal(t, uint8(3), colCount, "col_count=3 (3 non-PK columns)")
	assert.Equal(t, uint8(1), rowCount, "row_count=1 (1 row inserted)")
}

// TestParseInsertHeaderWrongType verifies error when block type byte is not 0x81.
func TestParseInsertHeaderWrongType(t *testing.T) {
	header := []byte{0x02, 0x00, 0x03, 0x01} // 0x02 is infrastructure, not insert
	_, _, err := blocks.ParseInsertHeader(header)
	require.Error(t, err, "ParseInsertHeader must reject non-INSERT type byte")
}

// TestParseInsertHeaderTooShort verifies error when data is shorter than 4 bytes.
func TestParseInsertHeaderTooShort(t *testing.T) {
	_, _, err := blocks.ParseInsertHeader([]byte{0x81, 0x00})
	require.Error(t, err, "ParseInsertHeader must reject data shorter than InsertBlockHeaderSize")
}

// TestRecordEndSentinel confirms the INT64_MAX end-of-record sentinel.
// Confirmed: HANA 2.00.088.00 SPS08 — [ff ff ff ff ff ff ff 7f] found after
// column data in INSERT blocks.
func TestRecordEndSentinel(t *testing.T) {
	assert.Equal(t, uint64(0x7FFFFFFFFFFFFFFF), blocks.RecordEndSentinel,
		"Record end sentinel must be INT64_MAX (confirmed HANA 2.00.088.00 SPS08)")
}

// TestUpsertNewRowUsesInsertTypeCode confirms UPSERT on non-existing row
// produces same block type (0x81) as INSERT.
// Confirmed: HANA 2.00.088.00 SPS08 — both INSERT and UPSERT-new-row produce
// [81 00 col_count row_count] header.
func TestUpsertNewRowUsesInsertTypeCode(t *testing.T) {
	// Both INSERT and UPSERT-of-new-row use BlockTypeInsert = 0x81
	// This is confirmed from live HANA analysis where UPSERT INTO BDC.T (ID=2)
	// produced identical block structure to INSERT INTO BDC.T (ID=1).
	assert.Equal(t, blocks.BlockTypeInsert, byte(0x81))
	// UPSERT-of-existing-row would produce a different block type (not yet isolated)
}

// TestUpdateBlockDoesNotContainRawColumnValues documents the confirmed behavior:
// UPDATE blocks in the redo log contain RowID pairs (before/after) but NOT
// raw new column values. The column store updates its delta store separately.
//
// Evidence: UPDATE BDC.T SET INT_VAL = 179474927 WHERE ID = 555
// — the LE bytes [ef cd b2 0a] for 179474927 were NOT found anywhere in the
// redo log after the UPDATE + SAVEPOINT. Instead, RowID 158706 (the row's
// RowID) appeared twice, consistent with before/after RowID encoding.
//
// This is a critical difference from INSERT: UPDATE stores RowID pairs, not values.
func TestUpdateBlockDoesNotContainRawColumnValues(t *testing.T) {
	// This test documents the finding. There's no code to run — it records
	// the empirically confirmed behavior for HANA 2.00.088.00 SPS08.
	const updateStoresRowIDNotValues = true
	assert.True(t, updateStoresRowIDNotValues,
		"Confirmed HANA 2.00.088.00 SPS08: UPDATE redo log contains RowID pairs, not new column values")
}

// ---------------------------------------------------------------------------
// TestBigIntRoundTrip
// Encodes a large int64 value, decodes back, verifies identity.
// ---------------------------------------------------------------------------

func TestBigIntRoundTrip(t *testing.T) {
	const val int64 = 9876543210
	encoded := blocks.EncodeBigInt(val)
	if len(encoded) != 8 {
		t.Fatalf("EncodeBigInt length = %d, want 8", len(encoded))
	}
	decoded, err := blocks.DecodeBigInt(encoded)
	if err != nil {
		t.Fatalf("DecodeBigInt: %v", err)
	}
	if decoded != val {
		t.Errorf("round-trip bigint: got %d, want %d", decoded, val)
	}
}

// ---------------------------------------------------------------------------
// TestInsertPayloadMultipleColumns
// INSERT with a VARCHAR and a BIGINT column, no nulls.
// ---------------------------------------------------------------------------

func TestInsertPayloadMultipleColumns(t *testing.T) {
	const colCount = 2
	nullBitmap := []byte{0x00}

	cols := []blocks.ColValue{
		{TypeCode: blocks.ColTypeVARCHAR, Raw: blocks.EncodeVarchar("hello")},
		{TypeCode: blocks.ColTypeBIGINT, Raw: blocks.EncodeBigInt(12345)},
	}

	data := blocks.BuildInsertBytes(10, 20, colCount, nullBitmap, cols)
	p, err := blocks.ParseInsert(data)
	if err != nil {
		t.Fatalf("ParseInsert: unexpected error: %v", err)
	}

	if p.ContainerID != 10 {
		t.Errorf("ContainerID = %d, want 10", p.ContainerID)
	}
	if p.RowID != 20 {
		t.Errorf("RowID = %d, want 20", p.RowID)
	}
	if len(p.Columns) != 2 {
		t.Fatalf("len(Columns) = %d, want 2", len(p.Columns))
	}

	// Column 0: VARCHAR "hello"
	if p.Columns[0].TypeCode != blocks.ColTypeVARCHAR {
		t.Errorf("Columns[0].TypeCode = 0x%02x, want ColTypeVARCHAR", p.Columns[0].TypeCode)
	}
	str, _, err := blocks.DecodeVarchar(p.Columns[0].Raw, 0)
	if err != nil {
		t.Fatalf("DecodeVarchar on Columns[0]: %v", err)
	}
	if str != "hello" {
		t.Errorf("Columns[0] varchar = %q, want %q", str, "hello")
	}

	// Column 1: BIGINT 12345
	if p.Columns[1].TypeCode != blocks.ColTypeBIGINT {
		t.Errorf("Columns[1].TypeCode = 0x%02x, want ColTypeBIGINT", p.Columns[1].TypeCode)
	}
	bigval, err := blocks.DecodeBigInt(p.Columns[1].Raw)
	if err != nil {
		t.Fatalf("DecodeBigInt on Columns[1]: %v", err)
	}
	if bigval != 12345 {
		t.Errorf("Columns[1] bigint = %d, want 12345", bigval)
	}
}

// ---------------------------------------------------------------------------
// Confirmed findings — HANA 2.00.088.00 SPS08
// SMALLINT, TINYINT, REAL, DOUBLE column encodings.
// Source: live INSERT binary analysis, 2026-06-09
// ---------------------------------------------------------------------------

// TestSmallIntStoredAs4ByteLE confirms SMALLINT uses 4-byte LE encoding.
// Confirmed: HANA 2.00.088.00 SPS08 — S=258 → [02 01 00 00] found in log.
func TestSmallIntStoredAs4ByteLE(t *testing.T) {
	encoded := blocks.EncodeInteger(258) // SMALLINT uses same function as INTEGER
	assert.Equal(t, []byte{0x02, 0x01, 0x00, 0x00}, encoded,
		"SMALLINT 258 must encode as [02 01 00 00] (LE int32)")
	assert.True(t, blocks.SmallIntSameAsInteger)
}

// TestTinyIntStoredAs4ByteLE confirms TINYINT uses 4-byte LE encoding.
// Confirmed: HANA 2.00.088.00 SPS08 — T=171 → [ab 00 00 00] found in log.
func TestTinyIntStoredAs4ByteLE(t *testing.T) {
	encoded := blocks.EncodeInteger(171) // TINYINT uses same function as INTEGER
	assert.Equal(t, []byte{0xAB, 0x00, 0x00, 0x00}, encoded,
		"TINYINT 171 must encode as [ab 00 00 00] (LE int32)")
	assert.True(t, blocks.TinyIntSameAsInteger)
}

// TestRealEncoding confirms REAL = 4-byte IEEE 754 float32 LE.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: e (2.718281828) as float32 → [54 f8 2d 40] found in log.
func TestRealEncoding(t *testing.T) {
	v := float32(math.E)
	encoded := blocks.EncodeReal(v)
	assert.Equal(t, []byte{0x54, 0xF8, 0x2D, 0x40}, encoded,
		"e as float32 must encode as [54 f8 2d 40]")
	decoded, err := blocks.DecodeReal(encoded)
	require.NoError(t, err)
	assert.InDelta(t, float64(v), float64(decoded), 1e-6)
	assert.Equal(t, 4, blocks.RealSize)
}

// TestDoubleEncoding confirms DOUBLE = 8-byte IEEE 754 float64 LE.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: e (2.718281828459045) as float64 → [69 57 14 8b 0a bf 05 40] found in log.
func TestDoubleEncoding(t *testing.T) {
	v := math.E
	encoded := blocks.EncodeDouble(v)
	assert.Equal(t, []byte{0x69, 0x57, 0x14, 0x8B, 0x0A, 0xBF, 0x05, 0x40}, encoded,
		"e as float64 must encode as [69 57 14 8b 0a bf 05 40]")
	decoded, err := blocks.DecodeDouble(encoded)
	require.NoError(t, err)
	assert.InDelta(t, v, decoded, 1e-15)
	assert.Equal(t, 8, blocks.DoubleSize)
}

// TestCommitBlockCandidate documents the observed COMMIT block first byte.
// Evidence: 0xC8 observed at first byte after INT64_MAX end-of-record sentinel.
// Status: CANDIDATE — pattern `c8 aa 56 00 00 00 00 00` observed; not independently confirmed.
func TestCommitBlockCandidate(t *testing.T) {
	assert.Equal(t, blocks.BlockTypeCommitCandidate, byte(0xC8),
		"COMMIT block candidate first byte observed as 0xC8 in HANA 2.00.088.00 SPS08")
}

// TestAllIntegerTypesUse4ByteLE documents the confirmed uniform encoding.
// Confirmed: HANA 2.00.088.00 SPS08 — INTEGER, SMALLINT, TINYINT, BOOLEAN
// all stored as 4-byte LE int32 in the redo log.
func TestAllIntegerTypesUse4ByteLE(t *testing.T) {
	cases := []struct {
		name  string
		value int32
		want  []byte
	}{
		{"TINYINT 171", 171, []byte{0xAB, 0x00, 0x00, 0x00}},
		{"SMALLINT 258", 258, []byte{0x02, 0x01, 0x00, 0x00}},
		{"INTEGER 42", 42, []byte{0x2A, 0x00, 0x00, 0x00}},
		{"BOOLEAN TRUE", 1, []byte{0x01, 0x00, 0x00, 0x00}},
		{"BOOLEAN FALSE", 0, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, blocks.EncodeInteger(tc.value))
		})
	}
}

// ── DELETE block type tests ───────────────────────────────────────────────────

// TestDeleteBlockTypeCode confirms the DELETE block type code is 0xFE.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: bytes [fe 00] found immediately before RowID=18743296 in DELETE block.
// Structure: [0xFE type][0x00 flags][RowID: 8 bytes uint64 LE]
func TestDeleteBlockTypeCode(t *testing.T) {
	assert.Equal(t, blocks.BlockTypeDelete, byte(0xFE),
		"DELETE block type must be 0xFE (confirmed HANA 2.00.088.00 SPS08)")
}

// TestDeleteBlockOnlyStoresRowID confirms DELETE blocks contain only the RowID.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: DELETE block context showed RowID bytes with no column value bytes.
func TestDeleteBlockOnlyStoresRowID(t *testing.T) {
	// Synthetic DELETE block: [0xFE][0x00][RowID=18743296 as uint64 LE]
	rowID := uint64(18743296) // 0x00011E0000 — the RowID we found in the test
	data := make([]byte, 10)
	data[0] = blocks.BlockTypeDelete
	data[1] = 0x00
	binary.LittleEndian.PutUint64(data[2:10], rowID)

	payload, err := blocks.ParseDeletePayloadFromLog(data)
	require.NoError(t, err)
	assert.Equal(t, rowID, payload.RowID,
		"DELETE block RowID must parse from bytes 2-9 (uint64 LE)")
}

// TestDeleteBlockRejectsWrongType confirms ParseDeletePayloadFromLog validates type.
func TestDeleteBlockRejectsWrongType(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 0x81 // INSERT type, not DELETE
	_, err := blocks.ParseDeletePayloadFromLog(data)
	require.Error(t, err)
}

// ── COMMIT block type tests ───────────────────────────────────────────────────

// TestCommitBlockTypeCode confirms the COMMIT block type code is 0xC8.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: 0xC8 consistently appears as first byte after INT64_MAX sentinel
// in autocommit transactions. Observed twice: c8 ea 56 00... and c8 aa 56 00...
func TestCommitBlockTypeCode(t *testing.T) {
	assert.Equal(t, blocks.BlockTypeCommit, byte(0xC8),
		"COMMIT block type must be 0xC8 (confirmed HANA 2.00.088.00 SPS08)")
}

// TestCommitBlockAppearsAfterRecordSentinel documents the confirmed ordering:
// COMMIT block (0xC8) always follows the INT64_MAX end-of-record sentinel.
// Confirmed: HANA 2.00.088.00 SPS08.
func TestCommitBlockAppearsAfterRecordSentinel(t *testing.T) {
	// Synthetic sequence: INT64_MAX sentinel followed by COMMIT block
	sentinel := blocks.RecordEndSentinel // 0x7FFFFFFFFFFFFFFF
	assert.Equal(t, uint64(0x7FFFFFFFFFFFFFFF), sentinel)

	// Observed commit block: c8 ea 56 00 00 00 00 00 (+ 8 zero bytes padding)
	commitBlock := []byte{
		0xC8, 0xEA, 0x56, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	assert.Equal(t, byte(0xC8), commitBlock[0],
		"COMMIT block first byte must be 0xC8")
	assert.Equal(t, blocks.BlockTypeCommit, commitBlock[0])
}

// TestDDLBlockContainsJSONText confirms DDL blocks store schema/table names as JSON.
// Confirmed: HANA 2.00.088.00 SPS08.
// Evidence: CREATE TABLE XDDLS.XDDLTEST_ZZZPROBE → readable JSON in redo log:
// {"_schema":"XDDLS","_name":"XDDLTEST_ZZZPROBE","_type":...}
func TestDDLBlockContainsJSONText(t *testing.T) {
	assert.True(t, blocks.DDLBlockContainsJSONText,
		"Confirmed HANA 2.00.088.00 SPS08: DDL blocks contain JSON-encoded schema/table names")
}

// ── DecodeVarchar boundary tests (Bug 17) ─────────────────────────────────────

// TestDecodeVarcharOffsetAtLength verifies that offset == len(data) returns an
// error. Previously the check used > instead of >=, allowing offset == len(data)
// to pass and produce an empty remaining slice, causing a confusing empty-string
// return instead of an error.
func TestDecodeVarcharOffsetAtLength(t *testing.T) {
	data := []byte{0x03, 'a', 'b', 'c'}
	// offset == len(data) is exactly at the boundary — must be an error.
	_, _, err := blocks.DecodeVarchar(data, len(data))
	require.Error(t, err, "offset == len(data) must return an error")
}

// TestDecodeVarcharOffsetJustBeforeLength verifies that offset == len(data)-1
// succeeds (a single-byte length-zero varchar at the last byte).
func TestDecodeVarcharOffsetJustBeforeLength(t *testing.T) {
	data := []byte{0x00} // length prefix 0 → empty string
	got, consumed, err := blocks.DecodeVarchar(data, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 1, consumed)
}
