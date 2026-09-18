// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestEncodeRowsJoinsWithNewlines(t *testing.T) {
	body, n, err := EncodeRows([][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`)}, false)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	want := "{\"a\":1}\n{\"a\":2}\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
	if n != len(want) {
		t.Errorf("uncompressedLen = %d, want %d", n, len(want))
	}
}

func TestEncodeRowsTrimsSurroundingWhitespace(t *testing.T) {
	body, _, err := EncodeRows([][]byte{[]byte("  {\"a\":1}\n")}, false)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if string(body) != "{\"a\":1}\n" {
		t.Errorf("body = %q, want a single trimmed line", body)
	}
}

func TestEncodeRowsCompressedRoundTrips(t *testing.T) {
	rows := [][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`)}
	body, n, err := EncodeRows(rows, true)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if n != len("{\"a\":1}\n{\"a\":2}\n") {
		t.Errorf("uncompressedLen = %d, want the pre-compression length", n)
	}
	dec, err := zstd.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(dec); err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if out.String() != "{\"a\":1}\n{\"a\":2}\n" {
		t.Errorf("round trip = %q", out.String())
	}
}

func TestEncodeRowsRejectsOversizeBatch(t *testing.T) {
	big := bytes.Repeat([]byte("x"), MaxRequestBytes+1)
	if _, _, err := EncodeRows([][]byte{big}, false); err == nil {
		t.Fatal("expected an explicit error for an oversize batch, got nil")
	}
}

func TestEncodeRowsSkipsEmptyRows(t *testing.T) {
	body, _, err := EncodeRows([][]byte{[]byte(`{"a":1}`), {}, []byte("   ")}, false)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if string(body) != "{\"a\":1}\n" {
		t.Errorf("body = %q, want empty rows skipped", body)
	}
}

// TestEncodeRowsSilentlyDropsAllEmptyAndWhitespaceOnlyRows pins a finding, not
// a fix: an entirely empty or whitespace-only batch -- the shape an empty
// Benthos message produces -- yields zero output rows and zero error. There
// is no signal anywhere that anything was dropped. This is deliberate
// existing behaviour; this test documents it rather than asserting it as
// correct.
func TestEncodeRowsSilentlyDropsAllEmptyAndWhitespaceOnlyRows(t *testing.T) {
	body, n, err := EncodeRows([][]byte{{}, []byte("   "), []byte("\n\t "), nil}, false)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("body = %q, want empty -- an all-empty/whitespace batch produces no rows", body)
	}
	if n != 0 {
		t.Errorf("uncompressedLen = %d, want 0", n)
	}
}

// TestEncodeRowsCompressedEmptyInputRoundTrips mirrors the SDK's
// test_compress_empty_input (rust/src/compression/compressor.rs): compressing
// zero bytes must still produce a valid zstd frame that decodes back to
// empty, not an error and not a malformed frame.
func TestEncodeRowsCompressedEmptyInputRoundTrips(t *testing.T) {
	body, n, err := EncodeRows(nil, true)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if n != 0 {
		t.Errorf("uncompressedLen = %d, want 0", n)
	}
	dec, err := zstd.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("zstd reader on empty-input frame: %v", err)
	}
	defer dec.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(dec); err != nil {
		t.Fatalf("decompress empty-input frame: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("round trip = %q, want empty", out.String())
	}
}

// TestEncodeRowsCompressedLargeInputRoundTrips mirrors the SDK's
// test_compress_large_input / test_compress_multi_chunk
// (rust/src/compression/compressor.rs, BUFFER_SIZE = 4096): the input must
// cross zstd's internal chunk boundary and still round-trip byte-for-byte,
// catching truncation or corruption at a multi-write boundary that a
// single small row would never exercise.
func TestEncodeRowsCompressedLargeInputRoundTrips(t *testing.T) {
	var rows [][]byte
	var want bytes.Buffer
	// Each row is 100 bytes before the trailing newline EncodeRows adds; 100
	// rows comfortably clears the 4096-byte internal buffer several times
	// over while staying far under MaxRequestBytes.
	row := bytes.Repeat([]byte("x"), 100)
	for range 100 {
		rows = append(rows, row)
		want.Write(row)
		want.WriteByte('\n')
	}
	body, n, err := EncodeRows(rows, true)
	if err != nil {
		t.Fatalf("EncodeRows: %v", err)
	}
	if n != want.Len() {
		t.Errorf("uncompressedLen = %d, want %d", n, want.Len())
	}
	dec, err := zstd.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(dec); err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(out.Bytes(), want.Bytes()) {
		t.Errorf("round trip mismatch: got %d bytes, want %d bytes", out.Len(), want.Len())
	}
}

// TestEncodeRowsExactlyAtLimitDoesNotError and
// TestEncodeRowsOneByteOverLimitErrors pin our own boundary precisely: the
// SDK has no equivalent hard-reject-at-4MB test to compare against (its 4MB
// threshold is a soft Inline/FileFragment mode switch in mode_selector.rs,
// not a reject -- verified against the SDK source).
// EncodeRows uses a strict > against MaxRequestBytes; these tests make that
// boundary explicit rather than leaving it implied by the > operator alone.
func TestEncodeRowsExactlyAtLimitDoesNotError(t *testing.T) {
	// One row of exactly MaxRequestBytes-1 content bytes plus the trailing
	// newline EncodeRows adds lands the uncompressed total at exactly
	// MaxRequestBytes.
	row := bytes.Repeat([]byte("x"), MaxRequestBytes-1)
	_, n, err := EncodeRows([][]byte{row}, false)
	if err != nil {
		t.Fatalf("EncodeRows at exactly MaxRequestBytes: %v", err)
	}
	if n != MaxRequestBytes {
		t.Fatalf("uncompressedLen = %d, want exactly MaxRequestBytes (%d)", n, MaxRequestBytes)
	}
}

func TestEncodeRowsOneByteOverLimitErrors(t *testing.T) {
	row := bytes.Repeat([]byte("x"), MaxRequestBytes)
	if _, _, err := EncodeRows([][]byte{row}, false); err == nil {
		t.Fatal("expected an error one byte over MaxRequestBytes, got nil")
	}
}
