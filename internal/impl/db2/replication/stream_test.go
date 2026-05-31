// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPollQuery(t *testing.T) {
	s := &Streamer{config: StreamConfig{PollBatchSize: 100}}
	afterCSN := NewCSN(0)
	upperCSN := NewCSN(12345)

	query := s.buildPollQuery("ASNCDC.EMPLOYEES_CT", afterCSN, 0, upperCSN)

	// New LEAD/LAG query: selects computed IBMSNAP_OPCODE column then cdc.*
	assert.Contains(t, query, "IBMSNAP_OPCODE")
	assert.Contains(t, query, "LEAD(cdc.IBMSNAP_OPERATION")
	assert.Contains(t, query, "LAG(cdc.IBMSNAP_OPERATION")
	assert.Contains(t, query, "FROM ASNCDC.EMPLOYEES_CT cdc")
	assert.Contains(t, query, "cdc.IBMSNAP_COMMITSEQ >")
	assert.Contains(t, query, "cdc.IBMSNAP_COMMITSEQ <=")
	assert.Contains(t, query, "cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ")
	assert.Contains(t, query, "FETCH FIRST 100 ROWS ONLY")
	assert.Contains(t, query, "X'00000000000000000000'") // afterCSN = 0
	// 12345 = 0x3039 → uint64 big-endian in bytes 0-7: [0,0,0,0,0,0,0x30,0x39]; bytes 8-9: zeros
	assert.Contains(t, query, "X'00000000000030390000'") // 12345 = 0x3039
}

func TestBuildPollQueryHexPadding(t *testing.T) {
	s := &Streamer{config: StreamConfig{PollBatchSize: 1000}}
	afterCSN := NewCSN(255)   // 0xFF
	upperCSN := NewCSN(65535) // 0xFFFF

	query := s.buildPollQuery("SCHEMA.TABLE_CT", afterCSN, 0, upperCSN)

	// uint64 big-endian in bytes 0-7; bytes 8-9 are zero padding.
	// 0xFF → bytes 0-6: zero, byte 7: 0xFF, bytes 8-9: zero → "00000000000000FF0000"
	assert.Contains(t, query, "X'00000000000000FF0000'") // 255
	// 0xFFFF → byte 6: 0xFF, byte 7: 0xFF, others: zero → "000000000000FFFF0000"
	assert.Contains(t, query, "X'000000000000FFFF0000'") // 65535
}

func TestSortEventsByCSN(t *testing.T) {
	s := &Streamer{}
	events := []ChangeEvent{
		{CSN: NewCSN(300), IntentSeq: 1, Operation: OpTypeInsert},
		{CSN: NewCSN(100), IntentSeq: 2, Operation: OpTypeDelete},
		{CSN: NewCSN(200), IntentSeq: 1, Operation: OpTypeDelete},
		{CSN: NewCSN(100), IntentSeq: 1, Operation: OpTypeInsert},
	}

	sorted := s.sortEventsByCSN(events)

	require.Len(t, sorted, 4)
	assert.Equal(t, uint64(100), sorted[0].CSN.Uint64())
	assert.Equal(t, int64(1), sorted[0].IntentSeq)
	assert.Equal(t, uint64(100), sorted[1].CSN.Uint64())
	assert.Equal(t, int64(2), sorted[1].IntentSeq)
	assert.Equal(t, uint64(200), sorted[2].CSN.Uint64())
	assert.Equal(t, uint64(300), sorted[3].CSN.Uint64())
}

func TestSortEventsByCSNEmpty(t *testing.T) {
	s := &Streamer{}
	result := s.sortEventsByCSN(nil)
	assert.Nil(t, result)

	result = s.sortEventsByCSN([]ChangeEvent{})
	assert.Empty(t, result)
}

func TestGetBytesHelper(t *testing.T) {
	// nil pointer
	var v1 any = nil
	dest1 := &v1
	assert.Nil(t, getBytes(dest1))

	// []byte value
	expected := []byte{0x01, 0x02, 0x03}
	var v2 any = expected
	dest2 := &v2
	assert.Equal(t, expected, getBytes(dest2))

	// string value
	var v3 any = "hello"
	dest3 := &v3
	assert.Equal(t, []byte("hello"), getBytes(dest3))
}

func TestGetInt64Helper(t *testing.T) {
	var v1 any = int64(42)
	assert.Equal(t, int64(42), getInt64(&v1))

	var v2 any = int32(99)
	assert.Equal(t, int64(99), getInt64(&v2))

	var v3 any = "123"
	assert.Equal(t, int64(123), getInt64(&v3))

	var v4 any = nil
	assert.Equal(t, int64(0), getInt64(&v4))
}

// TestComputeSafeCSN verifies that the per-table watermark uses the minimum
// safe CSN, not the global penultimate.
//
// Bug scenario: TableA is full at CSN 50, TableB is not full with events up to
// CSN 100. The old code used the global penultimate (90) as maxCSN, causing
// the next poll (COMMITSEQ > 90) to permanently skip TableA's remaining rows
// at CSN 50. The fix computes per-table safe CSNs and takes the minimum.
func TestComputeSafeCSN(t *testing.T) {
	t.Parallel()
	upper := NewCSN(1000)

	t.Run("full table with all-same-CSN (edge case) narrows to tableMax", func(t *testing.T) {
		t.Parallel()
		// TableA: full batch, all rows at CSN 50 (edge case — advance anyway).
		// TableB: not full — no constraint.
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(50)}, {CSN: NewCSN(50)}}, full: true},
			{events: []ChangeEvent{{CSN: NewCSN(80)}}, full: false},
		}
		safe := computeSafeCSN(results, upper, NullCSN())
		assert.Equal(t, uint64(50), safe.Uint64())
	})

	t.Run("full table with distinct penultimate CSN narrows to penultimate", func(t *testing.T) {
		t.Parallel()
		// TableA: full batch, rows at CSN 40 and 50 → penultimate = 40.
		// TableB: not full, events up to CSN 80 → no constraint.
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(40)}, {CSN: NewCSN(50)}}, full: true},
			{events: []ChangeEvent{{CSN: NewCSN(80)}}, full: false},
		}
		safe := computeSafeCSN(results, upper, NullCSN())
		assert.Equal(t, uint64(40), safe.Uint64())
	})

	t.Run("no full tables — safe CSN equals upperCSN", func(t *testing.T) {
		t.Parallel()
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(50)}}, full: false},
			{events: []ChangeEvent{{CSN: NewCSN(80)}}, full: false},
		}
		safe := computeSafeCSN(results, upper, NullCSN())
		assert.Equal(t, upper.Uint64(), safe.Uint64())
	})

	t.Run("two full tables — minimum per-table safe CSN wins", func(t *testing.T) {
		t.Parallel()
		// TableA: full, penultimate = 30. TableB: full, penultimate = 70.
		// Global safe = min(30, 70) = 30.
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(30)}, {CSN: NewCSN(40)}}, full: true},
			{events: []ChangeEvent{{CSN: NewCSN(70)}, {CSN: NewCSN(90)}}, full: true},
		}
		safe := computeSafeCSN(results, upper, NullCSN())
		assert.Equal(t, uint64(30), safe.Uint64())
	})

	t.Run("full table with heldCSN and no events — safe CSN capped at afterCSN", func(t *testing.T) {
		t.Parallel()
		// Table has only a D-row at CSN 8; the D-row was stripped to pendingBeforeByTable.
		// events is empty, heldCSN = 8. afterCSN = 5. Safe must not advance past 5 so
		// the matching I@8 can be fetched next poll.
		held := NewCSN(8)
		after := NewCSN(5)
		results := []tableResult{
			{events: nil, full: true, heldCSN: &held},
		}
		safe := computeSafeCSN(results, upper, after)
		assert.Equal(t, uint64(5), safe.Uint64())
	})

	t.Run("full table with heldCSN and events below it — standard penultimate logic", func(t *testing.T) {
		t.Parallel()
		// Table has [evt@5, D@8]; D stripped to pending. events=[evt@5], heldCSN=8.
		// Penultimate logic still governs: tableSafe = 5 because there's only one event.
		held := NewCSN(8)
		after := NewCSN(3)
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(5)}}, full: true, heldCSN: &held},
		}
		safe := computeSafeCSN(results, upper, after)
		assert.Equal(t, uint64(5), safe.Uint64())
	})

	t.Run("full table with heldCSN and events at same CSN — advance to tableMax, composite pagination handles I-row", func(t *testing.T) {
		t.Parallel()
		// Table has [evt@8, D@8]; D stripped to pending. events=[evt@8], heldCSN=8.
		// tableSafe = tableMax = 8 (all rows share one CSN → edge case advances to max).
		// The matching I-row at CSN=8 remains reachable via anyFullAtReturnCSN composite
		// pagination: next poll uses "(COMMITSEQ = 8 AND INTENTSEQ > maxSeen)" to fetch it.
		// Capping at afterCSN=5 was the OLD behavior and caused an infinite loop when the
		// full batch (PollBatchSize rows) all shared one CSN — every event was trimmed.
		held := NewCSN(8)
		after := NewCSN(5)
		results := []tableResult{
			{events: []ChangeEvent{{CSN: NewCSN(8)}}, full: true, heldCSN: &held},
		}
		safe := computeSafeCSN(results, upper, after)
		assert.Equal(t, uint64(8), safe.Uint64())
	})

	t.Run("PollBatchSize events all same CSN with heldCSN — no infinite loop regression", func(t *testing.T) {
		t.Parallel()
		// Regression test for: large transaction produces ≥ PollBatchSize rows at CSN=5,
		// with D-row at tail (stripped to heldCSN). events = 999 rows all at CSN=5.
		// Old code capped safeCSN at afterCSN=4, trimming all 999 events → infinite loop.
		// New code: tableSafe = tableMax = 5; I-row reachable via composite pagination.
		events := make([]ChangeEvent, 999)
		for i := range events {
			events[i] = ChangeEvent{CSN: NewCSN(5)}
		}
		held := NewCSN(5)
		after := NewCSN(4)
		results := []tableResult{
			{events: events, full: true, heldCSN: &held},
		}
		safe := computeSafeCSN(results, upper, after)
		// Must be 5, not 4 — advancing to tableMax allows all 999 events to be emitted.
		assert.Equal(t, uint64(5), safe.Uint64())
	})
}

// TestComputeReturnCSN verifies that the watermark returned from a poll round is
// the minimum of the safe per-table ceiling and the actual max event CSN.
func TestComputeReturnCSN(t *testing.T) {
	t.Parallel()

	t.Run("no full tables — capped at max event CSN not upper", func(t *testing.T) {
		t.Parallel()
		safe := NewCSN(1000) // safeCSN == upperCSN when no table is full
		sorted := []ChangeEvent{{CSN: NewCSN(50)}, {CSN: NewCSN(80)}}
		assert.Equal(t, uint64(80), computeReturnCSN(safe, sorted).Uint64())
	})

	t.Run("full table — safe ceiling is below max event, safe wins", func(t *testing.T) {
		t.Parallel()
		safe := NewCSN(40) // penultimate from a full batch
		sorted := []ChangeEvent{{CSN: NewCSN(30)}, {CSN: NewCSN(40)}}
		assert.Equal(t, uint64(40), computeReturnCSN(safe, sorted).Uint64())
	})

	t.Run("empty sorted slice — returns safeCSN unchanged", func(t *testing.T) {
		t.Parallel()
		safe := NewCSN(100)
		assert.Equal(t, uint64(100), computeReturnCSN(safe, nil).Uint64())
	})

	t.Run("single event below safe — returns event CSN", func(t *testing.T) {
		t.Parallel()
		safe := NewCSN(500)
		sorted := []ChangeEvent{{CSN: NewCSN(200)}}
		assert.Equal(t, uint64(200), computeReturnCSN(safe, sorted).Uint64())
	})
}

// TestTrimToSafeCSN verifies the safe-ceiling trim helper.
//
// Regression: the old inline code initialised trimIdx = len(sorted) instead of 0,
// so when every event exceeded safeCSN (heldCSN edge case where safeCSN == afterCSN)
// the loop found no safe event and the full slice was incorrectly returned — emitting
// events above the safe ceiling and permanently advancing the watermark.
func TestTrimToSafeCSN(t *testing.T) {
	t.Parallel()

	t.Run("all events below safe — no trimming", func(t *testing.T) {
		t.Parallel()
		events := []ChangeEvent{{CSN: NewCSN(10)}, {CSN: NewCSN(20)}}
		got := trimToSafeCSN(events, NewCSN(50))
		assert.Equal(t, events, got)
	})

	t.Run("some events above safe — tail trimmed", func(t *testing.T) {
		t.Parallel()
		events := []ChangeEvent{{CSN: NewCSN(10)}, {CSN: NewCSN(20)}, {CSN: NewCSN(30)}}
		got := trimToSafeCSN(events, NewCSN(20))
		require.Len(t, got, 2)
		assert.Equal(t, uint64(10), got[0].CSN.Uint64())
		assert.Equal(t, uint64(20), got[1].CSN.Uint64())
	})

	t.Run("all events above safe (heldCSN edge case) — returns empty", func(t *testing.T) {
		t.Parallel()
		// Bug scenario: safeCSN == afterCSN (= 5), all events are at CSN 10, 15, 20.
		// Old code (trimIdx = len(sorted)) would return all events unchanged.
		// Fixed code (trimIdx = 0) returns empty slice.
		events := []ChangeEvent{{CSN: NewCSN(10)}, {CSN: NewCSN(15)}, {CSN: NewCSN(20)}}
		got := trimToSafeCSN(events, NewCSN(5))
		assert.Empty(t, got, "all events above safe ceiling must be trimmed")
	})

	t.Run("empty slice — no-op", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, trimToSafeCSN(nil, NewCSN(100)))
	})

	t.Run("single event at safe — kept", func(t *testing.T) {
		t.Parallel()
		events := []ChangeEvent{{CSN: NewCSN(50)}}
		got := trimToSafeCSN(events, NewCSN(50))
		require.Len(t, got, 1)
		assert.Equal(t, uint64(50), got[0].CSN.Uint64())
	})
}

// TestBuildPollQueryBinaryColumnComparison verifies the X'...' hex literal
// format embedded in the poll query.  The literal must be exactly 2*byteLen
// characters to avoid SQLSTATE 22001.
//
// When CommitSeqByteLen is 0 (undetected), buildPollQuery defaults to 10 bytes.
// CSNs created via NewCSN (no rawBytes) write the uint64 big-endian into the
// first 8 bytes of the N-byte buffer; bytes 8..N-1 are zero.
func TestBuildPollQueryBinaryColumnComparison(t *testing.T) {
	t.Parallel()

	// CommitSeqByteLen=0 → buildPollQuery falls back to 10-byte default.
	s := &Streamer{config: StreamConfig{PollBatchSize: 50}}

	tests := []struct {
		name      string
		afterCSN  CSN
		upperCSN  CSN
		wantAfter string
		wantUpper string
	}{
		{
			name:      "zero bounds",
			afterCSN:  NewCSN(0),
			upperCSN:  NewCSN(0),
			wantAfter: "X'00000000000000000000'",
			wantUpper: "X'00000000000000000000'",
		},
		{
			// uint64 big-endian in bytes 0-7, zero-pad bytes 8-9.
			// 0x3E8 → bytes 0-7: [0,0,0,0,0,0,0x03,0xE8]; bytes 8-9: [0,0]
			// hex: "00000000000003E8" + "0000" = "00000000000003E80000"
			name:      "typical CDC range",
			afterCSN:  NewCSN(1000), // 0x3E8
			upperCSN:  NewCSN(2000), // 0x7D0
			wantAfter: "X'00000000000003E80000'",
			wantUpper: "X'00000000000007D00000'",
		},
		{
			// 0xFFFFFFFF → bytes 0-7: [0,0,0,0,0xFF,0xFF,0xFF,0xFF]; bytes 8-9: [0,0]
			// 0x100000000 → bytes 0-7: [0,0,0,0x01,0,0,0,0]; bytes 8-9: [0,0]
			name:      "large CSN spanning 32-bit boundary",
			afterCSN:  NewCSN(0xFFFFFFFF),
			upperCSN:  NewCSN(0x100000000),
			wantAfter: "X'00000000FFFFFFFF0000'",
			wantUpper: "X'00000001000000000000'",
		},
		{
			// rawBytes path: CSN from DB read uses rawBytes verbatim.
			// [0,0,0,0,0,0,0x17,0x14,0xAB,0x00] → "0000000000001714AB00"
			name:     "rawBytes path (10-byte DB2 CSN)",
			afterCSN: NewCSNFromBytes([]byte{0, 0, 0, 0, 0, 0, 0x17, 0x14, 0xAB, 0x00}),
			upperCSN: NewCSNFromBytes([]byte{0, 0, 0, 0, 0, 0, 0x17, 0x15, 0x00, 0x00}),
			// rawBytes has 10 bytes → SQLHex(10) = all 10 bytes verbatim
			wantAfter: "X'0000000000001714AB00'",
			wantUpper: "X'00000000000017150000'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := s.buildPollQuery("ASNCDC.T_CT", tc.afterCSN, 0, tc.upperCSN)
			assert.Contains(t, q, tc.wantAfter,
				"afterCSN hex literal must be 20-char for CHAR(10) FOR BIT DATA")
			assert.Contains(t, q, tc.wantUpper,
				"upperCSN hex literal must be 20-char for CHAR(10) FOR BIT DATA")
		})
	}
}

// TestBuildPollQueryIntentSeqHexLiteral verifies that composite (CSN, IntentSeq) pagination
// encodes afterIntentSeq as a CHAR(10) FOR BIT DATA hex literal — not a decimal integer.
// IBMSNAP_INTENTSEQ is typed CHAR(10) FOR BIT DATA on DB2 LUW; comparing it to a decimal
// integer literal is a type mismatch that DB2 would reject with SQLSTATE 42818.
func TestBuildPollQueryIntentSeqHexLiteral(t *testing.T) {
	t.Parallel()

	s := &Streamer{config: StreamConfig{PollBatchSize: 50}}
	afterCSN := NewCSN(5)
	upperCSN := NewCSN(10)

	tests := []struct {
		name           string
		afterIntentSeq int64
		wantHex        string // expected X'...' fragment in the query
		wantNotDecimal string // decimal form that must NOT appear verbatim
	}{
		{
			name:           "intentSeq=0 — simple pagination, no INTENTSEQ predicate",
			afterIntentSeq: 0,
			wantHex:        "", // no composite predicate at all
		},
		{
			// int64=1 into bytes[0:8] = 00 00 00 00 00 00 00 01,
			// then 0xFF 0xFF padding → 00 00 00 00 00 00 00 01 FF FF (10 bytes, 20 hex chars)
			name:           "intentSeq=1",
			afterIntentSeq: 1,
			wantHex:        "X'0000000000000001FFFF'",
			wantNotDecimal: "> 1)",
		},
		{
			// 999 = 0x3E7; bytes[0:8] = 00 00 00 00 00 00 03 E7,
			// then 0xFF 0xFF → 00 00 00 00 00 00 03 E7 FF FF (20 hex chars)
			name:           "intentSeq=999",
			afterIntentSeq: 999,
			wantHex:        "X'00000000000003E7FFFF'",
			wantNotDecimal: "> 999)",
		},
		{
			// 65536 = 0x10000; bytes[0:8] = 00 00 00 00 00 01 00 00,
			// then 0xFF 0xFF → 00 00 00 00 00 01 00 00 FF FF (20 hex chars)
			name:           "intentSeq=65536",
			afterIntentSeq: 65536,
			wantHex:        "X'0000000000010000FFFF'",
			wantNotDecimal: "> 65536)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := s.buildPollQuery("ASNCDC.T_CT", afterCSN, tc.afterIntentSeq, upperCSN)
			if tc.wantHex != "" {
				assert.Contains(t, q, tc.wantHex,
					"IBMSNAP_INTENTSEQ must be compared via X'...' hex literal (CHAR(10) FOR BIT DATA)")
			}
			if tc.wantNotDecimal != "" {
				assert.NotContains(t, q, tc.wantNotDecimal,
					"decimal comparison to CHAR(10) FOR BIT DATA is a type mismatch (SQLSTATE 42818)")
			}
		})
	}
}
