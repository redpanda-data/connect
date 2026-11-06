// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

func TestCompareOffsetTokens(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		wantPos bool // a > b
		wantNeg bool // a < b
	}{
		{name: "equal numeric", a: "9", b: "9"},
		{name: "digit count boundary", a: "10", b: "9", wantPos: true},
		{name: "digit count boundary reversed", a: "9", b: "10", wantNeg: true},
		{name: "larger digit count boundary", a: "1000", b: "999", wantPos: true},
		{name: "non numeric falls back to lexicographic", a: "b", b: "a", wantPos: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := compareOffsetTokens(test.a, test.b)
			switch {
			case test.wantPos:
				require.Positive(t, got)
			case test.wantNeg:
				require.Negative(t, got)
			default:
				require.Zero(t, got)
			}
		})
	}
}

func tokenBatch(tokens ...string) service.MessageBatch {
	batch := make(service.MessageBatch, len(tokens))
	for i, tok := range tokens {
		batch[i] = service.NewMessage([]byte(tok))
	}
	return batch
}

func batchContents(t *testing.T, batch service.MessageBatch) []string {
	t.Helper()
	out := make([]string, len(batch))
	for i, m := range batch {
		b, err := m.AsBytes()
		require.NoError(t, err)
		out[i] = string(b)
	}
	return out
}

// TestFilterAlreadyCommittedDigitBoundary reproduces CON-567: with a purely
// lexicographic comparison, a genuinely new row at offset "10" is
// incorrectly classified as already-committed (because the string "10"
// sorts before "9") once the channel's latest committed offset token is "9",
// so the row is silently dropped instead of being inserted.
func TestFilterAlreadyCommittedDigitBoundary(t *testing.T) {
	mapping, err := service.NewInterpolatedString("${! content() }")
	require.NoError(t, err)

	latest := streaming.OffsetToken("9")
	batch := tokenBatch("10")

	filtered, offsets, err := filterAlreadyCommitted(&latest, mapping, batch)
	require.NoError(t, err)
	require.Len(t, filtered, 1, "new row at a digit-count boundary must not be dropped")
	require.NotNil(t, offsets)
	require.Equal(t, streaming.OffsetToken("10"), offsets.Start)
	require.Equal(t, streaming.OffsetToken("10"), offsets.End)
}

// TestFilterAlreadyCommittedDropsRedelivered ensures already-committed rows
// redelivered after a restart are still correctly filtered out, including
// across a digit-count boundary.
func TestFilterAlreadyCommittedDropsRedelivered(t *testing.T) {
	mapping, err := service.NewInterpolatedString("${! content() }")
	require.NoError(t, err)

	latest := streaming.OffsetToken("10")
	batch := tokenBatch("9")

	filtered, offsets, err := filterAlreadyCommitted(&latest, mapping, batch)
	require.NoError(t, err)
	require.Empty(t, filtered, "already-committed row must not be re-inserted as a duplicate")
	require.Nil(t, offsets)
}

// TestFilterAlreadyCommittedMixedBatch checks a batch straddling the digit
// boundary: some rows already committed, some new.
func TestFilterAlreadyCommittedMixedBatch(t *testing.T) {
	mapping, err := service.NewInterpolatedString("${! content() }")
	require.NoError(t, err)

	latest := streaming.OffsetToken("9")
	batch := tokenBatch("8", "9", "10", "11")

	filtered, offsets, err := filterAlreadyCommitted(&latest, mapping, batch)
	require.NoError(t, err)
	require.Equal(t, []string{"10", "11"}, batchContents(t, filtered))
	require.NotNil(t, offsets)
	require.Equal(t, streaming.OffsetToken("10"), offsets.Start)
	require.Equal(t, streaming.OffsetToken("11"), offsets.End)
}

// TestFilterAlreadyCommittedNoPriorOffset ensures the fast path (no offset
// committed yet on the channel) still works as before.
func TestFilterAlreadyCommittedNoPriorOffset(t *testing.T) {
	mapping, err := service.NewInterpolatedString("${! content() }")
	require.NoError(t, err)

	batch := tokenBatch("1", "2", "3")

	filtered, offsets, err := filterAlreadyCommitted(nil, mapping, batch)
	require.NoError(t, err)
	require.Len(t, filtered, 3)
	require.NotNil(t, offsets)
	require.Equal(t, streaming.OffsetToken("1"), offsets.Start)
	require.Equal(t, streaming.OffsetToken("3"), offsets.End)
}
