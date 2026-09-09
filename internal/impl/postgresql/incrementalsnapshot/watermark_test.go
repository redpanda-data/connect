// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSnapshotValid(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Watermark
	}{
		{
			name: "with xip list",
			raw:  "100:104:101,103",
			want: Watermark{Xmin: 100, Xmax: 104},
		},
		{
			name: "without xip list",
			raw:  "100:104:",
			want: Watermark{Xmin: 100, Xmax: 104},
		},
		{
			name: "equal xmin and xmax",
			raw:  "50:50:",
			want: Watermark{Xmin: 50, Xmax: 50},
		},
		{
			name: "single xip entry",
			raw:  "10:20:15",
			want: Watermark{Xmin: 10, Xmax: 20},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSnapshot(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseSnapshotMalformed(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty string", raw: ""},
		{name: "missing colon", raw: "100104"},
		{name: "only one colon", raw: "100:104"},
		{name: "too many colons", raw: "100:104:101:extra"},
		{name: "non-numeric xmin", raw: "abc:104:"},
		{name: "non-numeric xmax", raw: "100:abc:"},
		{name: "non-numeric xip entry", raw: "100:104:abc"},
		{name: "negative xmin", raw: "-1:104:"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSnapshot(tc.raw)
			require.Error(t, err)
		})
	}
}

func TestWatermarkOpensAt(t *testing.T) {
	// Xmin is the oldest transaction that is in flight. Each value that is
	// equal to xmin or larger started late enough to open the window.
	wm := Watermark{Xmin: 100, Xmax: 105}

	assert.False(t, wm.OpensAt(99))
	assert.True(t, wm.OpensAt(100), "xmin itself must open the window")
	assert.True(t, wm.OpensAt(101))
}

func TestWatermarkClosesAt(t *testing.T) {
	// Xmax is the first id that Postgres had not given out. Only a larger
	// id started after the watermark.
	wm := Watermark{Xmin: 100, Xmax: 105}

	assert.False(t, wm.ClosesAt(104))
	assert.False(t, wm.ClosesAt(105), "xmax itself must not close the window")
	assert.True(t, wm.ClosesAt(106))
}

func TestWatermarkZeroValueDoesNotClose(t *testing.T) {
	// The coordinator holds a zero Watermark until it plans the first chunk.
	// Until then it must not report a closed window.
	var wm Watermark

	assert.False(t, wm.ClosesAt(0))
	assert.True(t, wm.OpensAt(0))
}

func TestNormalizeXID(t *testing.T) {
	const epoch = uint64(1) << 32

	tests := []struct {
		name string
		xid  uint32
		ref  uint64
		want uint64
	}{
		{
			name: "epoch 0 passes through",
			xid:  100,
			ref:  120,
			want: 100,
		},
		{
			// This case is the reason for the function. The watermark is in
			// epoch 1, and the xid has no epoch.
			name: "lifted into ref's epoch",
			xid:  100,
			ref:  epoch + 120,
			want: epoch + 100,
		},
		{
			name: "equal to ref within an epoch",
			xid:  100,
			ref:  epoch + 100,
			want: epoch + 100,
		},
		{
			// The xid is a little larger than ref. This case is usual for
			// a commit that arrives a short time after the watermark.
			name: "just ahead of ref",
			xid:  121,
			ref:  epoch + 120,
			want: epoch + 121,
		},
		{
			// ref is a little after a return to zero, and xid is a little
			// before it. The epoch of ref would put xid one full epoch too
			// late.
			name: "xid before a wrap that ref is after",
			xid:  math.MaxUint32 - 10,
			ref:  epoch + 5,
			want: epoch - 11,
		},
		{
			// This case is the opposite. ref is before a return to zero and
			// xid is after it.
			name: "xid after a wrap that ref is before",
			xid:  5,
			ref:  epoch - 11,
			want: epoch + 5,
		},
		{
			// Epoch 0 has no earlier epoch. A much larger xid must not
			// cause an underflow to a very large uint64.
			name: "no underflow below epoch 0",
			xid:  math.MaxUint32 - 10,
			ref:  5,
			want: uint64(math.MaxUint32) - 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, normalizeXID(test.xid, test.ref))
		})
	}
}

func TestWatermarkComparisonsSurviveEpochWraparound(t *testing.T) {
	// Regression test. The bounds are 64-bit values that include an epoch,
	// and the streamed xid is a 32-bit value. A direct comparison makes each
	// bound in epoch 1 or later larger than each possible xid. The window
	// then never opens and the snapshot stops without a message.
	const epoch = uint64(1) << 32
	wm := Watermark{Xmin: epoch + 100, Xmax: epoch + 105}

	assert.False(t, wm.OpensAt(99))
	assert.True(t, wm.OpensAt(100), "xmin itself must open the window")
	assert.True(t, wm.OpensAt(101))

	assert.False(t, wm.ClosesAt(105), "xmax itself must not close the window")
	assert.True(t, wm.ClosesAt(106))
}
