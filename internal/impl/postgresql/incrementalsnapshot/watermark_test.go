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

func TestWatermark(t *testing.T) {
	t.Run("Opens at", func(t *testing.T) {
		wm := Watermark{Xmin: 100, Xmax: 105}

		assert.False(t, wm.OpensAt(99))
		assert.True(t, wm.OpensAt(100), "xmin itself must open the window")
		assert.True(t, wm.OpensAt(101))
	})

	t.Run("Closes at", func(t *testing.T) {
		wm := Watermark{Xmin: 100, Xmax: 105}

		assert.False(t, wm.ClosesAt(104))
		assert.False(t, wm.ClosesAt(105), "xmax itself must not close the window")
		assert.True(t, wm.ClosesAt(106))
	})

	t.Run("Zero value does not close", func(t *testing.T) {
		var wm Watermark

		assert.False(t, wm.ClosesAt(0))
		assert.True(t, wm.OpensAt(0))
	})
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
			name: "just ahead of ref",
			xid:  121,
			ref:  epoch + 120,
			want: epoch + 121,
		},
		{
			name: "xid before a wrap that ref is after",
			xid:  math.MaxUint32 - 10,
			ref:  epoch + 5,
			want: epoch - 11,
		},
		{
			name: "xid after a wrap that ref is before",
			xid:  5,
			ref:  epoch - 11,
			want: epoch + 5,
		},
		{
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
	const epoch = uint64(1) << 32
	wm := Watermark{Xmin: epoch + 100, Xmax: epoch + 105}

	assert.False(t, wm.OpensAt(99))
	assert.True(t, wm.OpensAt(100), "xmin itself must open the window")
	assert.True(t, wm.OpensAt(101))

	assert.False(t, wm.ClosesAt(105), "xmax itself must not close the window")
	assert.True(t, wm.ClosesAt(106))
}
