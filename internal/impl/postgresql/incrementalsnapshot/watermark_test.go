// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
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
	// Xmin is the oldest transaction still in flight, so anything at or
	// above it began late enough to prove the window has opened.
	wm := Watermark{Xmin: 100, Xmax: 105}

	assert.False(t, wm.OpensAt(99))
	assert.True(t, wm.OpensAt(100), "xmin itself must open the window")
	assert.True(t, wm.OpensAt(101))
}

func TestWatermarkClosesAt(t *testing.T) {
	// Xmax is the first id not yet assigned, so only ids strictly above it
	// are known to have started after the watermark was taken.
	wm := Watermark{Xmin: 100, Xmax: 105}

	assert.False(t, wm.ClosesAt(104))
	assert.False(t, wm.ClosesAt(105), "xmax itself must not close the window")
	assert.True(t, wm.ClosesAt(106))
}

func TestWatermarkZeroValueDoesNotClose(t *testing.T) {
	// The coordinator holds a zero Watermark until its first chunk is
	// planned. It must not report a window as closed before then.
	var wm Watermark

	assert.False(t, wm.ClosesAt(0))
	assert.True(t, wm.OpensAt(0))
}
