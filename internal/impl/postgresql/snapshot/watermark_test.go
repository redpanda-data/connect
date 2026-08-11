// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

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
