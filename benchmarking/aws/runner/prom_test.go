// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshots_TwoFrames(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
process_cpu_seconds_total 87.4
###timestamp=1747856140
go_goroutines 314
process_cpu_seconds_total 92.1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Equal(t, int64(1747856130), snaps[0].UnixTime)
	require.Contains(t, snaps[0].Body, "go_goroutines 312")
	require.False(t, snaps[0].Errored)
	require.Equal(t, int64(1747856140), snaps[1].UnixTime)
	require.Contains(t, snaps[1].Body, "go_goroutines 314")
}

func TestParseSnapshots_ErrorFrame(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
###timestamp=1747856140
###scrape_error
###timestamp=1747856150
go_goroutines 314
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 3)
	require.False(t, snaps[0].Errored)
	require.True(t, snaps[1].Errored, "frame with ###scrape_error marked")
	require.False(t, snaps[2].Errored)
}

func TestParseSnapshots_IgnoresLeadingNoise(t *testing.T) {
	raw := `noisy line
another
###timestamp=100
go_goroutines 1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 1)
	require.Equal(t, int64(100), snaps[0].UnixTime)
}

func TestParseSnapshots_TruncatedLastFrame(t *testing.T) {
	raw := `###timestamp=100
go_goroutines 1
###timestamp=200
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Empty(t, strings.TrimSpace(snaps[1].Body))
}

func TestParseSnapshots_Empty(t *testing.T) {
	snaps := parseSnapshots(strings.NewReader(""))
	require.Empty(t, snaps)
}
