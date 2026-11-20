// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func matchEntry(id, output string, size int, mode string, recorded time.Time) Entry {
	return Entry{
		ID:          id,
		Input:       "postgres_cdc",
		Output:      output,
		Processors:  []string{"mapping"},
		Fingerprint: "fp1",
		Workload:    Workload{MessageSizeBytes: size, Mode: mode},
		RecordedAt:  recorded,
	}
}

func TestFindMatches(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := []Entry{
		matchEntry("far", "kafka", 2048, "cdc", t0),
		matchEntry("exact", "kafka", 1024, "cdc", t0),
		matchEntry("other-output", "s3", 1024, "cdc", t0),
	}
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, Fingerprint: "fp1", MessageSizeBytes: 1024, Mode: "cdc"}

	ms := FindMatches(entries, q)
	require.Len(t, ms, 2)
	require.Equal(t, "exact", ms[0].Entry.ID)
	require.Equal(t, MatchExact, ms[0].Kind)
	require.Equal(t, "far", ms[1].Entry.ID)
	require.Equal(t, MatchClose, ms[1].Kind)
	require.Contains(t, ms[1].Differences, "message size 2048 vs 1024 bytes")
}

func TestFindMatchesSizeTolerance(t *testing.T) {
	t0 := time.Now()
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, MessageSizeBytes: 1000, Mode: "cdc"}
	ms := FindMatches([]Entry{matchEntry("a", "kafka", 1100, "cdc", t0)}, q)
	require.Equal(t, MatchExact, ms[0].Kind)
	ms = FindMatches([]Entry{matchEntry("a", "kafka", 1101, "cdc", t0)}, q)
	require.Equal(t, MatchClose, ms[0].Kind)
}

func TestFindMatchesNewerWinsTie(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, Fingerprint: "fp1", MessageSizeBytes: 1024, Mode: "cdc"}
	ms := FindMatches([]Entry{
		matchEntry("old", "kafka", 1024, "cdc", t0),
		matchEntry("new", "kafka", 1024, "cdc", t0.Add(time.Hour)),
	}, q)
	require.Len(t, ms, 2)
	require.Equal(t, "new", ms[0].Entry.ID)
}

func TestFindMatchesFingerprintAndProcessors(t *testing.T) {
	t0 := time.Now()
	e := matchEntry("a", "kafka", 1024, "cdc", t0)
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, Fingerprint: "other", MessageSizeBytes: 1024, Mode: "cdc"}
	require.Equal(t, MatchClose, FindMatches([]Entry{e}, q)[0].Kind)

	// Empty query fingerprint ignores fingerprint but not processors.
	q.Fingerprint = ""
	require.Equal(t, MatchExact, FindMatches([]Entry{e}, q)[0].Kind)
	q.Processors = []string{"mapping", "branch"}
	m := FindMatches([]Entry{e}, q)[0]
	require.Equal(t, MatchClose, m.Kind)
	require.InDelta(t, 0.5, m.Score, 1e-9)
}

func TestFindMatchesModeDiffers(t *testing.T) {
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, MessageSizeBytes: 1024, Mode: "bulk"}
	m := FindMatches([]Entry{matchEntry("a", "kafka", 1024, "cdc", time.Now())}, q)[0]
	require.Equal(t, MatchClose, m.Kind)
	require.InDelta(t, 1, m.Score, 1e-9)
	require.NotEmpty(t, m.Differences)
}

func TestFindMatchesUnknownSize(t *testing.T) {
	t0 := time.Now()
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, Fingerprint: "fp1", MessageSizeBytes: 0, Mode: "cdc"}
	m := FindMatches([]Entry{matchEntry("a", "kafka", 1024, "cdc", t0)}, q)[0]
	require.Equal(t, MatchClose, m.Kind)
	require.InDelta(t, 0, m.Score, 1e-9)
	require.Empty(t, m.Differences)

	// Entry with unknown size is never exact either.
	q.MessageSizeBytes = 1024
	m = FindMatches([]Entry{matchEntry("b", "kafka", 0, "cdc", t0)}, q)[0]
	require.Equal(t, MatchClose, m.Kind)
	require.InDelta(t, 0, m.Score, 1e-9)
	require.Empty(t, m.Differences)
}

func TestFindMatchesIDTieBreak(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := Query{Input: "postgres_cdc", Output: "kafka", Processors: []string{"mapping"}, Fingerprint: "fp1", MessageSizeBytes: 1024, Mode: "cdc"}
	for range 5 {
		ms := FindMatches([]Entry{
			matchEntry("b", "kafka", 1024, "cdc", t0),
			matchEntry("a", "kafka", 1024, "cdc", t0),
		}, q)
		require.Equal(t, "a", ms[0].Entry.ID)
		require.Equal(t, "b", ms[1].Entry.ID)
	}
}
