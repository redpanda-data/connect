// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func priorEntries(sessionIDs ...string) []soakIndexEntry {
	entries := make([]soakIndexEntry, len(sessionIDs))
	for i, id := range sessionIDs {
		entries[i] = soakIndexEntry{SessionID: id}
	}
	return entries
}

func TestCompareSoakBaseline_ImmatureBaselineNeverRegresses(t *testing.T) {
	current := soakIndexEntry{SessionID: "current", MedianMBps: 1, RSSMaxBytes: 999_999_999_999}
	prior := []soakIndexEntry{
		{SessionID: "a", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
	}
	res := compareSoakBaseline(current, prior, "current")
	require.False(t, res.Mature)
	require.Equal(t, 2, res.PriorCount)
	require.False(t, res.ThroughputRegressed)
	require.False(t, res.RSSRegressed)
	require.False(t, res.Regressed())
}

func TestCompareSoakBaseline_ThroughputRegression(t *testing.T) {
	// Baseline median_mbps = 100; 0.85*100 = 85. current at 80 < 85 -> regression.
	prior := []soakIndexEntry{
		{SessionID: "a", MedianMBps: 90, RSSMaxBytes: 100},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "c", MedianMBps: 110, RSSMaxBytes: 100},
	}
	current := soakIndexEntry{SessionID: "current", MedianMBps: 80, RSSMaxBytes: 100}
	res := compareSoakBaseline(current, prior, "current")
	require.True(t, res.Mature)
	require.InDelta(t, 100, res.BaselineMedianMBps, 1e-9)
	require.True(t, res.ThroughputRegressed)
	require.False(t, res.RSSRegressed)
	require.True(t, res.Regressed())
}

func TestCompareSoakBaseline_RSSRegression(t *testing.T) {
	// Baseline rss median = 100; 1.30*100 = 130. current at 131 > 130 -> regression.
	prior := []soakIndexEntry{
		{SessionID: "a", MedianMBps: 100, RSSMaxBytes: 90},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "c", MedianMBps: 100, RSSMaxBytes: 110},
	}
	current := soakIndexEntry{SessionID: "current", MedianMBps: 100, RSSMaxBytes: 131}
	res := compareSoakBaseline(current, prior, "current")
	require.True(t, res.Mature)
	require.InDelta(t, 100, res.BaselineRSSMedianBytes, 1e-9)
	require.False(t, res.ThroughputRegressed)
	require.True(t, res.RSSRegressed)
	require.True(t, res.Regressed())
}

func TestCompareSoakBaseline_BothDimensionsPass(t *testing.T) {
	prior := []soakIndexEntry{
		{SessionID: "a", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "c", MedianMBps: 100, RSSMaxBytes: 100},
	}
	current := soakIndexEntry{SessionID: "current", MedianMBps: 100, RSSMaxBytes: 100}
	res := compareSoakBaseline(current, prior, "current")
	require.True(t, res.Mature)
	require.False(t, res.ThroughputRegressed)
	require.False(t, res.RSSRegressed)
	require.False(t, res.Regressed())
}

func TestCompareSoakBaseline_ExactlyAtThresholdIsNotARegression(t *testing.T) {
	// current == exactly 0.85 * baseline / 1.30 * baseline must NOT regress
	// (the rules are strictly < / strictly >).
	prior := []soakIndexEntry{
		{SessionID: "a", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "c", MedianMBps: 100, RSSMaxBytes: 100},
	}
	current := soakIndexEntry{SessionID: "current", MedianMBps: 85, RSSMaxBytes: 130}
	res := compareSoakBaseline(current, prior, "current")
	require.True(t, res.Mature)
	require.False(t, res.ThroughputRegressed)
	require.False(t, res.RSSRegressed)
}

func TestCompareSoakBaseline_ExcludesCurrentSessionID(t *testing.T) {
	// The listing handed to compareSoakBaseline includes the just-uploaded
	// current entry (as if a caller's exclusion upstream failed) — it must
	// still be excluded here, and with it gone, only 2 genuinely-prior
	// entries remain: below the maturity floor.
	prior := []soakIndexEntry{
		{SessionID: "current", MedianMBps: 1, RSSMaxBytes: 1},
		{SessionID: "a", MedianMBps: 100, RSSMaxBytes: 100},
		{SessionID: "b", MedianMBps: 100, RSSMaxBytes: 100},
	}
	current := soakIndexEntry{SessionID: "current", MedianMBps: 100, RSSMaxBytes: 100}
	res := compareSoakBaseline(current, prior, "current")
	require.Equal(t, 2, res.PriorCount, "the current session's own entry must not count toward the baseline")
	require.False(t, res.Mature)
}

func TestCompareSoakBaseline_MinimumMatureBoundary(t *testing.T) {
	current := soakIndexEntry{SessionID: "current", MedianMBps: 100, RSSMaxBytes: 100}

	res2 := compareSoakBaseline(current, priorEntriesWithValues(2, 100, 100), "current")
	require.False(t, res2.Mature, "2 prior entries must remain immature")

	res3 := compareSoakBaseline(current, priorEntriesWithValues(3, 100, 100), "current")
	require.True(t, res3.Mature, "3 prior entries must be the maturity floor")
}

func priorEntriesWithValues(n int, mbps float64, rss uint64) []soakIndexEntry {
	entries := make([]soakIndexEntry, n)
	for i := range entries {
		entries[i] = soakIndexEntry{SessionID: fmt.Sprintf("prior-%d", i), MedianMBps: mbps, RSSMaxBytes: rss}
	}
	return entries
}

func TestMedian(t *testing.T) {
	require.Equal(t, float64(0), median(nil))
	require.Equal(t, float64(5), median([]float64{5}))
	require.Equal(t, float64(2), median([]float64{3, 1, 2}))
	require.Equal(t, float64(2.5), median([]float64{1, 2, 3, 4}))

	// median must not mutate its input.
	in := []float64{3, 1, 2}
	_ = median(in)
	require.Equal(t, []float64{3, 1, 2}, in)
}

func TestSoakBaselineRegressionError_NamesFailingRuleAndNumbers(t *testing.T) {
	res := soakBaselineResult{
		Mature: true, PriorCount: 5,
		BaselineMedianMBps: 100, ThroughputRegressed: true,
	}
	current := soakIndexEntry{Scenario: "postgres_cdc-soak", MedianMBps: 80, RSSMaxBytes: 1}
	err := soakBaselineRegressionError(res, current)
	require.Error(t, err)
	require.ErrorContains(t, err, "postgres_cdc-soak")
	require.ErrorContains(t, err, "80.00")
	require.ErrorContains(t, err, "100.00")
	require.ErrorContains(t, err, "5-run")
}

func TestSoakBaselineRegressionError_NilWhenNoRegression(t *testing.T) {
	res := soakBaselineResult{Mature: true, PriorCount: 5}
	require.NoError(t, soakBaselineRegressionError(res, soakIndexEntry{}))
}

func TestSoakBaselineRegressionError_BothDimensionsNamedTogether(t *testing.T) {
	res := soakBaselineResult{
		Mature: true, PriorCount: 4,
		ThroughputRegressed: true, RSSRegressed: true,
		BaselineMedianMBps: 50, BaselineRSSMedianBytes: 1000,
	}
	current := soakIndexEntry{Scenario: "x", MedianMBps: 10, RSSMaxBytes: 2000}
	err := soakBaselineRegressionError(res, current)
	require.ErrorContains(t, err, "median_mbps")
	require.ErrorContains(t, err, "rss_max_bytes")
}

func TestListRecentSoakIndexEntries_TakesUpTo7MostRecentExcludingCurrent(t *testing.T) {
	const scenario = "postgres_cdc-soak"
	const current = "bench-20260817-120000"
	// 9 prior sessions plus the current one, all under the scenario's
	// prefix. Chronological order is lexicographic (bench-YYYYMMDD-HHMMSS).
	sessionIDs := []string{
		"bench-20260810-000000", "bench-20260811-000000", "bench-20260812-000000",
		"bench-20260813-000000", "bench-20260814-000000", "bench-20260815-000000",
		"bench-20260816-000000", "bench-20260816-120000", "bench-20260817-000000",
		current,
	}
	lister := &FakeSoakIndexLister{}
	fetcher := &FakeLogFetcher{Contents: map[string]string{}}
	for _, id := range sessionIDs {
		key := fmt.Sprintf("soak-index/%s/%s.json", scenario, id)
		lister.Keys = append(lister.Keys, key)
		raw, err := json.Marshal(soakIndexEntry{SessionID: id, MedianMBps: 1})
		require.NoError(t, err)
		fetcher.Contents[key] = string(raw)
	}

	entries, err := listRecentSoakIndexEntries(context.Background(), lister, fetcher, "archive-bucket", scenario, current)
	require.NoError(t, err)
	require.Len(t, entries, 7, "must cap at soakBaselineMaxPriorEntries and never include the current session")

	var got []string
	for _, e := range entries {
		got = append(got, e.SessionID)
		require.NotEqual(t, current, e.SessionID)
	}
	// Most recent first: bench-20260817-000000 down through bench-20260812-000000.
	require.Equal(t, []string{
		"bench-20260817-000000", "bench-20260816-120000", "bench-20260816-000000",
		"bench-20260815-000000", "bench-20260814-000000", "bench-20260813-000000",
		"bench-20260812-000000",
	}, got)
}

func TestListRecentSoakIndexEntries_ListSentCorrectPrefix(t *testing.T) {
	lister := &FakeSoakIndexLister{}
	fetcher := &FakeLogFetcher{Contents: map[string]string{}}
	_, err := listRecentSoakIndexEntries(context.Background(), lister, fetcher, "bucket", "my-scenario", "bench-current")
	require.NoError(t, err)
	require.Len(t, lister.Requests, 1)
	require.Equal(t, "soak-index/my-scenario/", *lister.Requests[0].Prefix)
	require.Equal(t, "bucket", *lister.Requests[0].Bucket)
}

func TestListRecentSoakIndexEntries_CorruptEntryIsSkippedNotFatal(t *testing.T) {
	const scenario = "s"
	lister := &FakeSoakIndexLister{Keys: []string{
		"soak-index/s/bench-a.json",
		"soak-index/s/bench-b.json",
	}}
	fetcher := &FakeLogFetcher{Contents: map[string]string{
		"soak-index/s/bench-a.json": "{not valid json",
		"soak-index/s/bench-b.json": `{"session_id":"bench-b","median_mbps":42}`,
	}}
	entries, err := listRecentSoakIndexEntries(context.Background(), lister, fetcher, "bucket", scenario, "bench-current")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "bench-b", entries[0].SessionID)
}

func TestListRecentSoakIndexEntries_ListErrorPropagates(t *testing.T) {
	lister := &FakeSoakIndexLister{Err: errors.New("access denied")}
	fetcher := &FakeLogFetcher{}
	_, err := listRecentSoakIndexEntries(context.Background(), lister, fetcher, "bucket", "s", "bench-current")
	require.ErrorContains(t, err, "access denied")
}

func TestCompareSoakRunToBaseline_NoRegressionReturnsNil(t *testing.T) {
	prev := stdout
	stdout = discardWriter{}
	defer func() { stdout = prev }()

	lister := &FakeSoakIndexLister{}
	fetcher := &FakeLogFetcher{Contents: map[string]string{}}
	for _, id := range []string{"bench-a", "bench-b", "bench-c"} {
		key := fmt.Sprintf("soak-index/scn/%s.json", id)
		lister.Keys = append(lister.Keys, key)
		raw, _ := json.Marshal(soakIndexEntry{SessionID: id, MedianMBps: 100, RSSMaxBytes: 100})
		fetcher.Contents[key] = string(raw)
	}
	current := soakIndexEntry{Scenario: "scn", SessionID: "bench-current", MedianMBps: 100, RSSMaxBytes: 100}

	err := compareSoakRunToBaselineWithLister(context.Background(), lister, benchOpts{soakArchiveBucket: "bucket"}, &Scenario{Name: "scn"}, "bench-current", current, fetcher)
	require.NoError(t, err)
}

func TestCompareSoakRunToBaseline_RegressionReturnsError(t *testing.T) {
	prev := stdout
	stdout = discardWriter{}
	defer func() { stdout = prev }()

	lister := &FakeSoakIndexLister{}
	fetcher := &FakeLogFetcher{Contents: map[string]string{}}
	for _, id := range []string{"bench-a", "bench-b", "bench-c"} {
		key := fmt.Sprintf("soak-index/scn/%s.json", id)
		lister.Keys = append(lister.Keys, key)
		raw, _ := json.Marshal(soakIndexEntry{SessionID: id, MedianMBps: 100, RSSMaxBytes: 100})
		fetcher.Contents[key] = string(raw)
	}
	current := soakIndexEntry{Scenario: "scn", SessionID: "bench-current", MedianMBps: 1, RSSMaxBytes: 100}

	err := compareSoakRunToBaselineWithLister(context.Background(), lister, benchOpts{soakArchiveBucket: "bucket"}, &Scenario{Name: "scn"}, "bench-current", current, fetcher)
	require.Error(t, err)
	require.ErrorContains(t, err, "scn")
	require.ErrorContains(t, err, "median_mbps")
}

// discardWriter is a minimal io.Writer that drops everything, used to
// silence stdout in tests that exercise print-heavy functions without
// caring about their output.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
