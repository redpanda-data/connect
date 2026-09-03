// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// soakBaselineMaxPriorEntries bounds how many prior runs the rolling
// baseline is computed from — recent enough to reflect the current build,
// not so many that one old regression permanently drags the median.
const soakBaselineMaxPriorEntries = 7

// soakBaselineMinMatureEntries is the fewest prior runs the comparator
// trusts enough to fail a build over. Below it, a median is too noisy to
// be anything but advisory — see compareSoakBaseline.
const soakBaselineMinMatureEntries = 3

// soakThroughputRegressionFactor: current median_mbps below this fraction
// of the baseline median is a throughput regression.
const soakThroughputRegressionFactor = 0.85

// soakRSSRegressionFactor: current rss_max_bytes above this multiple of
// the baseline's rss_max_bytes median is a memory regression.
const soakRSSRegressionFactor = 1.30

// SoakIndexLister is the narrow slice of S3 the rolling-baseline comparator
// needs to enumerate a scenario's prior soak-index entries. Tests fake
// this; production wires an instance backed by the real SDK — see
// NewSoakIndexLister.
type SoakIndexLister interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

type awsSoakIndexLister struct {
	client *s3.Client
}

// NewSoakIndexLister builds a SoakIndexLister backed by the AWS SDK in the
// given region.
func NewSoakIndexLister(ctx context.Context, region string) (SoakIndexLister, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsSoakIndexLister{client: s3.NewFromConfig(cfg)}, nil
}

func (a *awsSoakIndexLister) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return a.client.ListObjectsV2(ctx, in)
}

// FakeSoakIndexLister returns a canned key listing — for tests. Keys need
// not be pre-sorted; listRecentSoakIndexEntries sorts them itself.
type FakeSoakIndexLister struct {
	Keys []string
	Err  error
	// Requests records every ListObjectsV2Input this fake was called with,
	// in call order.
	Requests []*s3.ListObjectsV2Input
}

func (f *FakeSoakIndexLister) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	f.Requests = append(f.Requests, in)
	if f.Err != nil {
		return nil, f.Err
	}
	out := &s3.ListObjectsV2Output{}
	for _, k := range f.Keys {
		key := k
		out.Contents = append(out.Contents, s3types.Object{Key: &key})
	}
	return out, nil
}

// soakBaselineResult is compareSoakBaseline's pure output: whether each
// measured dimension regressed against the scenario's rolling baseline,
// and the numbers behind that verdict.
type soakBaselineResult struct {
	// PriorCount is how many prior runs (after excluding the current
	// session) fed the baseline, capped at soakBaselineMaxPriorEntries by
	// the caller that builds `prior` (see listRecentSoakIndexEntries).
	PriorCount int
	// Mature is PriorCount >= soakBaselineMinMatureEntries. False makes
	// every regression flag below permanently false too — see
	// compareSoakBaseline.
	Mature                 bool
	BaselineMedianMBps     float64
	BaselineRSSMedianBytes float64
	ThroughputRegressed    bool
	RSSRegressed           bool
}

// Regressed reports whether either dimension regressed against a mature
// baseline. An immature baseline's result is never regressed, by
// construction (see compareSoakBaseline) — this method exists so callers
// don't need to know that invariant to ask the one question they care
// about.
func (r soakBaselineResult) Regressed() bool {
	return r.ThroughputRegressed || r.RSSRegressed
}

// compareSoakBaseline compares current against its scenario's rolling
// baseline: the median of prior's median_mbps and the median of prior's
// rss_max_bytes, computed over prior EXCLUDING any entry whose SessionID
// equals currentSessionID (defensive — a caller's listing should already
// exclude it, but this must never double-count the very run being judged).
//
// Fewer than soakBaselineMinMatureEntries prior runs makes the median too
// noisy to fail a build on: Mature is false and neither regression flag is
// ever set, so callers must treat the result as advisory-only in that case
// (see printSoakBaselineComparison and compareSoakRunToBaseline).
func compareSoakBaseline(current soakIndexEntry, prior []soakIndexEntry, currentSessionID string) soakBaselineResult {
	filtered := make([]soakIndexEntry, 0, len(prior))
	for _, p := range prior {
		if p.SessionID == currentSessionID {
			continue
		}
		filtered = append(filtered, p)
	}

	res := soakBaselineResult{PriorCount: len(filtered)}
	if len(filtered) < soakBaselineMinMatureEntries {
		return res
	}
	res.Mature = true

	mbps := make([]float64, len(filtered))
	rss := make([]float64, len(filtered))
	for i, p := range filtered {
		mbps[i] = p.MedianMBps
		rss[i] = float64(p.RSSMaxBytes)
	}
	res.BaselineMedianMBps = median(mbps)
	res.BaselineRSSMedianBytes = median(rss)

	if current.MedianMBps < soakThroughputRegressionFactor*res.BaselineMedianMBps {
		res.ThroughputRegressed = true
	}
	if float64(current.RSSMaxBytes) > soakRSSRegressionFactor*res.BaselineRSSMedianBytes {
		res.RSSRegressed = true
	}
	return res
}

// median returns the middle value of vals (averaging the two middle values
// for an even-length input), without mutating vals.
func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// listRecentSoakIndexEntries lists up to soakBaselineMaxPriorEntries most
// recent soak-index entries for scenarioName under archiveBucket,
// excluding currentSessionID's own entry. "Most recent" exploits
// newSessionID's key format (bench-YYYYMMDD-HHMMSS) being lexicographically
// ordered identically to chronological order, rather than parsing
// StartedAt out of every entry just to sort them.
//
// A per-entry fetch/parse failure is logged and that entry is dropped from
// the baseline (never fatal) — one corrupt or half-written prior entry must
// not block every future comparison for the scenario. Returns an error
// only when the LIST call itself fails, since without a listing there is
// nothing to fall back to.
func listRecentSoakIndexEntries(ctx context.Context, lister SoakIndexLister, fetcher LogFetcher, archiveBucket, scenarioName, currentSessionID string) ([]soakIndexEntry, error) {
	prefix := fmt.Sprintf("soak-index/%s/", scenarioName)
	var keys []string
	var continuationToken *string
	for {
		out, err := lister.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &archiveBucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list soak-index entries for scenario %q: %w", scenarioName, err)
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	currentKey := fmt.Sprintf("%s%s.json", prefix, currentSessionID)

	var entries []soakIndexEntry
	for _, key := range keys {
		if key == currentKey {
			continue
		}
		if len(entries) >= soakBaselineMaxPriorEntries {
			break
		}
		entry, err := fetchSoakIndexEntry(ctx, fetcher, archiveBucket, key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v (excluded from soak baseline)\n", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func fetchSoakIndexEntry(ctx context.Context, fetcher LogFetcher, bucket, key string) (soakIndexEntry, error) {
	body, err := fetcher.Fetch(ctx, bucket, key)
	if err != nil {
		return soakIndexEntry{}, fmt.Errorf("fetch soak-index entry %s: %w", key, err)
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return soakIndexEntry{}, fmt.Errorf("read soak-index entry %s: %w", key, err)
	}
	var entry soakIndexEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return soakIndexEntry{}, fmt.Errorf("parse soak-index entry %s: %w", key, err)
	}
	return entry, nil
}

// printSoakBaselineComparison writes a small comparison table to stdout:
// current vs. baseline for both dimensions, with a pass/fail verdict per
// rule. An immature baseline is labeled ADVISORY so an operator watching
// the run understands why a numeric regression (if any) isn't failing the
// build.
func printSoakBaselineComparison(res soakBaselineResult, current soakIndexEntry) {
	label := "regression check"
	if !res.Mature {
		label = fmt.Sprintf("regression check — ADVISORY: baseline immature (%d runs, need %d)", res.PriorCount, soakBaselineMinMatureEntries)
	}
	fmt.Fprintf(stdout, "soak baseline comparison for %s (%s):\n", current.Scenario, label)
	fmt.Fprintf(stdout, "  %-16s %16s %16s %8s\n", "metric", "current", "baseline", "verdict")
	fmt.Fprintf(stdout, "  %-16s %16.2f %16.2f %8s\n",
		"median_mbps", current.MedianMBps, res.BaselineMedianMBps, soakBaselineVerdict(res.Mature, res.ThroughputRegressed))
	fmt.Fprintf(stdout, "  %-16s %16d %16.0f %8s\n",
		"rss_max_bytes", current.RSSMaxBytes, res.BaselineRSSMedianBytes, soakBaselineVerdict(res.Mature, res.RSSRegressed))
}

func soakBaselineVerdict(mature, regressed bool) string {
	if !mature {
		return "n/a"
	}
	if regressed {
		return "FAIL"
	}
	return "pass"
}

// soakBaselineRegressionError builds the error compareSoakRunToBaseline
// returns when a mature baseline flags a regression — named per failing
// rule, with the numbers and baseline size, so a failed run tells the
// operator exactly what moved without re-deriving it from the raw index
// entries. Returns nil when res.Regressed() is false.
func soakBaselineRegressionError(res soakBaselineResult, current soakIndexEntry) error {
	if !res.Regressed() {
		return nil
	}
	var reasons []string
	if res.ThroughputRegressed {
		reasons = append(reasons, fmt.Sprintf(
			"median_mbps %.2f is below %.0f%% of the %d-run baseline median %.2f",
			current.MedianMBps, soakThroughputRegressionFactor*100, res.PriorCount, res.BaselineMedianMBps))
	}
	if res.RSSRegressed {
		reasons = append(reasons, fmt.Sprintf(
			"rss_max_bytes %d is above %.0f%% of the %d-run baseline median %.0f",
			current.RSSMaxBytes, soakRSSRegressionFactor*100, res.PriorCount, res.BaselineRSSMedianBytes))
	}
	return fmt.Errorf("soak baseline regression detected for %q: %s", current.Scenario, strings.Join(reasons, "; "))
}

// compareSoakRunToBaseline is runBench's entry point into the
// rolling-baseline comparator. It builds the production SoakIndexLister
// and delegates to compareSoakRunToBaselineWithLister — split out so tests
// can inject a fake lister without touching AWS.
func compareSoakRunToBaseline(ctx context.Context, opts benchOpts, s *Scenario, sessionID string, current soakIndexEntry, fetcher LogFetcher) error {
	lister, err := NewSoakIndexLister(ctx, opts.region)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: build soak-index lister (non-fatal, skipping baseline comparison): %v\n", err)
		return nil
	}
	return compareSoakRunToBaselineWithLister(ctx, lister, opts, s, sessionID, current, fetcher)
}

// compareSoakRunToBaselineWithLister lists the scenario's recent prior
// soak-index entries, compares current against them, prints the small
// comparison table, and returns a non-nil error ONLY when a mature
// baseline (>= soakBaselineMinMatureEntries prior runs) flags a
// regression. Listing or per-entry fetch failures are non-fatal (logged,
// treated as if the baseline were merely immature) — a comparator that
// itself can't reach S3 must never fail a run whose actual measurement
// already archived successfully.
func compareSoakRunToBaselineWithLister(ctx context.Context, lister SoakIndexLister, opts benchOpts, s *Scenario, sessionID string, current soakIndexEntry, fetcher LogFetcher) error {
	prior, err := listRecentSoakIndexEntries(ctx, lister, fetcher, opts.soakArchiveBucket, s.Name, sessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v (non-fatal, skipping baseline comparison)\n", err)
		return nil
	}
	res := compareSoakBaseline(current, prior, sessionID)
	printSoakBaselineComparison(res, current)
	return soakBaselineRegressionError(res, current)
}
