// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"strings"
)

// findArmPoint returns the first PointResult in points whose Arm matches
// arm. A binary-arm soak (see Scenario.IsBinaryArmScenario) is validated to
// exactly one point per arm, so "first" and "only" coincide in practice.
func findArmPoint(points []PointResult, arm string) (PointResult, bool) {
	for _, p := range points {
		if p.Arm == arm {
			return p, true
		}
	}
	return PointResult{}, false
}

// rssMaxBytes returns the largest RSSBytes sample in prom, 0 if prom is
// empty.
func rssMaxBytes(prom []PromPoint) uint64 {
	var max uint64
	for _, pp := range prom {
		if pp.RSSBytes > max {
			max = pp.RSSBytes
		}
	}
	return max
}

// backlogMaxSec returns the largest BacklogSec sample in backlog, 0 if
// backlog is empty (e.g. the scenario set no expected write rate).
func backlogMaxSec(backlog []BacklogPoint) float64 {
	var max float64
	for _, b := range backlog {
		if b.BacklogSec > max {
			max = b.BacklogSec
		}
	}
	return max
}

// soakComparisonRow is one line of the base-vs-PR table.
type soakComparisonRow struct {
	Metric string
	Base   string
	PR     string
	Delta  string
}

// pctDelta formats (pr-base)/base as a signed percentage, or "n/a" when
// base is zero (a percentage delta off a zero baseline is meaningless).
func pctDelta(base, pr float64) string {
	if base == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%+.1f%%", 100*(pr-base)/base)
}

// pctRow builds a row whose delta column is a percentage — used for the
// rate-like metrics (throughput, records/s) where "how much faster/slower"
// is the meaningful question.
func pctRow(metric string, base, pr float64) soakComparisonRow {
	return soakComparisonRow{
		Metric: metric,
		Base:   fmt.Sprintf("%.2f", base),
		PR:     fmt.Sprintf("%.2f", pr),
		Delta:  pctDelta(base, pr),
	}
}

// absRow builds a row whose delta column is an absolute difference — used
// for the resource/lag metrics (RSS, backlog) where a percentage off a
// possibly-near-zero baseline would be misleading.
func absRow(metric string, base, pr float64) soakComparisonRow {
	return soakComparisonRow{
		Metric: metric,
		Base:   fmt.Sprintf("%.2f", base),
		PR:     fmt.Sprintf("%.2f", pr),
		Delta:  fmt.Sprintf("%+.2f", pr-base),
	}
}

// slopeRow builds the RSS-slope row, rendering "n/a" for either side that
// didn't have enough Prometheus samples to fit a trend line (see
// rssSlopeBytesPerMin) rather than presenting a zero as if it were measured.
func slopeRow(baseSlope float64, baseOK bool, prSlope float64, prOK bool) soakComparisonRow {
	row := soakComparisonRow{Metric: "RSS slope (MB/min)", Base: "n/a", PR: "n/a", Delta: "n/a"}
	if baseOK {
		row.Base = fmt.Sprintf("%.2f", baseSlope/bytesPerMB)
	}
	if prOK {
		row.PR = fmt.Sprintf("%.2f", prSlope/bytesPerMB)
	}
	if baseOK && prOK {
		row.Delta = fmt.Sprintf("%+.2f", (prSlope-baseSlope)/bytesPerMB)
	}
	return row
}

// soakComparisonVerdict applies increment 4's rolling-baseline thresholds
// (soakThroughputRegressionFactor, soakRSSRegressionFactor) to a two-run
// base-vs-PR comparison: pr median throughput below the throughput factor
// of base, or pr RSS max above the RSS factor of base, is a REGRESSION.
// Reusing the exact same factors keeps the two comparators' verdicts
// consistent with each other, even though this one is a single noisier
// sample rather than a mature multi-run median.
func soakComparisonVerdict(base, pr PointResult, baseRSSMax, prRSSMax uint64) (verdict string, reasons []string) {
	if base.Summary.MedianMBPerSec > 0 && pr.Summary.MedianMBPerSec < soakThroughputRegressionFactor*base.Summary.MedianMBPerSec {
		reasons = append(reasons, fmt.Sprintf(
			"pr median throughput %.2f MB/s is below %.0f%% of base %.2f MB/s",
			pr.Summary.MedianMBPerSec, soakThroughputRegressionFactor*100, base.Summary.MedianMBPerSec))
	}
	if baseRSSMax > 0 && float64(prRSSMax) > soakRSSRegressionFactor*float64(baseRSSMax) {
		reasons = append(reasons, fmt.Sprintf(
			"pr RSS max %.0f MB is above %.0f%% of base %.0f MB",
			float64(prRSSMax)/bytesPerMB, soakRSSRegressionFactor*100, float64(baseRSSMax)/bytesPerMB))
	}
	if len(reasons) > 0 {
		return "REGRESSION", reasons
	}
	return "OK", reasons
}

// BuildSoakComparisonMarkdown renders a GitHub-markdown table comparing a
// soak scenario's base and PR binary arms — the runner-side half of CON-179
// R6 increment 5's PR-triggered A/B: a workflow builds a merge-base and a
// PR-head redpanda-connect binary, runs both as arms of the SAME soak
// scenario back to back on identical infra (see Arm.Binary and the
// binary-arm shape Scenario.Validate enforces), and this renders the result
// for posting to the PR.
//
// Unlike compareSoakBaseline's rolling multi-run median, this is a two-run
// comparison — noisier, one sample per arm — so its verdict is explicitly
// labelled as such rather than presented with the same confidence as the
// nightly baseline.
//
// Returns an error when either arm has no matching point in points, so a
// caller (runBench) can treat "this scenario's arms aren't named base/pr"
// as a skip rather than a crash.
func BuildSoakComparisonMarkdown(scenarioName string, points []PointResult, baseArm, prArm string) (string, error) {
	base, ok := findArmPoint(points, baseArm)
	if !ok {
		return "", fmt.Errorf("soak comparison: no point found for base arm %q", baseArm)
	}
	pr, ok := findArmPoint(points, prArm)
	if !ok {
		return "", fmt.Errorf("soak comparison: no point found for PR arm %q", prArm)
	}

	baseRSSMax := rssMaxBytes(base.Prom)
	prRSSMax := rssMaxBytes(pr.Prom)
	baseSlope, baseSlopeOK := rssSlopeBytesPerMin(base.Prom)
	prSlope, prSlopeOK := rssSlopeBytesPerMin(pr.Prom)

	rows := []soakComparisonRow{
		pctRow("Median MB/s", base.Summary.MedianMBPerSec, pr.Summary.MedianMBPerSec),
		pctRow("P5 MB/s", base.Summary.P5MBPerSec, pr.Summary.P5MBPerSec),
		pctRow("P95 MB/s", base.Summary.P95MBPerSec, pr.Summary.P95MBPerSec),
		pctRow("Median records/s", base.Summary.MedianMsgPerSec, pr.Summary.MedianMsgPerSec),
		absRow("RSS max (MB)", float64(baseRSSMax)/bytesPerMB, float64(prRSSMax)/bytesPerMB),
		slopeRow(baseSlope, baseSlopeOK, prSlope, prSlopeOK),
		absRow("Backlog max (s)", backlogMaxSec(base.Backlog), backlogMaxSec(pr.Backlog)),
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "### Soak comparison — %s\n\n", scenarioName)
	fmt.Fprintf(&sb,
		"Two-run comparison (%d base samples, %d PR samples) — noisier than the rolling %d-run baseline; treat as directional, not a verdict on its own.\n\n",
		len(base.Samples), len(pr.Samples), soakBaselineMaxPriorEntries)
	sb.WriteString("| metric | base | pr | delta |\n")
	sb.WriteString("|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&sb, "| %s | %s | %s | %s |\n", r.Metric, r.Base, r.PR, r.Delta)
	}
	sb.WriteString("\n")

	verdict, reasons := soakComparisonVerdict(base, pr, baseRSSMax, prRSSMax)
	fmt.Fprintf(&sb, "**Verdict: %s**", verdict)
	if len(reasons) > 0 {
		fmt.Fprintf(&sb, " — %s", strings.Join(reasons, "; "))
	}
	sb.WriteString("\n")
	return sb.String(), nil
}
