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
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// defaultCatalogBucket is the persistent bucket holding the results catalogue.
const defaultCatalogBucket = "redpanda-connect-bench-catalog"

// catalogMeta is the per-run context a Result does not carry.
type catalogMeta struct {
	SessionID, RunURL, ResultKey string
	Now                          time.Time
}

// gitDescribe returns the release-describe string of the checkout at
// repoRoot, or "unknown".
func gitDescribe(repoRoot string) string {
	out, err := exec.Command("git", "-C", repoRoot, "describe", "--tags", "--match", "v4.*").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// runURLFromEnv returns the GitHub Actions run URL, or "" outside Actions.
func runURLFromEnv() string {
	server, repo, id := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if server == "" || repo == "" || id == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/actions/runs/%s", server, repo, id)
}

// shouldCatalogue reports whether a run belongs in the catalogue: plain
// sweeps only, not soak runs nor arm or binary comparisons.
func shouldCatalogue(s *Scenario, r *Result) bool {
	if s.Soak {
		return false
	}
	for _, p := range r.Points {
		if p.Arm != "" || p.Binary != "" {
			return false
		}
	}
	return true
}

// catalogEntryFromResult adapts a Scenario and its Result into a
// catalog.RunInput and maps it to an Entry. The identity is never the
// rendered harness config: a source scenario uses
// catalog.BenchSourcePipeline; an e2e scenario uses its plan's catalog
// block, or, hand-written without one, its pipeline minus any harness
// benchmark processor.
func catalogEntryFromResult(s *Scenario, r *Result, meta catalogMeta) (catalog.Entry, error) {
	run := catalog.RunInput{
		ID:                meta.SessionID,
		Scenario:          r.Scenario,
		GitSHA:            r.GitSHA,
		ConnectVersion:    r.ConnectVersion,
		InstanceType:      r.Infra.RunnerInstanceType,
		Region:            r.Infra.Region,
		RunURL:            meta.RunURL,
		ResultKey:         meta.ResultKey,
		MessageSizeBytes:  r.Dataset.RowSizeBytes,
		OfferedRatePerSec: s.Workload.WriteRatePerSec,
		Soak:              s.Soak,
		MemoryLimited:     s.MemoryLimited(),
		SinkMeasured:      true,
		Now:               meta.Now,
	}
	e2e := s.Direction == DirectionE2E
	if e2e {
		setE2EIdentity(&run, s)
		// msg/s is measured at the output, but it is the input's message
		// count unless the pipeline filters or fans out; times the
		// workload's message size it is the input rate the pipeline
		// sustains, which is what a customer sizes for.
		run.CapacityBasis = catalog.CapacityBasisInputEquivalent
		if s.Sink != nil {
			run.SinkSize = s.Sink.Size
		}
	} else {
		run.Pipeline = catalog.BenchSourcePipeline(s.Pipeline)
	}
	unverified := false
	for _, p := range r.Points {
		rp := catalog.RunPoint{
			VCPU:        p.VCPU,
			Engine:      p.Engine,
			Arm:         p.Arm,
			Binary:      p.Binary,
			MedianMBps:  p.Summary.MedianMBPerSec,
			MedianMsgps: p.Summary.MedianMsgPerSec,
		}
		if e2e {
			rp.MedianMBps = p.Summary.MedianMsgPerSec * float64(r.Dataset.RowSizeBytes) / 1e6
			rp.OutputMBps = p.Summary.MedianMBPerSec
		}
		for _, a := range p.Anomalies {
			switch a.Kind {
			case anomalyProducerBound:
				rp.LimitedBy = catalog.LimitedByProducer
			case anomalyWorkloadBound:
				rp.LimitedBy = catalog.LimitedByWorkload
			case anomalySinkUnverified:
				unverified = true
			}
		}
		for _, pp := range p.Prom {
			rp.RSSBytes = append(rp.RSSBytes, pp.RSSBytes)
		}
		run.Points = append(run.Points, rp)
	}
	if unverified {
		run.Caveats = append(slices.Clone(run.Caveats), catalog.CaveatSinkUnverified)
	}
	return catalog.EntryFromRun(run)
}

// setE2EIdentity fills run's identity, caveats and sink measurement for an
// e2e scenario. A drop sink, or a plan's sink_not_measured caveat, means the
// output's throughput was not measured.
func setE2EIdentity(run *catalog.RunInput, s *Scenario) {
	run.SinkMeasured = s.Sink == nil || s.Sink.Kind != "drop"
	c := s.Catalog
	if c == nil {
		run.Pipeline = withoutBenchmark(s.Pipeline)
		return
	}
	run.Identity = &catalog.Identity{
		Fingerprint: c.Fingerprint,
		Input:       c.Input,
		Processors:  c.Processors,
		Output:      c.Output,
	}
	run.SourceApproximated = c.SourceApproximated
	run.Caveats = c.Caveats
	if slices.Contains(c.Caveats, catalog.CaveatSinkNotMeasured) {
		run.SinkMeasured = false
	}
}

// withoutBenchmark returns a copy of pipeline whose output.processors has no
// benchmark processor: that is harness measurement, not customer workload.
// pipeline itself is not changed.
func withoutBenchmark(pipeline map[string]any) map[string]any {
	out := maps.Clone(pipeline)
	o, ok := pipeline["output"].(map[string]any)
	if !ok {
		return out
	}
	procs, ok := o["processors"].([]any)
	if !ok {
		return out
	}
	o = maps.Clone(o)
	var kept []any
	for _, p := range procs {
		if m, ok := p.(map[string]any); ok {
			if _, bench := m["benchmark"]; bench {
				continue
			}
		}
		kept = append(kept, p)
	}
	if len(kept) == 0 {
		delete(o, "processors")
	} else {
		o["processors"] = kept
	}
	out["output"] = o
	return out
}

// treeDirty reports whether the checkout at repoRoot has uncommitted changes
// to tracked files. A var so tests can substitute it.
var treeDirty = func(repoRoot string) (bool, error) {
	out, err := exec.Command("git", "-C", repoRoot, "status", "--porcelain", "--untracked-files=no").Output()
	if err != nil {
		return false, fmt.Errorf("git status: %w", err)
	}
	return strings.TrimSpace(string(out)) != "", nil
}

// catalogueEntryMarker starts the line the runner prints once a catalogue
// entry is written. bench_bot.yml reads the entry id from it, anchored at
// column 0: remote output is always prefixed, so only the runner can print it.
const catalogueEntryMarker = "catalogue-entry-id: "

func catalogueEntryLine(id string) string { return catalogueEntryMarker + id + "\n" }

// putCatalogEntry writes e to the catalogue bucket. A var so tests can
// substitute it.
var putCatalogEntry = func(ctx context.Context, opts benchOpts, e catalog.Entry) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	return catalog.NewS3Store(s3.NewFromConfig(cfg), opts.catalogBucket).Put(ctx, e)
}

// catalogueSweep writes a completed sweep's entry to the catalogue bucket and
// returns its id, or "" when nothing was written. A build from a dirty
// working tree is not the commit its build SHA names, so it is skipped with a
// warning on warn.
func catalogueSweep(ctx context.Context, opts benchOpts, s *Scenario, r *Result, sessionID string, warn io.Writer) (string, error) {
	dirty, err := treeDirty(opts.repoRoot)
	if err != nil {
		return "", fmt.Errorf("checking working tree: %w", err)
	}
	if dirty {
		fmt.Fprintln(warn, "warning: catalogue entry skipped: working tree dirty")
		return "", nil
	}
	e, err := catalogEntryFromResult(s, r, catalogMeta{
		SessionID: sessionID,
		RunURL:    runURLFromEnv(),
		Now:       time.Now().UTC(),
	})
	if err != nil {
		return "", err
	}
	if err := putCatalogEntry(ctx, opts, e); err != nil {
		return "", err
	}
	return e.ID, nil
}
