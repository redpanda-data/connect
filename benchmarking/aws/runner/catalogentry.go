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
	"os"
	"os/exec"
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
// catalog.RunInput and maps it to an Entry. The pipeline is the catalogued
// identity (catalog.BenchSourcePipeline), not the rendered harness config.
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
		Pipeline:          catalog.BenchSourcePipeline(s.Pipeline),
		MessageSizeBytes:  r.Dataset.RowSizeBytes,
		OfferedRatePerSec: s.Workload.WriteRatePerSec,
		Soak:              s.Soak,
		MemoryLimited:     s.MemoryLimited(),
		SinkMeasured:      true,
		Now:               meta.Now,
	}
	for _, p := range r.Points {
		rp := catalog.RunPoint{
			VCPU:        p.VCPU,
			Engine:      p.Engine,
			Arm:         p.Arm,
			Binary:      p.Binary,
			MedianMBps:  p.Summary.MedianMBPerSec,
			MedianMsgps: p.Summary.MedianMsgPerSec,
		}
		for _, a := range p.Anomalies {
			if a.Kind == anomalyProducerBound {
				rp.LimitedBy = "producer"
			}
		}
		for _, pp := range p.Prom {
			rp.RSSBytes = append(rp.RSSBytes, pp.RSSBytes)
		}
		run.Points = append(run.Points, rp)
	}
	return catalog.EntryFromRun(run)
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

// catalogueSweep writes a completed sweep's entry to the catalogue bucket. A
// build from a dirty working tree is not the commit its build SHA names, so
// it is skipped with a warning on warn.
func catalogueSweep(ctx context.Context, opts benchOpts, s *Scenario, r *Result, sessionID string, warn io.Writer) error {
	dirty, err := treeDirty(opts.repoRoot)
	if err != nil {
		return fmt.Errorf("checking working tree: %w", err)
	}
	if dirty {
		fmt.Fprintln(warn, "warning: catalogue entry skipped: working tree dirty")
		return nil
	}
	e, err := catalogEntryFromResult(s, r, catalogMeta{
		SessionID: sessionID,
		RunURL:    runURLFromEnv(),
		Now:       time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	return catalog.NewS3Store(s3.NewFromConfig(cfg), opts.catalogBucket).Put(ctx, e)
}
