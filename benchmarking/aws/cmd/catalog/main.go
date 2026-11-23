// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Command catalog queries and populates the benchmark results catalogue. Its
// JSON output and exit codes are the contract with the Slack agent that calls
// it: 0 on success (including empty results), 1 on a runtime failure (a JSON
// {"error":...} body on stdout), 2 on bad usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/benchmarking/aws/catalog"
)

const (
	defaultStore = "s3://redpanda-connect-bench-catalog"
	maxMatches   = 5
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// errUsage marks a bad-usage error (exit 2).
var errUsage = errors.New("usage")

func usageErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, a...))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: catalog <query|recommend|import> [flags]")
		return 2
	}
	sub, rest := args[0], args[1:]
	var fn func(context.Context, []string, io.Writer, io.Writer) error
	switch sub {
	case "query":
		fn = runQuery
	case "recommend":
		fn = runRecommend
	case "import":
		fn = runImport
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q; want query, recommend or import\n", sub)
		return 2
	}
	err := fn(context.Background(), rest, stdout, stderr)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, err)
		return 2
	default:
		writeJSON(stdout, map[string]string{"error": err.Error()})
		return 1
	}
}

func writeJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// newFlagSet returns a FlagSet that reports errors to stderr and lets run
// choose the exit code.
func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return fmt.Errorf("%w: %v", errUsage, err)
	}
	if fs.NArg() > 0 {
		return usageErr("unexpected arguments %v", fs.Args())
	}
	return nil
}

// openStore resolves a --store value: "s3://<bucket>" or "dir:<path>".
func openStore(ctx context.Context, spec string) (catalog.Store, error) {
	switch {
	case strings.HasPrefix(spec, "dir:"):
		return catalog.NewDirStore(strings.TrimPrefix(spec, "dir:")), nil
	case strings.HasPrefix(spec, "s3://"):
		bucket := strings.TrimPrefix(spec, "s3://")
		if bucket == "" {
			return nil, usageErr("--store %q: empty bucket", spec)
		}
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		return catalog.NewS3Store(s3.NewFromConfig(cfg), bucket), nil
	default:
		return nil, usageErr("--store %q: want s3://<bucket> or dir:<path>", spec)
	}
}

// stalenessFor never fails: when git cannot answer, staleness is "unknown"
// with the error as its reason.
func stalenessFor(ctx context.Context, g catalog.Git, e catalog.Entry) catalog.Staleness {
	s, err := catalog.CheckStaleness(ctx, g, e)
	if err != nil {
		return catalog.Staleness{State: "unknown", Reasons: []string{err.Error()}}
	}
	return s
}

type matchOut struct {
	Kind        catalog.MatchKind `json:"kind"`
	Score       float64           `json:"score"`
	Differences []string          `json:"differences"`
	Entry       catalog.Entry     `json:"entry"`
	Staleness   catalog.Staleness `json:"staleness"`
}

func runQuery(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("query", stderr)
	store := fs.String("store", defaultStore, "catalogue store: s3://<bucket> or dir:<path>")
	repo := fs.String("repo", ".", "git checkout used for staleness")
	input := fs.String("input", "", "input component name")
	output := fs.String("output", "", "output component name")
	procs := fs.String("processors", "", "comma-separated processor names")
	cfgPath := fs.String("config", "", "pipeline config file; overrides --input, --output and --processors")
	size := fs.Int("message-size", 0, "message size in bytes")
	mode := fs.String("mode", "", "workload mode: cdc or bulk (default: derived from the input)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *mode != "" && *mode != "cdc" && *mode != "bulk" {
		return usageErr("--mode %q: want cdc or bulk", *mode)
	}

	q := catalog.Query{Input: *input, Output: *output, MessageSizeBytes: *size, Mode: *mode}
	for p := range strings.SplitSeq(*procs, ",") {
		if p = strings.TrimSpace(p); p != "" {
			q.Processors = append(q.Processors, p)
		}
	}
	if *cfgPath != "" {
		raw, err := os.ReadFile(*cfgPath)
		if err != nil {
			return fmt.Errorf("reading --config: %w", err)
		}
		var pipeline map[string]any
		if err := yaml.Unmarshal(raw, &pipeline); err != nil {
			return fmt.Errorf("parsing --config: %w", err)
		}
		id, err := catalog.ConfigIdentity(pipeline)
		if err != nil {
			return fmt.Errorf("--config: %w", err)
		}
		q.Input, q.Processors, q.Output, q.Fingerprint = id.Input, id.Processors, id.Output, id.Fingerprint
	}
	if q.Input == "" || q.Output == "" {
		return usageErr("query needs --input and --output, or --config")
	}
	if q.Mode == "" {
		q.Mode = catalog.ModeFor(q.Input)
	}

	st, err := openStore(ctx, *store)
	if err != nil {
		return err
	}
	entries, err := st.List(ctx, q.Input)
	if err != nil {
		return fmt.Errorf("listing catalogue: %w", err)
	}
	matches := catalog.FindMatches(entries, q)
	if len(matches) > maxMatches {
		matches = matches[:maxMatches]
	}
	git := catalog.NewGitCLI(*repo)
	out := struct {
		Matches []matchOut `json:"matches"`
	}{Matches: []matchOut{}}
	for _, m := range matches {
		diffs := m.Differences
		if diffs == nil {
			diffs = []string{}
		}
		out.Matches = append(out.Matches, matchOut{
			Kind: m.Kind, Score: m.Score, Differences: diffs, Entry: m.Entry,
			Staleness: stalenessFor(ctx, git, m.Entry),
		})
	}
	writeJSON(stdout, out)
	return nil
}

func runRecommend(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("recommend", stderr)
	store := fs.String("store", defaultStore, "catalogue store: s3://<bucket> or dir:<path>")
	repo := fs.String("repo", ".", "git checkout used for staleness")
	id := fs.String("entry-id", "", "catalogue entry ID")
	input := fs.String("input", "", "input component of the entry (optional; narrows the listing)")
	target := fs.Float64("target-mb-s", 0, "target throughput in MB/s")
	sizing := fs.String("sizing", "", "sizing config JSON (default: built-in)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *id == "" {
		return usageErr("recommend needs --entry-id")
	}
	if *target <= 0 {
		return usageErr("recommend needs --target-mb-s > 0")
	}

	cfg, err := catalog.DefaultSizingConfig()
	if *sizing != "" {
		cfg, err = catalog.LoadSizingConfig(*sizing)
	}
	if err != nil {
		return err
	}
	st, err := openStore(ctx, *store)
	if err != nil {
		return err
	}
	entries, err := st.List(ctx, *input)
	if err != nil {
		return fmt.Errorf("listing catalogue: %w", err)
	}
	for _, e := range entries {
		if e.ID != *id {
			continue
		}
		rec, err := catalog.Recommend(e, *target, cfg)
		if err != nil {
			return err
		}
		writeJSON(stdout, map[string]any{
			"entry_id":        e.ID,
			"connect_version": e.ConnectVersion,
			"base_release":    e.BaseRelease(),
			"build_sha":       e.BuildSHA,
			"instance_type":   e.Infra.RunnerInstanceType,
			"sink_size":       e.Infra.SinkSize,
			"recommendation":  rec,
			"staleness":       stalenessFor(ctx, catalog.NewGitCLI(*repo), e),
			"points":          e.Points,
		})
		return nil
	}
	return errors.New("entry not found")
}
