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
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
)

// An e2e scenario writes to the customer's real sink. The runner provisions
// per-run scratch in it before the sweep (a topic, a prefix, a table), fills
// the output's ${BENCH_*} placeholders from what it provisioned, counts what
// each point wrote as a ground-truth check on the output-side samples, and
// tears the scratch down afterwards, even when the sweep fails.

// Sink is one e2e sink kind's scratch lifecycle.
type Sink interface {
	// Provision creates the run's scratch and returns the values for the
	// output's ${BENCH_*} placeholders, keyed by placeholder name.
	Provision(ctx context.Context, session string) (placeholders map[string]string, err error)
	// Count is the number of records in the scratch now. The orchestrator
	// calls it before and after each point and checks the delta.
	// ErrCountUnsupported when the sink cannot count.
	Count(ctx context.Context, point string) (int64, error)
	// Teardown removes the run's scratch. It is idempotent.
	Teardown(ctx context.Context, session string) error
}

// ErrCountUnsupported is Count's answer for a sink that cannot count its
// records; the ground-truth check is skipped for it.
var ErrCountUnsupported = errors.New("sink cannot count records")

// SinkDeps is what a sink needs from the run.
type SinkDeps struct {
	Region    string
	Outs      map[string]string // terraform outputs
	SessionID string
	RPK       RPKRunner
	// Glue is the orchestrator's Glue client, for the iceberg sink.
	Glue GlueAPI
	// Snowflake is the orchestrator's connection to the bench Snowflake
	// account and SnowflakeTarget its non-secret identity, for the
	// snowflake sink.
	Snowflake       SnowflakeExec
	SnowflakeTarget SnowflakeTarget
	// Output is the scenario's output config, for sinks whose scratch
	// depends on it (the snowflake sink reads schema_evolution).
	Output any
}

// RPKRunner runs rpk against the brokers and returns its combined output
// lines. Lines are returned on failure too, so callers can read the reason.
type RPKRunner interface {
	RunRPK(ctx context.Context, brokers string, args ...string) ([]string, error)
}

// rpkScript runs one rpk command with its output on stdout. The first
// sink call can reach a freshly booted runner whose cloud-init has not yet
// installed rpk, so it waits up to 10 minutes for the binary.
const rpkScript = `for _ in $(seq 120); do [ -x /usr/local/bin/rpk ] && break; sleep 5; done
/usr/local/bin/rpk %s -X brokers=%q 2>&1
`

// ssmRPK runs rpk on the runner host over SSM. The orchestrator cannot reach
// the brokers itself; the runner host has rpk and is in their VPC.
type ssmRPK struct {
	SSM      SSMExecutor
	Instance string
}

func (r ssmRPK) RunRPK(ctx context.Context, brokers string, args ...string) ([]string, error) {
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = fmt.Sprintf("%q", a)
	}
	script := fmt.Sprintf(rpkScript, strings.Join(quoted, " "), brokers)
	var lines []string
	err := r.SSM.Run(ctx, r.Instance, script, func(l string) { lines = append(lines, l) })
	return lines, err
}

// sinkFor builds the sink an e2e scenario names.
func sinkFor(spec SinkSpec, deps SinkDeps) (Sink, error) {
	switch spec.Kind {
	case "redpanda":
		return newRedpandaSink(deps)
	case "s3":
		return newS3Sink(deps)
	case "drop":
		return dropSink{}, nil
	case "iceberg":
		return newIcebergSink(deps)
	case "snowflake":
		return newSnowflakeSink(spec, deps)
	case "bigquery":
		return nil, fmt.Errorf("sink %s not implemented yet", spec.Kind)
	default:
		return nil, fmt.Errorf("unknown sink kind %q", spec.Kind)
	}
}

// sinkForScenario is an e2e scenario's sink, or nil for every other
// direction: source runs (soaks included) never touch a sink.
func sinkForScenario(s *Scenario, deps SinkDeps) (Sink, error) {
	if s.Direction != DirectionE2E {
		return nil, nil
	}
	if s.Sink == nil {
		return nil, errors.New("direction e2e requires sink.kind")
	}
	deps.Output = s.Pipeline["output"]
	return sinkFor(*s.Sink, deps)
}

// ErrNothingProvisioned marks a Provision error after which no scratch
// exists (it failed before creating anything), so there is nothing to tear
// down. Any other Provision error is treated as a partial provision.
var ErrNothingProvisioned = errors.New("nothing provisioned")

// withSink runs fn inside sink's scratch lifecycle: it provisions, hands fn
// the placeholders, and always tears down once anything may have been
// created, including after fn fails or the run's context is cancelled
// (teardown runs on context.WithoutCancel, so a Ctrl-C still removes the
// scratch). fn's error comes first; a teardown error is returned only when
// fn succeeded, and is logged either way. A nil sink just calls fn.
func withSink(ctx context.Context, sink Sink, session string, fn func(placeholders map[string]string) error) error {
	if sink == nil {
		return fn(nil)
	}
	teardown := func() error {
		terr := sink.Teardown(context.WithoutCancel(ctx), session)
		if terr != nil {
			fmt.Fprintf(os.Stderr, "sink teardown FAILED, scratch may remain until the reaper sweeps it: %v\n", terr)
			return fmt.Errorf("sink teardown: %w", terr)
		}
		return nil
	}
	ph, err := sink.Provision(ctx, session)
	if err != nil {
		if !errors.Is(err, ErrNothingProvisioned) {
			_ = teardown() // a partial provision; teardown logs its own failure
		}
		return fmt.Errorf("sink provision: %w", err)
	}
	ferr := fn(ph)
	terr := teardown()
	if ferr != nil {
		return ferr
	}
	return terr
}

// uncountedSink hides a sink's Count, so the ground-truth check is skipped
// while provisioning and teardown still run.
type uncountedSink struct{ Sink }

func (uncountedSink) Count(context.Context, string) (int64, error) { return 0, ErrCountUnsupported }

// countableSink returns sink, or sink with Count hidden when any point's
// output declares batching processors. Those run on the batch just before
// the write, and an archive collapses a batch into one record while a
// split or unarchive fans one message out into many, so the sink's record
// count stops tracking the message count the benchmark processor (which
// runs before batching) measures. The check would then fail for a correct
// run, so it is skipped as for a sink that cannot count.
func countableSink(sink Sink, s *Scenario, plan []sweepPoint) Sink {
	if sink == nil {
		return nil
	}
	fanOut := outputBatchesWithProcessors(s.Pipeline["output"])
	for _, pt := range plan {
		if pt.Pipeline != nil && outputBatchesWithProcessors(pt.Pipeline["output"]) {
			fanOut = true
		}
	}
	if fanOut {
		return uncountedSink{sink}
	}
	return sink
}

// outputBatchesWithProcessors reports whether any batching block anywhere in
// an output config (including outputs nested in a broker, switch or
// fallback) carries a processors key.
func outputBatchesWithProcessors(output any) bool {
	switch v := output.(type) {
	case map[string]any:
		for k, c := range v {
			if k == "batching" {
				if b, ok := c.(map[string]any); ok {
					if _, has := b["processors"]; has {
						return true
					}
				}
			}
			if outputBatchesWithProcessors(c) {
				return true
			}
		}
	case []any:
		for _, c := range v {
			if outputBatchesWithProcessors(c) {
				return true
			}
		}
	}
	return false
}

const (
	anomalySinkCountMismatch = "sink_count_mismatch"
	// The sink's count and the output-side samples may disagree by up to
	// max(sinkCountTolerance of the samples' sum, sinkCountFloorSec of the
	// point's median rate). The floor covers the edges a short point cannot
	// amortise: the partial rolling-stats interval lost at SIGTERM and the
	// interval in flight at launch.
	sinkCountTolerance = 0.02
	sinkCountFloorSec  = 3
)

// sinkCountAnomaly compares what the sink gained over a point with the sum
// of the point's per-second msg/sec samples. samples must be every sample
// of the point, warmup included, because the sink delta spans the whole
// point. medianMsgPerSec is the point's steady-state median, for the floor.
func sinkCountAnomaly(written int64, samples []Sample, medianMsgPerSec float64) (Anomaly, bool) {
	var measured float64
	for _, s := range samples {
		measured += s.MsgPerSec // each sample covers one second
	}
	diff := math.Abs(float64(written) - measured)
	tolerance := max(sinkCountTolerance*measured, sinkCountFloorSec*medianMsgPerSec)
	if diff == 0 || diff <= tolerance {
		return Anomaly{}, false
	}
	return Anomaly{
		Kind: anomalySinkCountMismatch,
		Note: fmt.Sprintf("%s: the sink gained %d records over the point but the output samples sum to %.0f messages; they differ by %.0f, over the %.0f tolerance (the larger of %.0f%% and %ds of the median rate)",
			anomalySinkCountMismatch, written, measured, diff, tolerance, sinkCountTolerance*100, sinkCountFloorSec),
	}, true
}

var benchPlaceholderRe = regexp.MustCompile(`\$\{BENCH_[A-Z0-9_]*\}`)

// checkPlaceholdersResolved fails when a rendered config still holds a
// ${BENCH_*} placeholder, so an e2e run with a sink value nothing provided
// stops before the sweep instead of launching Connect against a literal
// placeholder.
func checkPlaceholdersResolved(sets []renderedPointConfigs) error {
	for _, set := range sets {
		paths := append([]string{set.Single, set.Root}, set.Streams...)
		for _, p := range paths {
			if p == "" {
				continue
			}
			raw, err := os.ReadFile(p)
			if err != nil {
				return fmt.Errorf("read rendered config for point %q: %w", set.Key, err)
			}
			if m := benchPlaceholderRe.FindAll(raw, -1); len(m) > 0 {
				seen := map[string]bool{}
				var names []string
				for _, b := range m {
					if s := string(b); !seen[s] {
						seen[s] = true
						names = append(names, s)
					}
				}
				return fmt.Errorf("rendered config for point %q has unresolved placeholders %s", set.Key, strings.Join(names, ", "))
			}
		}
	}
	return nil
}
