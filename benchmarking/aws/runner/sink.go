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
	case "iceberg", "snowflake", "bigquery":
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
	return sinkFor(*s.Sink, deps)
}

// provisionSink provisions sink and merges its placeholders into outs. The
// returned teardown is for a defer over the run's named error: it removes
// the scratch, logs a teardown failure, and only reports that failure when
// nothing else failed. A failed Provision is torn down before returning,
// since it may have created part of the scratch. A nil sink is a no-op.
func provisionSink(ctx context.Context, sink Sink, session string, outs map[string]string) (teardown func(errOut *error), err error) {
	if sink == nil {
		return func(*error) {}, nil
	}
	teardown = func(errOut *error) {
		// The run's context may be cancelled already (Ctrl-C); scratch
		// must still go.
		terr := sink.Teardown(context.WithoutCancel(ctx), session)
		if terr == nil {
			return
		}
		fmt.Fprintf(os.Stderr, "sink teardown FAILED, scratch may remain until the reaper sweeps it: %v\n", terr)
		if *errOut == nil {
			*errOut = fmt.Errorf("sink teardown: %w", terr)
		}
	}
	ph, err := sink.Provision(ctx, session)
	if err != nil {
		perr := fmt.Errorf("sink provision: %w", err)
		teardown(&perr)
		return nil, perr
	}
	for k, v := range ph {
		outs[k] = v
	}
	return teardown, nil
}

const (
	anomalySinkCountMismatch = "sink_count_mismatch"
	// sinkCountTolerance is the largest relative disagreement between the
	// sink's count and the output-side samples that still passes.
	sinkCountTolerance = 0.02
)

// sinkCountAnomaly compares what the sink gained over a point with the sum
// of the point's per-second msg/sec samples. samples must be every sample
// of the point, warmup included, because the sink delta spans the whole
// point.
func sinkCountAnomaly(written int64, samples []Sample) (Anomaly, bool) {
	var measured float64
	for _, s := range samples {
		measured += s.MsgPerSec // each sample covers one second
	}
	diff := math.Abs(float64(written) - measured)
	if diff == 0 || (measured > 0 && diff/measured <= sinkCountTolerance) {
		return Anomaly{}, false
	}
	return Anomaly{
		Kind: anomalySinkCountMismatch,
		Note: fmt.Sprintf("%s: the sink gained %d records over the point but the output samples sum to %.0f messages; they disagree by more than %.0f%%",
			anomalySinkCountMismatch, written, measured, sinkCountTolerance*100),
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
