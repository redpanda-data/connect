// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package plan turns a customer's Connect config and a workload into a
// stored Plan that the bench can run, with identity swapped for bench
// placeholders so no customer secret survives.
package plan

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// DefaultSinkSize is the Snowflake warehouse size used when none is given,
// and the size snowflake_streaming is always pinned to.
const DefaultSinkSize = "XSMALL"

// sizedOutput is the only output whose throughput depends on the Snowflake
// warehouse size. snowflake_streaming ingests through serverless Snowpipe
// Streaming, which the warehouse does not drive.
const sizedOutput = "snowflake_put"

// Workload is the load profile requested for a plan.
type Workload struct {
	MessageSizeBytes int     `json:"message_size_bytes"`
	TargetMBps       float64 `json:"target_mbps"`
	Mode             string  `json:"mode"` // "cdc" | "bulk"; derived from the input when empty
}

// SlackRef points at the Slack thread that requested a plan.
type SlackRef struct {
	Channel  string `json:"channel"`
	ThreadTS string `json:"thread_ts"`
}

// Plan is a runnable (or explicitly not runnable) bench plan.
type Plan struct {
	ID         string     `json:"plan_id"`
	CreatedAt  time.Time  `json:"created_at"`
	Runnable   bool       `json:"runnable"`
	Reason     string     `json:"reason,omitempty"`
	Input      string     `json:"input"`
	Processors []string   `json:"processors"`
	Output     string     `json:"output"`
	Source     SourceKind `json:"source"`
	Sink       SinkKind   `json:"sink"`
	SinkSize   string     `json:"sink_size,omitempty"`
	// SinkSizeAdjustable is true only for snowflake_put, the one output a
	// warehouse size changes; only then is a size worth offering.
	SinkSizeAdjustable bool           `json:"sink_size_adjustable"`
	Pipeline           map[string]any `json:"pipeline"`
	// Fingerprint is catalog.Fingerprint of the customer's ORIGINAL config
	// (input, pipeline, output), taken before any identity swap. It is a hash
	// only. Catalogue identity must use this, never Fingerprint(Plan.Pipeline),
	// which can never equal it.
	Fingerprint string   `json:"fingerprint"`
	Workload    Workload `json:"workload"`
	Caveats     []string `json:"caveats"`
	// Estimate is the sweep's expected cost and duration (catalog.EstimateRun
	// at SinkSize); zero for an unrunnable plan.
	Estimate        catalog.Estimate `json:"estimate"`
	Slack           SlackRef         `json:"slack"`
	DispatchedRunID int64            `json:"dispatched_run_id,omitempty"`
}

// BuildOption adjusts how Build makes a plan.
type BuildOption func(*buildOptions)

type buildOptions struct{ sinkSize string }

// WithSinkSize requests a Snowflake warehouse size. It applies only when the
// output is snowflake_put; for any other output it is ignored.
func WithSinkSize(size string) BuildOption {
	return func(o *buildOptions) { o.sinkSize = strings.ToUpper(strings.TrimSpace(size)) }
}

// Build classifies a customer pipeline config and produces a Plan. A config
// that cannot be run (CDC input with no bench stack) yields a Plan with
// Runnable false and an empty Pipeline; only a malformed config or an
// unknown requested sink size is an error.
func Build(config map[string]any, w Workload, slack SlackRef, now time.Time, opts ...BuildOption) (Plan, error) {
	var o buildOptions
	for _, opt := range opts {
		opt(&o)
	}
	input, processors, output, err := catalog.Components(config)
	if err != nil {
		return Plan{}, fmt.Errorf("reading pipeline components: %w", err)
	}
	orig := map[string]any{}
	for _, k := range []string{"input", "pipeline", "output"} {
		if v, ok := config[k]; ok {
			orig[k] = v
		}
	}
	fp, err := catalog.Fingerprint(orig)
	if err != nil {
		return Plan{}, fmt.Errorf("fingerprinting pipeline: %w", err)
	}
	id, err := newID()
	if err != nil {
		return Plan{}, err
	}
	if processors == nil {
		processors = []string{}
	}
	if w.Mode == "" {
		w.Mode = catalog.ModeFor(input)
	}
	p := Plan{
		ID:          id,
		CreatedAt:   now,
		Input:       input,
		Processors:  processors,
		Output:      output,
		Workload:    w,
		Caveats:     []string{},
		Slack:       slack,
		Pipeline:    map[string]any{},
		Fingerprint: fp,
	}

	inMap, _ := config["input"].(map[string]any)
	src, reason, ok, err := classifySource(inMap)
	if err != nil {
		return Plan{}, fmt.Errorf("classifying input: %w", err)
	}
	if !ok {
		p.Reason = reason
		return p, nil
	}
	sink := classifySink(output)
	p.Runnable, p.Source, p.Sink = true, src, sink
	if sink == SinkSnowflake {
		p.SinkSize = DefaultSinkSize
		if output == sizedOutput {
			p.SinkSizeAdjustable = true
			if o.sinkSize != "" {
				p.SinkSize = o.sinkSize
			}
		}
	}
	if p.Estimate, err = catalog.EstimateRun(stackFor(src), string(sink), p.SinkSize); err != nil {
		return Plan{}, fmt.Errorf("estimating run: %w", err)
	}

	pipeline, caveats, err := SwapIdentity(config, src, sink)
	if err != nil {
		return Plan{}, err
	}
	p.Pipeline = pipeline
	if src == SourceApproximated {
		caveats = append(caveats, CaveatSourceApproximated)
	}
	if sink == SinkDrop {
		caveats = append(caveats, CaveatSinkNotMeasured)
	}
	p.Caveats = normaliseCaveats(caveats)
	return p, nil
}

// stackFor is the bench stack a source kind runs on.
func stackFor(src SourceKind) string {
	switch src {
	case SourcePostgres:
		return "postgres"
	case SourceMySQL:
		return "mysql"
	default:
		return "none"
	}
}

func normaliseCaveats(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	out = slices.Compact(out)
	if out == nil {
		return []string{}
	}
	return out
}

func newID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating plan id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
