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
	"time"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// DefaultSinkSize is the Snowflake warehouse size used when none is given.
const DefaultSinkSize = "XSMALL"

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
	ID              string         `json:"plan_id"`
	CreatedAt       time.Time      `json:"created_at"`
	Runnable        bool           `json:"runnable"`
	Reason          string         `json:"reason,omitempty"`
	Input           string         `json:"input"`
	Processors      []string       `json:"processors"`
	Output          string         `json:"output"`
	Source          SourceKind     `json:"source"`
	Sink            SinkKind       `json:"sink"`
	SinkSize        string         `json:"sink_size,omitempty"`
	Pipeline        map[string]any `json:"pipeline"`
	Workload        Workload       `json:"workload"`
	Caveats         []string       `json:"caveats"`
	Slack           SlackRef       `json:"slack"`
	DispatchedRunID int64          `json:"dispatched_run_id,omitempty"`
}

// Build classifies a customer pipeline config and produces a Plan. A config
// that cannot be run (CDC input with no bench stack) yields a Plan with
// Runnable false and an empty Pipeline; only a malformed config is an error.
func Build(config map[string]any, w Workload, slack SlackRef, now time.Time) (Plan, error) {
	input, processors, output, err := catalog.Components(config)
	if err != nil {
		return Plan{}, fmt.Errorf("reading pipeline components: %w", err)
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
		ID:         id,
		CreatedAt:  now,
		Input:      input,
		Processors: processors,
		Output:     output,
		Workload:   w,
		Caveats:    []string{},
		Slack:      slack,
		Pipeline:   map[string]any{},
	}

	src, reason, ok := classifySource(input)
	if !ok {
		p.Reason = reason
		return p, nil
	}
	sink := classifySink(output)
	p.Runnable, p.Source, p.Sink = true, src, sink
	if sink == SinkSnowflake {
		p.SinkSize = DefaultSinkSize
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
