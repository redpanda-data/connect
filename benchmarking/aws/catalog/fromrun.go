// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"errors"
	"fmt"
	"time"
)

// RunPoint is one measured sweep point of a bench run.
type RunPoint struct {
	VCPU                    int
	Engine, Arm, Binary     string
	MedianMBps, MedianMsgps float64
	RSSBytes                []uint64 // process RSS samples; the peak is kept
	LimitedBy               string   // see Point.LimitedBy
}

// BenchSourceOutput is the catalogued identity of a source-direction bench's
// output: the component only. The harness's benchmark processor and its
// per-session topic are measurement plumbing, not part of the workload a
// customer runs, so they are left out of the entry's processors and
// fingerprint.
func BenchSourceOutput() map[string]any {
	return map[string]any{"redpanda": map[string]any{}}
}

// BenchSourcePipeline builds the catalogued pipeline of a source-direction
// bench scenario from its pipeline section: the scenario's input, its
// pipeline.pipeline section (if any) and BenchSourceOutput. The runner and
// the importer both use it so their entries share one identity.
func BenchSourcePipeline(scenario map[string]any) map[string]any {
	p := map[string]any{"input": scenario["input"], "output": BenchSourceOutput()}
	if pp, ok := scenario["pipeline"]; ok {
		p["pipeline"] = pp
	}
	return p
}

// RunInput is a completed bench run in the shape EntryFromRun maps to an
// Entry. It is shared by the runner and the historical-results importer.
type RunInput struct {
	ID, Scenario, GitSHA, ConnectVersion, InstanceType, Region, RunURL, ResultKey string

	// Pipeline is {"input":..., "pipeline":{"processors":...}, "output":...}.
	Pipeline          map[string]any
	MessageSizeBytes  int
	OfferedRatePerSec int
	Soak              bool
	MemoryLimited     bool
	// SinkMeasured is true when throughput was measured end to end, with the
	// output writing to a real sink.
	SinkMeasured bool
	// AWSCostUSD is the run's AWS cost; nil when not recorded.
	AWSCostUSD *float64
	Points     []RunPoint
	Now        time.Time
}

// EntryFromRun maps a completed bench sweep to a catalogue Entry. Soak runs
// and arm/binary comparison runs are not catalogue material and are rejected,
// as are runs whose build or Connect version is not identifiable.
func EntryFromRun(r RunInput) (Entry, error) {
	if r.GitSHA == "" || r.GitSHA == "unknown" {
		return Entry{}, errors.New("build sha unknown")
	}
	if r.ConnectVersion == "" || r.ConnectVersion == "unknown" {
		return Entry{}, errors.New("connect version unknown")
	}
	if r.Soak {
		return Entry{}, errors.New("soak result")
	}
	if r.InstanceType == "" {
		return Entry{}, errors.New("runner instance type missing")
	}
	var points []Point
	for _, p := range r.Points {
		if p.Arm != "" || p.Binary != "" {
			return Entry{}, errors.New("arm/binary result")
		}
		if p.Engine != "" && p.Engine != "connect" {
			continue
		}
		var peak uint64
		for _, b := range p.RSSBytes {
			peak = max(peak, b)
		}
		points = append(points, Point{
			VCPU:          float64(p.VCPU),
			CUs:           p.VCPU * 10,
			CapacityMBps:  p.MedianMBps,
			CapacityMsgps: p.MedianMsgps,
			PeakRSSBytes:  peak,
			LimitedBy:     p.LimitedBy,
		})
	}
	input, processors, output, err := Components(r.Pipeline)
	if err != nil {
		return Entry{}, fmt.Errorf("components: %w", err)
	}
	fp, err := Fingerprint(r.Pipeline)
	if err != nil {
		return Entry{}, fmt.Errorf("fingerprint: %w", err)
	}
	e := Entry{
		SchemaVersion: SchemaVersion,
		ID:            r.ID,
		Scenario:      r.Scenario,
		Input:         input,
		Processors:    processors,
		Output:        output,
		Fingerprint:   fp,
		Workload: Workload{
			MessageSizeBytes:  r.MessageSizeBytes,
			Mode:              ModeFor(input),
			OfferedRatePerSec: r.OfferedRatePerSec,
		},
		ConnectVersion: r.ConnectVersion,
		BuildSHA:       r.GitSHA,
		Infra:          Infra{RunnerInstanceType: r.InstanceType, Region: r.Region},
		SinkMeasured:   r.SinkMeasured,
		MemoryLimited:  r.MemoryLimited,
		Points:         points,
		AWSCostUSD:     r.AWSCostUSD,
		RunURL:         r.RunURL,
		ResultKey:      r.ResultKey,
		RecordedAt:     r.Now,
	}
	if err := e.Validate(); err != nil {
		return Entry{}, fmt.Errorf("invalid entry: %w", err)
	}
	return e, nil
}
