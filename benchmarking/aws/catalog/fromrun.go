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
	AWSCostUSD        float64
	Points            []RunPoint
	Now               time.Time
}

// EntryFromRun maps a completed bench sweep to a catalogue Entry. Soak runs
// and arm/binary comparison runs are not catalogue material and are rejected,
// as are runs whose build is not identifiable.
func EntryFromRun(r RunInput) (Entry, error) {
	if r.GitSHA == "" || r.GitSHA == "unknown" {
		return Entry{}, errors.New("build sha unknown")
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
		SinkMeasured:   true,
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
