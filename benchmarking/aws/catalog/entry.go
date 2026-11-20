// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package catalog stores and queries benchmark results.
package catalog

import (
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the current Entry schema version.
const SchemaVersion = 1

// Entry is one benchmark session's result in the catalogue.
type Entry struct {
	SchemaVersion   int       `json:"schema_version"`
	ID              string    `json:"id"` // bench session ID
	Scenario        string    `json:"scenario"`
	Input           string    `json:"input"`
	Processors      []string  `json:"processors"`
	Output          string    `json:"output"`
	Fingerprint     string    `json:"fingerprint"`
	Workload        Workload  `json:"workload"`
	ConnectVersion  string    `json:"connect_version"` // git describe output
	BuildSHA        string    `json:"build_sha"`
	Infra           Infra     `json:"infra"`
	SinkMeasured    bool      `json:"sink_measured"`
	ExternalStubbed bool      `json:"external_calls_stubbed"`
	MemoryLimited   bool      `json:"memory_limited"` // hard per-point limit applied
	Points          []Point   `json:"points"`
	AWSCostUSD      *float64  `json:"aws_cost_usd,omitempty"`  // nil = not recorded
	SaaSCostUSD     *float64  `json:"saas_cost_usd,omitempty"` // nil = not recorded
	RunURL          string    `json:"run_url,omitempty"`
	ResultKey       string    `json:"result_key,omitempty"`
	RecordedAt      time.Time `json:"recorded_at"`
}

// Workload describes the load profile of a benchmark.
type Workload struct {
	MessageSizeBytes  int    `json:"message_size_bytes"`
	Mode              string `json:"mode"` // "cdc" | "bulk"
	OfferedRatePerSec int    `json:"offered_rate_per_sec"`
}

// Infra describes the infrastructure a benchmark ran on.
type Infra struct {
	RunnerInstanceType string `json:"runner_instance_type"`
	Region             string `json:"region"`
	SinkSize           string `json:"sink_size,omitempty"`
}

// Point is the measured capacity at one vCPU allocation.
type Point struct {
	VCPU          float64 `json:"vcpu"` // float: 0.5 is allowed
	CUs           int     `json:"cus"`  // VCPU*10
	CapacityMBps  float64 `json:"capacity_mb_s"`
	CapacityMsgps float64 `json:"capacity_msg_s"`
	PeakRSSBytes  uint64  `json:"peak_rss_bytes"` // 0 = not measured
}

// Validate reports the first schema violation in the entry, naming the field.
func (e Entry) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version: got %d, want %d", e.SchemaVersion, SchemaVersion)
	}
	for _, f := range []struct{ name, val string }{
		{"id", e.ID},
		{"input", e.Input},
		{"output", e.Output},
		{"fingerprint", e.Fingerprint},
		{"build_sha", e.BuildSHA},
	} {
		if f.val == "" {
			return fmt.Errorf("%s: must not be empty", f.name)
		}
	}
	if m := e.Workload.Mode; m != "cdc" && m != "bulk" {
		return fmt.Errorf("workload.mode: got %q, want \"cdc\" or \"bulk\"", m)
	}
	if len(e.Points) == 0 {
		return fmt.Errorf("points: must not be empty")
	}
	seen := make(map[float64]struct{}, len(e.Points))
	for i, p := range e.Points {
		if want := int(p.VCPU * 10); p.CUs != want {
			return fmt.Errorf("points[%d].cus: got %d, want %d (vcpu*10)", i, p.CUs, want)
		}
		if _, dup := seen[p.VCPU]; dup {
			return fmt.Errorf("points[%d].vcpu: duplicate value %v", i, p.VCPU)
		}
		seen[p.VCPU] = struct{}{}
	}
	return nil
}

// ModeFor returns the workload mode implied by an input component name.
func ModeFor(input string) string {
	if strings.HasSuffix(input, "_cdc") {
		return "cdc"
	}
	return "bulk"
}
