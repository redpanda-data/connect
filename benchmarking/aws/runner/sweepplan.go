// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strconv"
)

// sweepPoint is one measured leg of the sweep: a vCPU pin plus the launch
// topology to measure at it. A scenario without matrix.arms produces exactly
// one point per cpu_points entry with GOMAXPROCS == VCPU and a single stream,
// which is byte-for-byte the pre-arms behaviour.
type sweepPoint struct {
	VCPU int
	// ArmID is "" for arm-less scenarios. Non-empty ids appear in artifact
	// names via Key.
	ArmID      string
	GOMAXPROCS int
	Streams    int
	// FanIn mirrors Arm.FanIn: renderPointConfigs renders this point as one
	// pipeline subscribed to all of dataset.topics' topics instead of the
	// per-Streams rendering below. false for every arm-less scenario and for
	// arms that don't set fan_in.
	FanIn bool
	// Pipeline is the scenario pipeline with this arm's overrides merged in.
	// nil for arm-less scenarios, whose callers use Scenario.Pipeline directly.
	Pipeline map[string]any
	// Binary mirrors Arm.Binary: the logical binary name this point launches,
	// or "" for the scenario's single default staged binary (every
	// arm-less point, and every arm that doesn't set one). See
	// MatrixRunner.binaryPathFor for how this resolves to a runner-host path.
	Binary string
}

// Key identifies the point in log filenames and S3 keys. Arm-less points key
// off the bare vCPU count so existing scenarios keep their artifact paths.
func (p sweepPoint) Key() string {
	if p.ArmID == "" {
		return strconv.Itoa(p.VCPU)
	}
	return fmt.Sprintf("%d-%s", p.VCPU, p.ArmID)
}

// buildSweepPlan expands matrix.cpu_points × matrix.arms into the ordered list
// of points to measure. Arms are validated to a single cpu_points entry
// (Scenario.Validate), so the nested loop yields len(arms) points in practice.
func buildSweepPlan(s *Scenario) []sweepPoint {
	if len(s.Matrix.Arms) == 0 {
		pts := make([]sweepPoint, 0, len(s.Matrix.CPUPoints))
		for _, n := range s.Matrix.CPUPoints {
			pts = append(pts, sweepPoint{VCPU: n, GOMAXPROCS: n, Streams: 1})
		}
		return pts
	}
	pts := make([]sweepPoint, 0, len(s.Matrix.CPUPoints)*len(s.Matrix.Arms))
	for _, n := range s.Matrix.CPUPoints {
		for _, a := range s.Matrix.Arms {
			gmp := a.GOMAXPROCS
			if gmp == 0 {
				gmp = n
			}
			streams := a.Streams
			if streams == 0 {
				streams = 1
			}
			pts = append(pts, sweepPoint{
				VCPU:       n,
				ArmID:      a.ID,
				GOMAXPROCS: gmp,
				Streams:    streams,
				FanIn:      a.FanIn,
				Pipeline:   mergePipeline(s.Pipeline, a.Pipeline),
				Binary:     a.Binary,
			})
		}
	}
	return pts
}

// planMaxStreams is the largest stream count in the plan. The between-points
// reset pre-creates the union of every arm's tables (see
// BenchNames.IcebergResetTables), which lets one precomputed reset script serve
// every arm regardless of its own stream count.
func planMaxStreams(plan []sweepPoint) int {
	max := 1
	for _, p := range plan {
		if p.Streams > max {
			max = p.Streams
		}
	}
	return max
}

// mergePipeline returns a deep copy of base with override's keys merged in
// recursively. Nested maps merge key-by-key; every other value (scalars,
// sequences) is replaced wholesale. Neither argument is mutated and the result
// shares no sub-maps with either, so sibling arms can be edited independently.
func mergePipeline(base, override map[string]any) map[string]any {
	out, _ := deepCopyValue(base).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range override {
		if vm, ok := v.(map[string]any); ok {
			if bm, ok := out[k].(map[string]any); ok {
				out[k] = mergePipeline(bm, vm)
				continue
			}
		}
		out[k] = deepCopyValue(v)
	}
	return out
}

// deepCopyValue copies the map/slice spine of a yaml.v3-decoded value. Scalars
// are returned as-is (they are immutable in practice).
func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = deepCopyValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = deepCopyValue(vv)
		}
		return out
	default:
		return v
	}
}
