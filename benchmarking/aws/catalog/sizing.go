// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
)

//go:embed sizing.json
var defaultSizingJSON []byte

// SizingConfig holds the constants used to turn measured capacity into CUs.
type SizingConfig struct {
	Headroom   float64 `json:"headroom"`
	CUsPerVCPU int     `json:"cus_per_vcpu"`
	BytesPerCU int64   `json:"bytes_per_cu"`
}

// LoadSizingConfig reads and validates a sizing config file.
func LoadSizingConfig(path string) (SizingConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return SizingConfig{}, fmt.Errorf("read sizing config: %w", err)
	}
	return parseSizingConfig(b)
}

// DefaultSizingConfig parses the sizing config embedded in the binary.
func DefaultSizingConfig() (SizingConfig, error) {
	return parseSizingConfig(defaultSizingJSON)
}

func parseSizingConfig(b []byte) (SizingConfig, error) {
	var cfg SizingConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return SizingConfig{}, fmt.Errorf("parse sizing config: %w", err)
	}
	if cfg.Headroom <= 0 || cfg.Headroom > 1 {
		return SizingConfig{}, fmt.Errorf("headroom: got %v, want 0 < headroom <= 1", cfg.Headroom)
	}
	if cfg.CUsPerVCPU <= 0 {
		return SizingConfig{}, fmt.Errorf("cus_per_vcpu: got %d, want > 0", cfg.CUsPerVCPU)
	}
	if cfg.BytesPerCU <= 0 {
		return SizingConfig{}, fmt.Errorf("bytes_per_cu: got %d, want > 0", cfg.BytesPerCU)
	}
	return cfg, nil
}

// CaveatSourceRateLimited marks a recommendation that rests on a point the
// bench producer, not the pipeline, capped (Point.LimitedBy): the measured
// capacity there is a lower bound.
const CaveatSourceRateLimited = "source_rate_limited"

// Recommendation is the CU sizing for a target throughput.
type Recommendation struct {
	CUs      int `json:"cus"`       // 0 when below_floor or exceeds_ceiling
	FloorCUs int `json:"floor_cus"` // CUs of the lowest measured point
	// VCPU is CUs expressed in vCPUs (CUs / cus_per_vcpu); 0 when CUs is 0.
	VCPU float64 `json:"vcpu"`
	// InterpolatedVCPU is the vCPU at which measured throughput meets the
	// target, before rounding up to whole CU steps; 0 when the target is
	// under the lowest point or over the ceiling.
	InterpolatedVCPU float64  `json:"interpolated_vcpu"`
	BoundBy          string   `json:"bound_by"`  // "throughput" | "memory"
	Estimated        bool     `json:"estimated"` // interpolated between points
	BelowFloor       bool     `json:"below_floor"`
	ExceedsCeiling   bool     `json:"exceeds_ceiling"`
	CeilingMBps      float64  `json:"ceiling_mb_s"`
	MemoryBound      string   `json:"memory_bound"` // "<n> CUs" | "not_measured"
	Headroom         float64  `json:"headroom"`
	Caveats          []string `json:"caveats"`
}

// relTol absorbs floating-point noise from dividing by the headroom, so a
// target that is exactly headroom*capacity lands on that measured point.
const relTol = 1e-9

func approxEq(a, b float64) bool {
	return math.Abs(a-b) <= relTol*math.Max(math.Abs(a), math.Abs(b))
}

// Recommend sizes CUs for targetMBps using the entry's measured points.
func Recommend(e Entry, targetMBps float64, cfg SizingConfig) (Recommendation, error) {
	if math.IsNaN(targetMBps) || math.IsInf(targetMBps, 0) || targetMBps <= 0 {
		return Recommendation{}, fmt.Errorf("target throughput must be positive and finite, got %v", targetMBps)
	}
	if len(e.Points) == 0 {
		return Recommendation{}, errors.New("entry has no points")
	}
	if cfg.Headroom <= 0 || cfg.CUsPerVCPU <= 0 || cfg.BytesPerCU <= 0 {
		return Recommendation{}, errors.New("invalid sizing config")
	}

	pts := append([]Point(nil), e.Points...)
	sort.Slice(pts, func(i, j int) bool { return pts[i].VCPU < pts[j].VCPU })

	rec := Recommendation{
		Headroom:    cfg.Headroom,
		FloorCUs:    pts[0].CUs,
		BoundBy:     "throughput",
		MemoryBound: "not_measured",
		Caveats:     []string{},
	}
	need := targetMBps / cfg.Headroom

	// memory records the memory bound for point p and returns it in CUs
	// (0 when memory was not measured).
	memory := func(p Point) int {
		if e.MemoryLimited && p.PeakRSSBytes > 0 {
			n := roundUp10(math.Ceil(float64(p.PeakRSSBytes) / float64(cfg.BytesPerCU)))
			rec.MemoryBound = fmt.Sprintf("%d CUs", n)
			return n
		}
		rec.Caveats = append(rec.Caveats, "memory_not_limited")
		return 0
	}

	if need <= pts[0].CapacityMBps || approxEq(need, pts[0].CapacityMBps) {
		// Throughput fits under the lowest point, but memory can still need
		// more than that point's CUs.
		if memCUs := memory(pts[0]); memCUs > rec.FloorCUs {
			rec.CUs = memCUs
			rec.BoundBy = "memory"
		} else {
			rec.BelowFloor = true
		}
		rec.setVCPU(cfg)
		rec.addLimitedCaveat(pts[0])
		rec.addEntryCaveats(e)
		return rec, nil
	}

	j := -1
	ceiling := 0.0
	ceilingPt := pts[0]
	for i, p := range pts {
		if p.CapacityMBps > ceiling {
			ceilingPt = p
		}
		ceiling = math.Max(ceiling, p.CapacityMBps)
		if j < 0 && (p.CapacityMBps >= need || approxEq(p.CapacityMBps, need)) {
			j = i
		}
	}
	if j < 0 {
		rec.ExceedsCeiling = true
		rec.CeilingMBps = ceiling
		rec.addLimitedCaveat(ceilingPt)
		rec.addEntryCaveats(e)
		return rec, nil
	}

	// j > 0 because need exceeds the first point's capacity.
	lo, hi := pts[j-1], pts[j]
	vcpu := hi.VCPU
	if !approxEq(need, hi.CapacityMBps) {
		vcpu = lo.VCPU + (need-lo.CapacityMBps)/(hi.CapacityMBps-lo.CapacityMBps)*(hi.VCPU-lo.VCPU)
		rec.Estimated = true
		rec.addLimitedCaveat(lo)
	}
	rec.addLimitedCaveat(hi)
	rec.InterpolatedVCPU = vcpu
	// The epsilon stops float noise such as 20.000000000000004 adding a CU step.
	cpuCUs := roundUp10(math.Ceil(vcpu*float64(cfg.CUsPerVCPU) - relTol))
	rec.CUs = cpuCUs

	if memCUs := memory(hi); memCUs > cpuCUs {
		rec.CUs = memCUs
		rec.BoundBy = "memory"
	}
	rec.setVCPU(cfg)
	rec.addEntryCaveats(e)
	return rec, nil
}

// setVCPU derives VCPU from the final CU count so the two always agree.
func (r *Recommendation) setVCPU(cfg SizingConfig) {
	r.VCPU = float64(r.CUs) / float64(cfg.CUsPerVCPU)
}

// addLimitedCaveat adds CaveatSourceRateLimited, once, when the
// recommendation rests on p and p was producer-limited.
func (r *Recommendation) addLimitedCaveat(p Point) {
	if p.LimitedBy != "" && !slices.Contains(r.Caveats, CaveatSourceRateLimited) {
		r.Caveats = append(r.Caveats, CaveatSourceRateLimited)
	}
}

func (r *Recommendation) addEntryCaveats(e Entry) {
	if !e.SinkMeasured {
		r.Caveats = append(r.Caveats, "sink_not_measured")
	}
	if e.ExternalStubbed {
		r.Caveats = append(r.Caveats, "external_calls_stubbed")
	}
}

// roundUp10 rounds a positive CU count up to the next multiple of 10.
func roundUp10(n float64) int {
	return int(math.Ceil(n/10-relTol)) * 10
}
