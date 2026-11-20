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

// Recommendation is the CU sizing for a target throughput.
type Recommendation struct {
	CUs            int      `json:"cus"` // 0 when below_floor or exceeds_ceiling
	VCPU           float64  `json:"vcpu"`
	BoundBy        string   `json:"bound_by"`  // "throughput" | "memory"
	Estimated      bool     `json:"estimated"` // interpolated between points
	BelowFloor     bool     `json:"below_floor"`
	ExceedsCeiling bool     `json:"exceeds_ceiling"`
	CeilingMBps    float64  `json:"ceiling_mb_s"`
	MemoryBound    string   `json:"memory_bound"` // "<n> CUs" | "not_measured"
	Headroom       float64  `json:"headroom"`
	Caveats        []string `json:"caveats"`
}

// Recommend sizes CUs for targetMBps using the entry's measured points.
func Recommend(e Entry, targetMBps float64, cfg SizingConfig) (Recommendation, error) {
	if targetMBps <= 0 {
		return Recommendation{}, errors.New("target throughput must be positive")
	}
	if len(e.Points) == 0 {
		return Recommendation{}, errors.New("entry has no points")
	}
	if cfg.Headroom <= 0 || cfg.CUsPerVCPU <= 0 || cfg.BytesPerCU <= 0 {
		return Recommendation{}, errors.New("invalid sizing config")
	}

	pts := append([]Point(nil), e.Points...)
	sort.Slice(pts, func(i, j int) bool { return pts[i].VCPU < pts[j].VCPU })

	rec := Recommendation{Headroom: cfg.Headroom, BoundBy: "throughput", MemoryBound: "not_measured"}
	need := targetMBps / cfg.Headroom

	if need <= pts[0].CapacityMBps {
		rec.BelowFloor = true
		rec.addEntryCaveats(e)
		return rec, nil
	}

	j := -1
	ceiling := 0.0
	for i, p := range pts {
		ceiling = math.Max(ceiling, p.CapacityMBps)
		if j < 0 && p.CapacityMBps >= need {
			j = i
		}
	}
	if j < 0 {
		rec.ExceedsCeiling = true
		rec.CeilingMBps = ceiling
		rec.addEntryCaveats(e)
		return rec, nil
	}

	// j > 0 because need exceeds the first point's capacity.
	lo, hi := pts[j-1], pts[j]
	vcpu := hi.VCPU
	if lo.CapacityMBps < hi.CapacityMBps {
		vcpu = lo.VCPU + (need-lo.CapacityMBps)/(hi.CapacityMBps-lo.CapacityMBps)*(hi.VCPU-lo.VCPU)
		rec.Estimated = need != hi.CapacityMBps
	}
	rec.VCPU = vcpu
	cpuCUs := roundUp10(math.Ceil(vcpu * float64(cfg.CUsPerVCPU)))
	rec.CUs = cpuCUs

	if e.MemoryLimited && hi.PeakRSSBytes > 0 {
		memCUs := roundUp10(math.Ceil(float64(hi.PeakRSSBytes) / float64(cfg.BytesPerCU)))
		rec.MemoryBound = fmt.Sprintf("%d CUs", memCUs)
		if memCUs > cpuCUs {
			rec.CUs = memCUs
			rec.BoundBy = "memory"
		}
	} else {
		rec.Caveats = append(rec.Caveats, "memory_not_limited")
	}
	rec.addEntryCaveats(e)
	return rec, nil
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
	return int(math.Ceil(n/10)) * 10
}
