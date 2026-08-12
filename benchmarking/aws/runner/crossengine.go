// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// CrossEngineAnomaly flags a vCPU point where the two engines' median
// throughputs diverged by more than a configured ratio. The faster
// engine + the divergence ratio go into the bench markdown so the
// operator sees "at 4 vCPU, KC is 3.2x slower than Connect" at a glance.
type CrossEngineAnomaly struct {
	VCPU         int     `json:"vcpu"`
	FasterEngine string  `json:"faster_engine"`
	SlowerEngine string  `json:"slower_engine"`
	Ratio        float64 `json:"ratio"` // faster median / slower median
	FasterMBPerS float64 `json:"faster_mb_per_sec"`
	SlowerMBPerS float64 `json:"slower_mb_per_sec"`
}

// DetectCrossEngineAnomalies pairs Connect points with KC points at the
// same vCPU and flags any pair where the ratio of the larger median to
// the smaller exceeds `threshold` (e.g. 2.0 = 2x divergence). Orphan
// vCPUs (one engine has the point, the other doesn't) and zero-median
// points are skipped without flagging.
func DetectCrossEngineAnomalies(connect, kc []PointResult, threshold float64) []CrossEngineAnomaly {
	byVCPU := map[int]PointResult{}
	for _, p := range kc {
		byVCPU[p.VCPU] = p
	}
	var out []CrossEngineAnomaly
	for _, c := range connect {
		k, ok := byVCPU[c.VCPU]
		if !ok {
			continue
		}
		cMB := c.Summary.MedianMBPerSec
		kMB := k.Summary.MedianMBPerSec
		if cMB <= 0 || kMB <= 0 {
			continue
		}
		var ratio float64
		var faster, slower string
		var fasterMB, slowerMB float64
		if cMB >= kMB {
			ratio = cMB / kMB
			faster, slower = "connect", "kafka_connect"
			fasterMB, slowerMB = cMB, kMB
		} else {
			ratio = kMB / cMB
			faster, slower = "kafka_connect", "connect"
			fasterMB, slowerMB = kMB, cMB
		}
		if ratio >= threshold {
			out = append(out, CrossEngineAnomaly{
				VCPU:         c.VCPU,
				FasterEngine: faster,
				SlowerEngine: slower,
				Ratio:        ratio,
				FasterMBPerS: fasterMB,
				SlowerMBPerS: slowerMB,
			})
		}
	}
	return out
}
