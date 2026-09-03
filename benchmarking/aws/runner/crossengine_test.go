// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestDetectCrossEngineAnomalies_BelowThreshold(t *testing.T) {
	connectPts := []PointResult{
		{VCPU: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 50}},
	}
	kcPts := []PointResult{
		{VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 45}},
	}
	anomalies := DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
	if len(anomalies) != 0 {
		t.Errorf("10%% gap should not flag; got %+v", anomalies)
	}
}

func TestDetectCrossEngineAnomalies_AboveThreshold(t *testing.T) {
	connectPts := []PointResult{
		{VCPU: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 100}},
	}
	kcPts := []PointResult{
		{VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 30}},
	}
	anomalies := DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
	if len(anomalies) != 1 {
		t.Fatalf("3.3x gap should flag; got %+v", anomalies)
	}
	a := anomalies[0]
	if a.VCPU != 1 {
		t.Errorf("VCPU = %d, want 1", a.VCPU)
	}
	if a.FasterEngine != "connect" {
		t.Errorf("FasterEngine = %s, want connect", a.FasterEngine)
	}
	if a.Ratio < 3.3 || a.Ratio > 3.4 {
		t.Errorf("Ratio = %f, want ~3.33", a.Ratio)
	}
}

func TestDetectCrossEngineAnomalies_HandlesSingleEnginePass(t *testing.T) {
	connectPts := []PointResult{
		{VCPU: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 100}},
		{VCPU: 2, Engine: "connect", Summary: Summary{MedianMBPerSec: 200}},
	}
	kcPts := []PointResult{
		// KC only ran at vCPU 1, not vCPU 2.
		{VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 90}},
	}
	anomalies := DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
	if len(anomalies) != 0 {
		t.Errorf("orphan vCPUs should be skipped; got %+v", anomalies)
	}
}

func TestDetectCrossEngineAnomalies_ZeroDenominatorSkipped(t *testing.T) {
	connectPts := []PointResult{
		{VCPU: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 0}},
	}
	kcPts := []PointResult{
		{VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 50}},
	}
	anomalies := DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
	// Both engines returning 0 OR one returning 0 means we can't compute
	// a ratio meaningfully. Skip rather than crash on divide-by-zero.
	if len(anomalies) != 0 {
		t.Errorf("zero-throughput point should not flag a divergence; got %+v", anomalies)
	}
}
