// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

const (
	anomalyMinSeconds = 60
	anomalyThreshold  = 0.8 // MB/sec below this fraction of the reference median is a dip
)

// Anomaly is a contiguous span where MB/sec dropped below the threshold.
type Anomaly struct {
	StartT      int     `json:"start_t"`
	DurationSec int     `json:"duration_s"`
	MinRatio    float64 `json:"min_ratio"`
	Note        string  `json:"note"`
}

// DetectAnomalies scans the sample stream for spans of >= 60 contiguous seconds
// where MB/sec drops below 0.8 * reference. Reference is typically the run's
// own median MB/sec.
func DetectAnomalies(samples []Sample, reference float64) []Anomaly {
	if reference <= 0 || len(samples) == 0 {
		return nil
	}
	threshold := reference * anomalyThreshold
	var out []Anomaly
	i := 0
	for i < len(samples) {
		if samples[i].MBPerSec >= threshold {
			i++
			continue
		}
		start := i
		minVal := samples[i].MBPerSec
		for i < len(samples) && samples[i].MBPerSec < threshold {
			if samples[i].MBPerSec < minVal {
				minVal = samples[i].MBPerSec
			}
			i++
		}
		dur := i - start
		if dur >= anomalyMinSeconds {
			out = append(out, Anomaly{
				StartT:      samples[start].T,
				DurationSec: dur,
				MinRatio:    minVal / reference,
				Note:        "MB/sec dropped below threshold — investigate before publishing",
			})
		}
	}
	return out
}
