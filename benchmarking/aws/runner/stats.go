// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"io"
	"regexp"
	"sort"
	"strconv"
)

// Sample is one second's snapshot of rolling stats from the benchmark processor.
type Sample struct {
	T         int     `json:"t"`
	MsgPerSec float64 `json:"msg_per_sec"`
	MBPerSec  float64 `json:"mb_per_sec"`
}

// Summary aggregates a window of samples.
type Summary struct {
	MedianMBPerSec  float64 `json:"median_mb_s"`
	P5MBPerSec      float64 `json:"p5_mb_s"`
	P95MBPerSec     float64 `json:"p95_mb_s"`
	PeakMBPerSec    float64 `json:"peak_mb_s"`
	MeanMBPerSec    float64 `json:"mean_mb_s"`
	MedianMsgPerSec float64 `json:"median_msg_s"`
	P5MsgPerSec     float64 `json:"p5_msg_s"`
	P95MsgPerSec    float64 `json:"p95_msg_s"`
	PeakMsgPerSec   float64 `json:"peak_msg_s"`
	MeanMsgPerSec   float64 `json:"mean_msg_s"`
}

// The benchmark processor emits bytes/sec via humanize.Bytes (SI base-10):
// "B", "kB" (lowercase k), "MB", "GB", "TB". All are normalised to MB/sec so
// summary statistics are unit-consistent across a sweep — without this, a
// low-throughput sweep point logs "500 kB/sec" and the regex previously
// dropped the sample entirely.
var rollingStatsRe = regexp.MustCompile(`rolling stats:\s+([0-9.]+)\s+msg/sec,\s+([0-9.]+)\s+(B|kB|MB|GB|TB)/sec`)

var bytesUnitToMB = map[string]float64{
	"B":  1.0 / 1_000_000,
	"kB": 1.0 / 1_000,
	"MB": 1,
	"GB": 1_000,
	"TB": 1_000_000,
}

// ParseRollingStatsLine extracts msg/sec and MB/sec from one log line.
// Returns ok=false if the line is not a rolling-stats line.
func ParseRollingStatsLine(line string) (Sample, bool) {
	m := rollingStatsRe.FindStringSubmatch(line)
	if len(m) != 4 {
		return Sample{}, false
	}
	msg, err1 := strconv.ParseFloat(m[1], 64)
	raw, err2 := strconv.ParseFloat(m[2], 64)
	mult, ok := bytesUnitToMB[m[3]]
	if err1 != nil || err2 != nil || !ok {
		return Sample{}, false
	}
	return Sample{MsgPerSec: msg, MBPerSec: raw * mult}, true
}

// ParseRollingStatsStream reads a Connect stdout stream and returns every
// rolling-stats sample in order. The T field is assigned by index.
func ParseRollingStatsStream(r io.Reader) ([]Sample, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var samples []Sample
	t := 0
	for scanner.Scan() {
		s, ok := ParseRollingStatsLine(scanner.Text())
		if !ok {
			continue
		}
		s.T = t
		samples = append(samples, s)
		t++
	}
	return samples, scanner.Err()
}

// Summarise computes percentiles and peak over the given samples.
func Summarise(samples []Sample) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	mb := make([]float64, len(samples))
	msg := make([]float64, len(samples))
	for i, s := range samples {
		mb[i] = s.MBPerSec
		msg[i] = s.MsgPerSec
	}
	return Summary{
		MedianMBPerSec:  percentile(mb, 50),
		P5MBPerSec:      percentile(mb, 5),
		P95MBPerSec:     percentile(mb, 95),
		PeakMBPerSec:    peak(mb),
		MeanMBPerSec:    mean(mb),
		MedianMsgPerSec: percentile(msg, 50),
		P5MsgPerSec:     percentile(msg, 5),
		P95MsgPerSec:    percentile(msg, 95),
		PeakMsgPerSec:   peak(msg),
		MeanMsgPerSec:   mean(msg),
	}
}

// SummariseTopicPoints derives a Summary from a broker-side TopicPoint
// stream. Mirrors Summarise(samples) — median, p5, p95, peak — but
// works against the per-engine broker series for engines that don't
// emit a rolling-stats log we can parse. The Msg* fields stay zero
// because broker bytes don't give us message count without an
// additional metric.
func SummariseTopicPoints(pts []TopicPoint) Summary {
	if len(pts) == 0 {
		return Summary{}
	}
	mb := make([]float64, len(pts))
	msg := make([]float64, len(pts))
	for i, p := range pts {
		mb[i] = p.MBPerSec
		msg[i] = p.MsgPerSec
	}
	return Summary{
		MedianMBPerSec:  percentile(mb, 50),
		P5MBPerSec:      percentile(mb, 5),
		P95MBPerSec:     percentile(mb, 95),
		PeakMBPerSec:    peak(mb),
		MeanMBPerSec:    mean(mb),
		MedianMsgPerSec: percentile(msg, 50),
		P5MsgPerSec:     percentile(msg, 5),
		P95MsgPerSec:    percentile(msg, 95),
		PeakMsgPerSec:   peak(msg),
		MeanMsgPerSec:   mean(msg),
	}
}

// percentile uses linear interpolation (Hazen / Type-5 method):
// rank = p/100 * n - 0.5, interpolated between adjacent sorted values.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	rank := (p/100)*float64(n) - 0.5
	if rank <= 0 {
		return sorted[0]
	}
	if rank >= float64(n-1) {
		return sorted[n-1]
	}
	lo := int(rank)
	hi := lo + 1
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func peak(values []float64) float64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
