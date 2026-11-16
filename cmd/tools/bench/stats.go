// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"io"
	"regexp"
	"slices"
	"strconv"
)

// bytesPerMB is the divisor of every MB figure: SI base-10, the unit the
// benchmark processor logs and the AWS harness in benchmarking/aws reports.
const bytesPerMB = 1_000_000

// Sample is one rolling-stats line of the benchmark processor.
type Sample struct {
	// T is the index of the sample in the run. Suites log one sample per
	// second, so T is also the number of seconds since the first sample.
	T         int     `json:"t"`
	MsgPerSec float64 `json:"msg_per_sec"`
	MBPerSec  float64 `json:"mb_per_sec"`
}

// Total is the total-stats line the benchmark processor logs at shutdown.
type Total struct {
	MsgPerSec float64 `json:"msg_per_sec"`
	MBPerSec  float64 `json:"mb_per_sec"`
}

// Summary aggregates the active window of a run: the samples from the first
// to the last one that saw messages. Field names match the Summary of the AWS
// harness so both kinds of result read the same.
type Summary struct {
	// Samples is the number of samples in the active window.
	Samples         int     `json:"samples"`
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

var (
	// The bytes figure is absent when the processor runs with count_bytes: false.
	statsRe = regexp.MustCompile(`(rolling|total) stats:\s+([0-9.e+]+)\s+msg/sec(?:,\s+([0-9.]+)\s+(B|kB|MB|GB|TB|PB|EB)/sec)?`)
	// The log line also carries the exact figures as structured fields, in
	// logfmt (bytes/sec=2.03e+08) or JSON ("bytes/sec":2.03e+08). They are
	// preferred over the message text, which rounds to three digits.
	exactMsgRe   = regexp.MustCompile(`msg/sec"?[=:]\s*([0-9.eE+-]+)`)
	exactBytesRe = regexp.MustCompile(`bytes/sec"?[=:]\s*([0-9.eE+-]+)`)
)

var bytesUnit = map[string]float64{
	"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15, "EB": 1e18,
}

// parseStatsLine extracts the throughput of a benchmark processor log line.
// The window is "rolling" or "total", or empty when the line is not one.
func parseStatsLine(line string) (window string, msgPerSec, mbPerSec float64) {
	m := statsRe.FindStringSubmatch(line)
	if m == nil {
		return "", 0, 0
	}
	msg, err := strconv.ParseFloat(m[2], 64)
	if err != nil {
		return "", 0, 0
	}
	var bytesPerSec float64
	if m[3] != "" {
		if v, err := strconv.ParseFloat(m[3], 64); err == nil {
			bytesPerSec = v * bytesUnit[m[4]]
		}
	}
	if e := exactMsgRe.FindStringSubmatch(line); e != nil {
		if v, err := strconv.ParseFloat(e[1], 64); err == nil {
			msg = v
		}
	}
	if e := exactBytesRe.FindStringSubmatch(line); e != nil {
		if v, err := strconv.ParseFloat(e[1], 64); err == nil {
			bytesPerSec = v
		}
	}
	return m[1], msg, bytesPerSec / bytesPerMB
}

// statsCollector accumulates the samples of a log, one line at a time.
type statsCollector struct {
	samples []Sample
	total   *Total
}

// add feeds one log line and reports whether it was a rolling sample.
func (c *statsCollector) add(line string) bool {
	window, msg, mb := parseStatsLine(line)
	switch window {
	case "rolling":
		c.samples = append(c.samples, Sample{T: len(c.samples), MsgPerSec: msg, MBPerSec: mb})
		return true
	case "total":
		c.total = &Total{MsgPerSec: msg, MBPerSec: mb}
	}
	return false
}

// trailingIdle returns how many samples at the end of the run saw no message,
// or zero when no sample saw one yet: a suite that is still starting up is not
// idle.
func (c *statsCollector) trailingIdle() int {
	from, to := activeWindow(c.samples)
	if from == to {
		return 0
	}
	return len(c.samples) - to
}

func parseStats(r io.Reader) (*statsCollector, error) {
	var c statsCollector
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			c.add(line)
		}
		if err == io.EOF {
			return &c, nil
		}
		if err != nil {
			return &c, err
		}
	}
}

// activeWindow returns the half-open index range from the first to the last
// sample that saw messages.
func activeWindow(samples []Sample) (from, to int) {
	from = slices.IndexFunc(samples, func(s Sample) bool { return s.MsgPerSec > 0 })
	if from < 0 {
		return 0, 0
	}
	for to = len(samples); samples[to-1].MsgPerSec <= 0; to-- {
	}
	return from, to
}

// summarise aggregates the active window of the samples. Warm-up before the
// first message and the idle tail after the last one would otherwise drag
// every figure down by an amount that depends on when the run was stopped.
func summarise(samples []Sample) Summary {
	from, to := activeWindow(samples)
	samples = samples[from:to]
	if len(samples) == 0 {
		return Summary{}
	}
	mb := make([]float64, len(samples))
	msg := make([]float64, len(samples))
	for i, s := range samples {
		mb[i] = s.MBPerSec
		msg[i] = s.MsgPerSec
	}
	slices.Sort(mb)
	slices.Sort(msg)
	return Summary{
		Samples:         len(samples),
		MedianMBPerSec:  percentile(mb, 50),
		P5MBPerSec:      percentile(mb, 5),
		P95MBPerSec:     percentile(mb, 95),
		PeakMBPerSec:    mb[len(mb)-1],
		MeanMBPerSec:    mean(mb),
		MedianMsgPerSec: percentile(msg, 50),
		P5MsgPerSec:     percentile(msg, 5),
		P95MsgPerSec:    percentile(msg, 95),
		PeakMsgPerSec:   msg[len(msg)-1],
		MeanMsgPerSec:   mean(msg),
	}
}

// percentile interpolates linearly between sorted values (Hazen method), the
// same way the AWS harness does.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	rank := (p/100)*float64(n) - 0.5
	if rank <= 0 {
		return sorted[0]
	}
	if rank >= float64(n-1) {
		return sorted[n-1]
	}
	lo := int(rank)
	return sorted[lo] + (rank-float64(lo))*(sorted[lo+1]-sorted[lo])
}

func mean(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
