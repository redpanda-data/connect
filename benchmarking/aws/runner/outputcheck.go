// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Point-level anomaly kinds of an e2e point (see e2ePointAnomalies).
const (
	// anomalyNoSamples: the point captured no output samples. Fails it.
	anomalyNoSamples = "no_samples"
	// anomalySinkErrors: Connect's output_error counter grew past
	// sinkErrorFailShare of the batches sent, or grew with no batch count
	// to compare it against. Fails it.
	anomalySinkErrors = "sink_errors"
	// anomalySinkRetried: output_error grew, but by no more than
	// sinkErrorFailShare of the batches sent (retried writes, reconnects).
	// A caveat, not a failure.
	anomalySinkRetried = "sink_retried"
	// anomalySinkSendMismatch: output_sent and the harness benchmark
	// processor's message count disagree beyond the count tolerance. Fails it.
	anomalySinkSendMismatch = "sink_send_mismatch"
	// anomalySinkUnverified: neither the sink count nor output_sent could
	// check what the sink received. A caveat, not a failure.
	anomalySinkUnverified = "sink_unverified"
	// anomalyWorkloadBound: the point kept up with the CDC workload's
	// offered rate, so the workload, not the pipeline, capped it. A
	// caveat; the catalogue point is LimitedBy "workload".
	anomalyWorkloadBound = "workload_bound"
)

// sinkErrorFailShare is the share of sent batches output_error may reach
// before a point fails; at or under it the point passes with sink_retried.
// output_error counts one per failed write, and a write is a batch, so it
// is judged against output_batch_sent, not output_sent (messages).
const sinkErrorFailShare = 0.01

// workloadBoundFraction is how close to the offered CDC write rate a
// point's median must be to count as workload-bound.
const workloadBoundFraction = 0.95

// outputCounters are Connect's output counters at the last complete
// /metrics snapshot of a point. Connect starts fresh at every point, so each
// value is the count since the point's launch.
type outputCounters struct {
	// Sent is output_sent; HasSent is false when it is absent, or when the
	// output is a nest of child outputs none of which is root.output
	// (a fan-out broker counts each message once per child).
	Sent    float64
	HasSent bool
	// BatchSent is output_batch_sent, under the same root.output rule as
	// Sent. output_error counts failed writes, which are batches, so the
	// error share is of BatchSent.
	BatchSent    float64
	HasBatchSent bool
	// Errors is output_error summed over every output.
	Errors    float64
	HasErrors bool
	// Processed is the harness benchmark processor's
	// benchmark_messages_total (the last processor of root.output), taken
	// from the same snapshot as Sent, so the two share one instant.
	Processed    float64
	HasProcessed bool
}

type labelledValue struct {
	labels string
	value  float64
}

var outputProcPath = regexp.MustCompile(`path="root\.output\.processors\.(\d+)"`)

// parseOutputCounters reads outputCounters from a per-point prom dump
// (prom-<key>.txt). Only complete frames count: not errored, and carrying
// process_resident_memory_bytes (see extractPromPoint).
func parseOutputCounters(raw []byte) outputCounters {
	var last map[string][]labelledValue
	for _, s := range parseSnapshots(bytes.NewReader(raw)) {
		if s.Errored {
			continue
		}
		m := labelledMetrics(s.Body)
		if _, complete := m["process_resident_memory_bytes"]; complete {
			last = m
		}
	}
	var c outputCounters
	if last == nil {
		return c
	}
	if vs := last["output_error"]; len(vs) > 0 {
		c.HasErrors = true
		for _, v := range vs {
			c.Errors += v.value
		}
	}
	c.Sent, c.HasSent = rootOutputValue(last["output_sent"])
	c.BatchSent, c.HasBatchSent = rootOutputValue(last["output_batch_sent"])
	best := -1
	for _, v := range last["benchmark_messages_total"] {
		idx := 0
		if m := outputProcPath.FindStringSubmatch(v.labels); m != nil {
			idx, _ = strconv.Atoi(m[1])
		} else if v.labels != "" {
			continue // a benchmark processor outside root.output
		}
		if idx > best {
			best, c.Processed, c.HasProcessed = idx, v.value, true
		}
	}
	return c
}

// rootOutputValue picks the root output's series of an output counter: the
// only series, else the one at root.output. A nest of child outputs none of
// which is root.output has none (a fan-out broker counts each message once
// per child).
func rootOutputValue(vs []labelledValue) (float64, bool) {
	if len(vs) == 1 {
		return vs[0].value, true
	}
	for _, v := range vs {
		if strings.Contains(v.labels, `path="root.output"`) {
			return v.value, true
		}
	}
	return 0, false
}

// labelledMetrics parses a /metrics text body into name -> series, keeping
// each series' label text.
func labelledMetrics(body string) map[string][]labelledValue {
	out := map[string][]labelledValue{}
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var name, labels, rest string
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name, labels, rest = line[:i], line[i+1:j], line[j+1:]
		} else if i := strings.IndexByte(line, ' '); i > 0 {
			name, rest = line[:i], line[i:]
		} else {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out[name] = append(out[name], labelledValue{labels: labels, value: v})
	}
	return out
}

// e2ePoint is what e2ePointAnomalies judges about one e2e point.
type e2ePoint struct {
	Samples  int // trimmed samples captured
	Counters outputCounters
	// SinkCounted is true when the sink's own count checked the point.
	SinkCounted bool
	// SinkDrop is true for the drop sink, which has nothing to verify.
	SinkDrop        bool
	MedianMsgPerSec float64
	// WorkloadRatePerSec is the CDC workload's offered rate; 0 when the
	// point has no fixed-rate workload (stack none, a source scenario).
	WorkloadRatePerSec float64
}

// e2ePointAnomalies returns an e2e point's point-level anomalies and
// whether any of them fails the point.
func e2ePointAnomalies(p e2ePoint) (out []Anomaly, fail bool) {
	if p.Samples == 0 {
		return []Anomaly{{Kind: anomalyNoSamples, Note: anomalyNoSamples + ": the point captured no output samples; see run artefacts in the session bucket"}}, true
	}
	c := p.Counters
	if c.HasErrors && c.Errors > 0 {
		// output_error counts failed writes (batches), so its share is of
		// output_batch_sent; never of messages, which would shrink it by
		// the batch size. With no batch count, errors cannot be judged small.
		base, hasBase := c.BatchSent, c.HasBatchSent && c.BatchSent > 0
		if hasBase && c.Errors <= sinkErrorFailShare*base {
			out = append(out, Anomaly{Kind: anomalySinkRetried, Note: fmt.Sprintf(
				"%s: the output reported %.0f write errors (output_error), %.2f%% of %.0f batches sent (output_batch_sent), at or under the %.0f%% limit; the sink retried writes",
				anomalySinkRetried, c.Errors, 100*c.Errors/base, base, sinkErrorFailShare*100)})
		} else {
			share := "with no batch count (output_batch_sent) to compare against"
			if hasBase {
				share = fmt.Sprintf("%.2f%% of %.0f batches sent (output_batch_sent), over the %.0f%% limit", 100*c.Errors/base, base, sinkErrorFailShare*100)
			}
			out = append(out, Anomaly{Kind: anomalySinkErrors, Note: fmt.Sprintf(
				"%s: the output reported %.0f write errors (output_error) during the point, %s", anomalySinkErrors, c.Errors, share)})
			fail = true
		}
	}
	sendChecked := false
	if c.HasSent && c.HasProcessed {
		sendChecked = true
		diff := math.Abs(c.Sent - c.Processed)
		tolerance := max(sinkCountTolerance*c.Processed, sinkCountFloorSec*p.MedianMsgPerSec)
		if diff > 0 && diff > tolerance {
			out = append(out, Anomaly{Kind: anomalySinkSendMismatch, Note: fmt.Sprintf(
				"%s: the output sent %.0f messages (output_sent) but the output samples counted %.0f; they differ by %.0f, over the %.0f tolerance (the larger of %.0f%% and %ds of the median rate)",
				anomalySinkSendMismatch, c.Sent, c.Processed, diff, tolerance, sinkCountTolerance*100, sinkCountFloorSec)})
			fail = true
		}
	}
	if !p.SinkDrop && !p.SinkCounted && !sendChecked {
		out = append(out, Anomaly{Kind: anomalySinkUnverified, Note: anomalySinkUnverified +
			": neither a sink count nor Connect's output_sent could check what the sink received"})
	}
	if p.WorkloadRatePerSec > 0 && p.MedianMsgPerSec >= workloadBoundFraction*p.WorkloadRatePerSec {
		out = append(out, Anomaly{Kind: anomalyWorkloadBound, Note: fmt.Sprintf(
			"%s: the median %.0f msg/s is at least %.0f%% of the workload's %.0f msg/s; the CDC source rate, not the pipeline, capped this point",
			anomalyWorkloadBound, p.MedianMsgPerSec, workloadBoundFraction*100, p.WorkloadRatePerSec)})
	}
	return out, fail
}

// e2eWorkloadRate is the offered rate an e2e point is judged
// workload_bound against: the CDC workload's write rate on the postgres and
// mysql stacks, 0 otherwise (stack none is judged by its source probe).
func e2eWorkloadRate(s *Scenario) float64 {
	if s.Direction != DirectionE2E || s.Workload == nil {
		return 0
	}
	switch s.Stack {
	case "postgres", "mysql":
		return float64(s.Workload.WriteRatePerSec)
	}
	return 0
}
