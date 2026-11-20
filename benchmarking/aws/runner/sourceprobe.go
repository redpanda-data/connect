// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// A stack-none point decides whether the producer limited it from the
// source side, not from output-side msg/s (which filtering or fan-out
// pipelines skew). The bench script runs rpk against the brokers at the
// start and end of the measurement window and frames the raw output on its
// stdout under sourceProbePrefix; parseSourceProbe turns it into the
// achieved producer rate and the consumer group's lag at the end.

// sourceProbePrefix frames source-probe lines on the bench script's stdout:
// "###src:<tag> <line>", with tags t0, hwm0, t1, hwm1, group and error.
const sourceProbePrefix = "###src:"

// producerBoundLagSec: a group that ends the window less than this many
// seconds of production behind has caught up with the producer, so the
// producer, not the pipeline, was the ceiling.
const producerBoundLagSec = 5

const (
	anomalyProducerBound       = "producer_bound"
	anomalyProducerUnreachable = "producer_unreachable"
)

// SourceProbeArgs names what the source probe describes. Group is set per
// point (BenchNames.ConsumerGroup).
type SourceProbeArgs struct {
	Brokers string
	Topic   string
	Group   string
}

// renderSourceProbe renders the bash of one probe phase. "start" records the
// window start time and the topic's high watermarks; "end" records the end
// time, the high watermarks and the group's committed offsets. A failed rpk
// call is framed as "error <tag>" rather than aborting the bench.
func renderSourceProbe(a SourceProbeArgs, phase string) string {
	describe := fmt.Sprintf(`/usr/local/bin/rpk topic describe -p %q -X brokers=%q`, a.Topic, a.Brokers)
	frame := func(tag, cmd string) string {
		return fmt.Sprintf(`{ %s 2>&1 | sed 's/^/%s%s /'; } || echo "%serror %s"`, cmd, sourceProbePrefix, tag, sourceProbePrefix, tag)
	}
	if phase == "start" {
		return fmt.Sprintf("echo \"%st0 $(date +%%s)\"\n%s", sourceProbePrefix, frame("hwm0", describe))
	}
	group := fmt.Sprintf(`/usr/local/bin/rpk group describe %q -X brokers=%q`, a.Group, a.Brokers)
	return fmt.Sprintf("echo \"%st1 $(date +%%s)\"\n%s\n%s", sourceProbePrefix, frame("hwm1", describe), frame("group", group))
}

// partitionOffsets is one partition row of `rpk topic describe -p`.
type partitionOffsets struct {
	LogStart      int64
	HighWatermark int64
}

// table finds the header row starting with first and returns the column
// index of each header and the rows that follow it, up to a blank line.
func table(lines []string, first string) (map[string]int, [][]string, bool) {
	for i, l := range lines {
		f := strings.Fields(l)
		if len(f) == 0 || f[0] != first {
			continue
		}
		cols := make(map[string]int, len(f))
		for j, h := range f {
			cols[h] = j
		}
		var rows [][]string
		for _, r := range lines[i+1:] {
			rf := strings.Fields(r)
			if len(rf) == 0 {
				break
			}
			rows = append(rows, rf)
		}
		return cols, rows, true
	}
	return nil, nil, false
}

func cell(row []string, cols map[string]int, name string) (string, error) {
	i, ok := cols[name]
	if !ok {
		return "", fmt.Errorf("column %s missing", name)
	}
	if i >= len(row) {
		return "", fmt.Errorf("row %q has no %s column", strings.Join(row, " "), name)
	}
	return row[i], nil
}

func intCell(row []string, cols map[string]int, name string) (int64, error) {
	v, err := cell(row, cols, name)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

// parseTopicPartitions parses `rpk topic describe -p` output.
func parseTopicPartitions(lines []string) (map[int]partitionOffsets, error) {
	cols, rows, ok := table(lines, "PARTITION")
	if !ok {
		return nil, fmt.Errorf("rpk topic describe: no PARTITION header in %q", strings.Join(lines, "\n"))
	}
	out := make(map[int]partitionOffsets, len(rows))
	for _, r := range rows {
		p, err := intCell(r, cols, "PARTITION")
		if err != nil {
			return nil, err
		}
		ls, err := intCell(r, cols, "LOG-START-OFFSET")
		if err != nil {
			return nil, err
		}
		hwm, err := intCell(r, cols, "HIGH-WATERMARK")
		if err != nil {
			return nil, err
		}
		out[int(p)] = partitionOffsets{LogStart: ls, HighWatermark: hwm}
	}
	if len(out) == 0 {
		return nil, errors.New("rpk topic describe: no partitions")
	}
	return out, nil
}

// parseGroupCommitted parses `rpk group describe` output into the committed
// offset per partition of topic. Partitions without a commit ("-") are left
// out, and a group with no commits at all (state Dead) yields an empty map.
func parseGroupCommitted(lines []string, topic string) (map[int]int64, error) {
	if _, _, ok := table(lines, "GROUP"); !ok {
		return nil, fmt.Errorf("rpk group describe: no GROUP line in %q", strings.Join(lines, "\n"))
	}
	out := map[int]int64{}
	cols, rows, ok := table(lines, "TOPIC")
	if !ok {
		return out, nil
	}
	for _, r := range rows {
		t, err := cell(r, cols, "TOPIC")
		if err != nil {
			return nil, err
		}
		if t != topic {
			continue
		}
		p, err := intCell(r, cols, "PARTITION")
		if err != nil {
			return nil, err
		}
		c, err := cell(r, cols, "CURRENT-OFFSET")
		if err != nil {
			return nil, err
		}
		if c == "-" {
			continue
		}
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("CURRENT-OFFSET: %w", err)
		}
		out[int(p)] = n
	}
	return out, nil
}

// SourceMeasurement is a stack-none point's source-side evidence.
type SourceMeasurement struct {
	WindowSec        float64
	HWMStart, HWMEnd int64 // summed high watermarks of the source topic
	LagEnd           int64 // the point's consumer group lag at window end
}

// ProducerRate is the producer's achieved msg/s over the window.
func (m SourceMeasurement) ProducerRate() float64 {
	if m.WindowSec <= 0 {
		return 0
	}
	return float64(m.HWMEnd-m.HWMStart) / m.WindowSec
}

// parseSourceProbe turns framed probe lines into a SourceMeasurement. Lag
// is computed per partition from the end high watermarks rather than taken
// from rpk's TOTAL-LAG, which ignores partitions the group never committed:
// those have consumed nothing, so their whole retained log is lag.
func parseSourceProbe(lines []string, topic string) (SourceMeasurement, error) {
	sections := map[string][]string{}
	for _, l := range lines {
		rest, ok := strings.CutPrefix(l, sourceProbePrefix)
		if !ok {
			continue
		}
		tag, body, _ := strings.Cut(rest, " ")
		if tag == "error" {
			return SourceMeasurement{}, fmt.Errorf("source probe: rpk failed for %s", body)
		}
		sections[tag] = append(sections[tag], body)
	}
	for _, tag := range []string{"t0", "t1", "hwm0", "hwm1", "group"} {
		if len(sections[tag]) == 0 {
			return SourceMeasurement{}, fmt.Errorf("source probe: no %s output", tag)
		}
	}
	t0, err := strconv.ParseInt(strings.TrimSpace(sections["t0"][0]), 10, 64)
	if err != nil {
		return SourceMeasurement{}, fmt.Errorf("source probe t0: %w", err)
	}
	t1, err := strconv.ParseInt(strings.TrimSpace(sections["t1"][0]), 10, 64)
	if err != nil {
		return SourceMeasurement{}, fmt.Errorf("source probe t1: %w", err)
	}
	if t1 <= t0 {
		return SourceMeasurement{}, fmt.Errorf("source probe: empty window %d..%d", t0, t1)
	}
	start, err := parseTopicPartitions(sections["hwm0"])
	if err != nil {
		return SourceMeasurement{}, fmt.Errorf("source probe start: %w", err)
	}
	end, err := parseTopicPartitions(sections["hwm1"])
	if err != nil {
		return SourceMeasurement{}, fmt.Errorf("source probe end: %w", err)
	}
	committed, err := parseGroupCommitted(sections["group"], topic)
	if err != nil {
		return SourceMeasurement{}, fmt.Errorf("source probe group: %w", err)
	}
	m := SourceMeasurement{WindowSec: float64(t1 - t0)}
	for _, p := range start {
		m.HWMStart += p.HighWatermark
	}
	for id, p := range end {
		m.HWMEnd += p.HighWatermark
		// Records retention already deleted cannot be lag.
		from := p.LogStart
		if c, ok := committed[id]; ok && c > from {
			from = c
		}
		m.LagEnd += max(0, p.HighWatermark-from)
	}
	return m, nil
}

// sourceAnomalies judges a stack-none point from its source measurement.
// Nothing produced in the window fails the point (producer_unreachable); a
// group within producerBoundLagSec of the producer is producer_bound.
func sourceAnomalies(m SourceMeasurement) (anomalies []Anomaly, fail bool) {
	rate := m.ProducerRate()
	if rate <= 0 {
		return []Anomaly{{
			Kind: anomalyProducerUnreachable,
			Note: fmt.Sprintf("%s: the source topic gained no records in the %.0fs window; the producer never reached the brokers", anomalyProducerUnreachable, m.WindowSec),
		}}, true
	}
	if float64(m.LagEnd) < producerBoundLagSec*rate {
		return []Anomaly{{
			Kind: anomalyProducerBound,
			Note: fmt.Sprintf("%s: the consumer group ended %d records behind, under %ds of the producer's %.0f msg/s; the producer, not the pipeline, was the ceiling", anomalyProducerBound, m.LagEnd, producerBoundLagSec, rate),
		}}, false
	}
	return nil, false
}
