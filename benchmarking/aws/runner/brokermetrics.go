// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// parseBrokerFrames splits a redpanda-<vcpu>.txt dump into the same
// frame shape that prom.go's parseSnapshots produces. We delegate to
// the existing private helper so a future schema change in the framing
// (timestamp marker format, etc.) only has to be made in one place.
//
// Returns (frames, nil) on success. The signature returns an error for
// forward-compatibility — if future framing changes need to surface
// I/O errors, we can fix that here without rippling through callers.
func parseBrokerFrames(r io.Reader) ([]promSnapshot, error) {
	return parseSnapshots(r), nil
}

// extractTopicProduceBytes scans a /public_metrics snapshot body for
// redpanda_kafka_request_bytes_total{redpanda_request="produce",topic=...}
// counter samples and returns the latest value per topic.
//
// Why produce-side only: Plan 3 attributes throughput to the engine that
// WROTE the bytes (Connect or KC writing into the per-engine topic).
// Consume-side bytes belong to downstream readers and aren't part of the
// engine comparison.
//
// Topics with the "_kc_" prefix (KC's internal config/status/offset
// topics) are excluded — they're worker bookkeeping, not bench output.
func extractTopicProduceBytes(body string) (map[string]float64, error) {
	out := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "redpanda_kafka_request_bytes_total{") {
			continue
		}
		labels, valueStr, ok := splitLabeledMetric(line)
		if !ok {
			continue
		}
		if labels["redpanda_request"] != "produce" {
			continue
		}
		topic := labels["topic"]
		if topic == "" || strings.HasPrefix(topic, "_kc_") {
			continue
		}
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", line, err)
		}
		out[topic] = v
	}
	return out, scanner.Err()
}

// TopicPoint is one inter-frame throughput sample for a single topic.
type TopicPoint struct {
	T           int     `json:"t"` // seconds since first frame
	MBPerSec    float64 `json:"mb_per_sec"`
	IntervalSec int     `json:"-"` // for debugging; not serialized
}

// ParseTopicSeries reads a full redpanda-<vcpu>.txt dump and returns a
// per-topic throughput series. Each topic's series has one point per
// inter-frame delta (so N frames produce N-1 points). T is measured in
// seconds since the FIRST frame's timestamp, matching the Sample.T
// convention used elsewhere in the runner.
//
// Counter resets (current < previous) are filtered out — that situation
// almost always indicates a broker restart and the delta would be a
// large negative value if computed naively.
func ParseTopicSeries(r io.Reader) (map[string][]TopicPoint, error) {
	frames, err := parseBrokerFrames(r)
	if err != nil {
		return nil, err
	}
	if len(frames) == 0 {
		return map[string][]TopicPoint{}, nil
	}
	baseT := frames[0].UnixTime
	prevBytes := map[string]float64{}
	out := map[string][]TopicPoint{}
	for i, f := range frames {
		if f.Errored {
			continue
		}
		bytesByTopic, err := extractTopicProduceBytes(f.Body)
		if err != nil {
			return nil, fmt.Errorf("frame %d at t=%d: %w", i, f.UnixTime, err)
		}
		for topic, cur := range bytesByTopic {
			prev, hadPrev := prevBytes[topic]
			prevBytes[topic] = cur
			if !hadPrev || i == 0 {
				continue
			}
			deltaBytes := cur - prev
			interval := int(f.UnixTime - frames[i-1].UnixTime)
			if interval <= 0 || deltaBytes < 0 {
				continue // counter reset or out-of-order frame; skip
			}
			out[topic] = append(out[topic], TopicPoint{
				T:           int(f.UnixTime - baseT),
				MBPerSec:    deltaBytes / float64(interval) / (1 << 20),
				IntervalSec: interval,
			})
		}
	}
	return out, nil
}

// AttributeByEngine groups per-topic series into per-engine series for a
// given bench session. The mapping rules mirror the topic-naming
// conventions baked into Plan 2:
//
//	Connect → exactly one topic:  bench_<session>_<connector>_connect
//	KC      → many topics:        bench_<session>_<connector>_kc.<schema>.<table>
//	                              (Debezium prepends topic.prefix to a
//	                              per-table topic)
//
// KC's per-table series are summed point-wise on matching T values; if
// the per-topic series have ragged T values, missing values count as
// zero. (In practice all Plan 2 KC topics scrape at the same cadence,
// so the merge is straightforward.)
func AttributeByEngine(series map[string][]TopicPoint, sessionID, connector string) (map[string][]TopicPoint, error) {
	connectTopic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)
	kcPrefix := fmt.Sprintf("bench_%s_%s_kc", sessionID, connector)
	out := map[string][]TopicPoint{
		"connect":       nil,
		"kafka_connect": nil,
	}
	if pts := series[connectTopic]; pts != nil {
		out["connect"] = pts
	}
	var kcTopics []string
	for t := range series {
		if t == kcPrefix || strings.HasPrefix(t, kcPrefix+".") {
			kcTopics = append(kcTopics, t)
		}
	}
	if len(kcTopics) > 0 {
		out["kafka_connect"] = mergeTopicSeries(series, kcTopics)
	}
	return out, nil
}

func mergeTopicSeries(series map[string][]TopicPoint, topics []string) []TopicPoint {
	byT := map[int]float64{}
	for _, t := range topics {
		for _, p := range series[t] {
			byT[p.T] += p.MBPerSec
		}
	}
	ts := make([]int, 0, len(byT))
	for t := range byT {
		ts = append(ts, t)
	}
	sort.Ints(ts)
	out := make([]TopicPoint, len(ts))
	for i, t := range ts {
		out[i] = TopicPoint{T: t, MBPerSec: byT[t]}
	}
	return out
}

// splitLabeledMetric parses a single metric line of the form
//
//	name{k1="v1",k2="v2",...} value
//
// into the labels map and the value substring. Hand-rolled rather than
// pulling in prometheus/common/expfmt: the redpanda exporter's label
// values don't contain commas inside quoted strings, so the cheap split
// is sufficient. If that ever changes, swap in a real text-format parser.
func splitLabeledMetric(line string) (map[string]string, string, bool) {
	open := strings.Index(line, "{")
	closeIdx := strings.Index(line, "}")
	if open < 0 || closeIdx < 0 || closeIdx < open {
		return nil, "", false
	}
	labelsRaw := line[open+1 : closeIdx]
	rest := strings.TrimSpace(line[closeIdx+1:])
	valueStr := rest
	if sp := strings.IndexAny(rest, " \t"); sp >= 0 {
		valueStr = rest[:sp]
	}
	labels := map[string]string{}
	for _, pair := range strings.Split(labelsRaw, ",") {
		eq := strings.Index(pair, "=")
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(pair[:eq])
		v := strings.TrimSpace(pair[eq+1:])
		v = strings.Trim(v, `"`)
		labels[k] = v
	}
	return labels, valueStr, true
}
