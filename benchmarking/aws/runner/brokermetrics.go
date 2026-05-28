// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"fmt"
	"io"
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
