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
//
// Multi-broker semantics: a single frame's body is the concatenation of
// /public_metrics output from EVERY broker (the bench scraper iterates
// over all brokers per interval — see renderBenchScript). Redpanda emits
// per-topic byte counters only on the broker leading the partition, so
// each broker's body contributes the bytes for the partitions it leads;
// summing across the brokers within a frame gives the cluster-wide total
// for that topic. We use `+=` here, not `=`, to aggregate those
// contributions. A single-broker scrape would degenerate to the same
// behavior (only one occurrence per topic).
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
		// Redpanda's /public_metrics labels topics as `redpanda_topic`,
		// not the bare `topic` a vanilla Kafka exporter would use.
		// Verified live on Redpanda v26.1.9.
		topic := labels["redpanda_topic"]
		if topic == "" ||
			strings.HasPrefix(topic, "_kc_") ||
			topic == "__consumer_offsets" ||
			topic == "controller" {
			continue
		}
		// Only attribute bytes from the user-topic namespace; the
		// `redpanda` namespace is for internal controller bookkeeping.
		if ns := labels["redpanda_namespace"]; ns != "" && ns != "kafka" {
			continue
		}
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", line, err)
		}
		// Multi-broker frames concatenate every broker's output; the
		// same topic may appear once per broker, with each broker
		// contributing the bytes for partitions it leads. Sum them.
		out[topic] += v
	}
	return out, scanner.Err()
}

// extractTopicProduceRecords scans a /public_metrics snapshot body for
// redpanda_kafka_records_produced_total{topic=...} counter samples and returns
// the latest value per topic.
//
// This is the COMPRESSION-INDEPENDENT throughput metric, and it is the only
// basis on which Connect and Kafka Connect can be fairly compared.
//
// Byte-based comparison is confounded twice over. First, the two engines'
// headline numbers are read at different points: Connect's comes from its own
// rolling-stats log (uncompressed logical message sizes) while KC's is derived
// from broker produce-request bytes (compressed, on the wire). Second, the two
// producers don't share compression settings. The gap is large: across the
// suite Connect's headline runs 11-17x above its own broker byte series on
// postgres, mysql, oracle and sqlserver — and only ~1.1x on mongodb, whose
// seeder is the one that emits DISTINCT row payloads (randomPayloadPool)
// instead of reusing a single identical payload for every row. Identical rows
// compress enormously; that ratio, not a throughput difference, is what the
// byte comparison was measuring.
//
// Record counts are immune to all of it: one row in, one record out.
//
// Same label shape and exclusions as extractTopicProduceBytes, and likewise
// summed across the per-broker bodies concatenated into a single frame.
// records_fetched_total is deliberately NOT read — that is consume-side.
func extractTopicProduceRecords(body string) (map[string]float64, error) {
	out := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "redpanda_kafka_records_produced_total{") {
			continue
		}
		labels, valueStr, ok := splitLabeledMetric(line)
		if !ok {
			continue
		}
		topic := labels["redpanda_topic"]
		if topic == "" ||
			strings.HasPrefix(topic, "_kc_") ||
			topic == "__consumer_offsets" ||
			topic == "controller" {
			continue
		}
		if ns := labels["redpanda_namespace"]; ns != "" && ns != "kafka" {
			continue
		}
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", line, err)
		}
		out[topic] += v
	}
	return out, scanner.Err()
}

// TopicPoint is one inter-frame throughput sample for a single topic.
type TopicPoint struct {
	T           int     `json:"t"` // seconds since first frame
	MBPerSec    float64 `json:"mb_per_sec"`
	MsgPerSec   float64 `json:"msg_per_sec"`
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
	prevRecords := map[string]float64{}
	out := map[string][]TopicPoint{}
	// noGoodFrameYet is the lastGoodTime sentinel before any non-errored
	// frame has been processed. Frame timestamps are unix seconds, always
	// positive, so -1 can never collide with a real timestamp.
	const noGoodFrameYet = int64(-1)
	// lastGoodTime is the timestamp of the most recently processed
	// NON-errored frame, not simply frames[i-1]. An errored frame is
	// skipped above before prevBytes/prevRecords are touched, so the next
	// good frame's delta still spans back to the last frame that actually
	// contributed counter values; using frames[i-1] there would understate
	// the interval and inflate the computed rate (see the regression this
	// guards against: an errored frame sandwiched between two good ones
	// used to compute a 10s interval for a delta that actually spanned
	// 20s).
	lastGoodTime := noGoodFrameYet
	for i, f := range frames {
		if f.Errored {
			continue
		}
		bytesByTopic, err := extractTopicProduceBytes(f.Body)
		if err != nil {
			return nil, fmt.Errorf("frame %d at t=%d: %w", i, f.UnixTime, err)
		}
		// Records are tracked alongside bytes so every TopicPoint carries a
		// compression-independent figure. Before this, MsgPerSec was declared
		// and serialized but never assigned, so every KC point in every result
		// on disk reported median_msg_s = 0 — silently removing the only metric
		// that made the head-to-head comparable.
		recordsByTopic, err := extractTopicProduceRecords(f.Body)
		if err != nil {
			return nil, fmt.Errorf("frame %d at t=%d: %w", i, f.UnixTime, err)
		}
		for topic, cur := range bytesByTopic {
			prev, hadPrev := prevBytes[topic]
			prevBytes[topic] = cur

			curRecs, hasRecs := recordsByTopic[topic]
			prevRecs, hadPrevRecs := prevRecords[topic]
			if hasRecs {
				prevRecords[topic] = curRecs
			}

			// !hadPrev covers a topic seen for the first time in this
			// frame; lastGoodTime == noGoodFrameYet covers this being the
			// first good frame overall (which implies !hadPrev too, but
			// spelling it out keeps the "no prior good frame" case
			// explicit rather than relying on the map's zero value).
			if !hadPrev || lastGoodTime == noGoodFrameYet {
				continue
			}
			deltaBytes := cur - prev
			interval := int(f.UnixTime - lastGoodTime)
			if interval <= 0 || deltaBytes < 0 {
				continue // counter reset or out-of-order frame; skip
			}
			// A records counter reset (or a topic whose records metric is
			// missing from this frame) leaves MsgPerSec at zero rather than
			// discarding the byte sample, so one flaky metric can't erase the
			// other.
			var msgPerSec float64
			if hasRecs && hadPrevRecs {
				if d := curRecs - prevRecs; d >= 0 {
					msgPerSec = d / float64(interval)
				}
			}
			out[topic] = append(out[topic], TopicPoint{
				T:           int(f.UnixTime - baseT),
				MBPerSec:    deltaBytes / float64(interval) / bytesPerMB,
				MsgPerSec:   msgPerSec,
				IntervalSec: interval,
			})
		}
		lastGoodTime = f.UnixTime
	}
	return out, nil
}

// AttributeConnect picks Connect's series out of a full per-topic series map
// for a given bench session. Connect writes to exactly one topic:
// bench_<session>_<connector>_connect.
//
// This used to fan out into a per-engine map (AttributeByEngine) that also
// merged Kafka Connect's topic-per-table series (Debezium prepends
// topic.prefix to a per-table topic, so KC's throughput was the point-wise
// sum across all of them). That merge — and the KC half of the attribution
// — returns with the kafka-connect bench PR.
func AttributeConnect(series map[string][]TopicPoint, sessionID, connector string) []TopicPoint {
	connectTopic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)
	return series[connectTopic]
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
