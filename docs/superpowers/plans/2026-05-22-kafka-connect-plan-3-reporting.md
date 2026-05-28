# Kafka Connect head-to-head — Plan 3: Reporting + cross-engine anomalies

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Plan 2's raw side-by-side `PointResult` rows into publishable markdown — KC's currently-zero throughput populated from broker-side metrics, the markdown table grouped/labeled by engine, and a cross-engine anomaly detector that flags vCPU points where the two engines diverge sharply. Also lands two scheduler/memory fairness fixes that were deferred from Plan 2 (`chrt --fifo` parity, KC heap-vs-total-RSS skew) so the post-Plan-3 numbers are honest.

**Architecture:** A new `brokermetrics` package parses Redpanda's `/public_metrics` Prom dumps (already captured in S3 as `redpanda-<vcpu>.txt` per Plan 1) into per-topic byte-rate time series. The matrix runner attributes those series to engines by topic name (`bench_<sess>_<conn>_connect` → Connect, `bench_<sess>_<conn>_kc.*` → KC). For KC points, broker-side throughput fills `Summary.MedianMBPerSec`. `AppendMarkdown` is reworked to produce one row per `(vcpu, engine)` with an explicit engine label and a delta column (Connect MB/s − KC MB/s). A new `DetectCrossEngineAnomalies` reads two `[]PointResult` slices and flags vCPU points where the engines disagree beyond a configurable ratio.

**Tech Stack:** Go (existing runner package), Redpanda 24.x `/public_metrics` (Prometheus text format), existing `prom.go` parser plumbing (reused, not duplicated).

**Spec:** [`docs/superpowers/specs/2026-05-22-kafka-connect-comparison-design.md`](../specs/2026-05-22-kafka-connect-comparison-design.md)
**Predecessors:** [Plan 1 (Redpanda + KC infra)](2026-05-22-kafka-connect-plan-1-infra.md), [Plan 2 (engine swap)](2026-05-22-kafka-connect-plan-2-engine-swap.md). Both must be applied before Plan 3 is run.

---

## File Structure

**New files:**

- `benchmarking/aws/runner/brokermetrics.go` — Redpanda `/public_metrics` parser. Reuses the `promSnapshot` frame extraction from `prom.go` but extracts a different set of metric names (`redpanda_kafka_request_bytes_total{topic=...,redpanda_request="produce"}`). Produces a `[]TopicSeries` keyed by topic name, each carrying `[]TopicPoint{T, BytesPerSec, ...}`.
- `benchmarking/aws/runner/brokermetrics_test.go` — table-driven tests over canned `/public_metrics` snapshot bodies.
- `benchmarking/aws/runner/crossengine.go` — `DetectCrossEngineAnomalies(connectPoints, kcPoints []PointResult) []CrossEngineAnomaly`. Compares medians at matching vCPU points.
- `benchmarking/aws/runner/crossengine_test.go` — tests for the divergence detector (above threshold, below threshold, single-engine case).

**Modified files:**

- `benchmarking/aws/runner/matrix.go` — `MatrixRunner.Run` fetches `redpanda-<vcpu>.txt` once per vCPU (regardless of engine) and slices the resulting topic series per engine. For the `kafka_connect` engine pass, `Summary` is derived from the KC-attributed byte rate; for `connect`, broker bytes are kept as a cross-check column in the result JSON but the existing rolling-stats-derived `Summary` remains the primary number (it's lower-jitter and matches the historical baseline).
- `benchmarking/aws/runner/render.go` — `PointResult` gains a `BrokerMBPerSec float64` field (Connect's broker-side bytes/s, retained as a cross-check) and a `Topics []string` field listing which topics' bytes were summed. `AppendMarkdown` is rewritten to produce one row per `(vcpu, engine)` rather than per vcpu; an extra "Δ vs other engine" column annotates each pair.
- `benchmarking/aws/runner/render_test.go` — fixture rewrite for dual-engine result.
- `benchmarking/aws/runner/summary.go` — `RefreshSummary` walks both engines per scenario; "best run" picks Connect's median (existing behavior) but the SUMMARY.md row shows both numbers.
- `benchmarking/aws/runner/summary_test.go` — fixture updates.
- `benchmarking/aws/runner/matrix.go` (renderBenchScript) — drop `chrt --fifo 50` for parity with KC (Plan 2 already dropped it on the KC side).
- `benchmarking/aws/runner/kcscript.go` (renderKCBenchScript) — KC heap budget changes from `-Xmx<memLimit>g` to `-Xmx<floor(0.75 × memLimit)>g` so JVM Metaspace + code cache + direct buffers fit inside the memLimit budget the way Connect's `GOMEMLIMIT` does.
- `benchmarking/aws/README.md` — Plan 3 section documenting the dual-engine markdown shape, broker-attribution rules, and the fairness changes.

**Why this split:**

- `brokermetrics.go` is its own file (and own package eventually if Plan 4 needs sink-side parsing too) because the Redpanda metric schema is different enough from Connect's `/metrics` that conflating the parsers in `prom.go` would muddy `PromPoint`'s semantics. The two parsers share infrastructure (snapshot framing, splitMetricLine) but extract different metrics.
- `crossengine.go` is a leaf utility called from `runBench` after both engines' points are produced; keeping it isolated means it's testable without any matrix-runner plumbing.

---

## Task ordering

Plan 3 has five phases:

- **Phase A (Tasks 1–4):** Broker-metrics parser. Pure TDD; no orchestrator wiring yet. After this phase the parser can be invoked manually but nothing in the runner calls it.
- **Phase B (Tasks 5–8):** Wire broker metrics into `PointResult.Summary` for KC. After this phase a Plan 2 result.json gets non-zero `MedianMBPerSec` on KC points (regenerated by re-running the bench).
- **Phase C (Tasks 9–11):** Engine-aware markdown + SUMMARY.md.
- **Phase D (Tasks 12–13):** Cross-engine anomaly detection.
- **Phase E (Tasks 14–16):** Scheduler/memory fairness fixes + README + operator-driven smoke.

Each task ends in a commit. After Phase A: parser usable but unused. After Phase B: bench produces honest KC numbers. After Phase C: markdown report is readable. After Phase D: divergence flagged. After Phase E: re-baselined under fair scheduler/memory.

---

### Task 1: Reuse `promSnapshot` framing for broker dumps

**Files:**
- Create: `benchmarking/aws/runner/brokermetrics.go`
- Create: `benchmarking/aws/runner/brokermetrics_test.go`

- [ ] **Step 1: Write the failing test for snapshot framing**

Create `benchmarking/aws/runner/brokermetrics_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func TestBrokerMetrics_FrameSplit(t *testing.T) {
	const body = `###timestamp=1000
redpanda_kafka_request_bytes_total{topic="t1",redpanda_request="produce"} 1024
###timestamp=1010
redpanda_kafka_request_bytes_total{topic="t1",redpanda_request="produce"} 2048
`
	frames, err := parseBrokerFrames(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parseBrokerFrames: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("want 2 frames, got %d", len(frames))
	}
	if frames[0].UnixTime != 1000 || frames[1].UnixTime != 1010 {
		t.Errorf("frame timestamps wrong: %d, %d", frames[0].UnixTime, frames[1].UnixTime)
	}
	if !strings.Contains(frames[0].Body, "1024") {
		t.Errorf("frame 0 body missing metric: %q", frames[0].Body)
	}
}
```

- [ ] **Step 2: Run — expect build failure**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go test ./benchmarking/aws/runner -run TestBrokerMetrics_FrameSplit -v
```

Expected: `undefined: parseBrokerFrames`.

- [ ] **Step 3: Implement framing by delegating to the existing snapshot extractor**

Create `benchmarking/aws/runner/brokermetrics.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "io"

// parseBrokerFrames splits a redpanda-<vcpu>.txt dump into the same
// frame shape that prom.go's snapshot reader produces. We delegate to
// the existing private helper so a future schema change in the framing
// (timestamp marker format, etc.) only has to be made in one place.
func parseBrokerFrames(r io.Reader) ([]promSnapshot, error) {
	return parsePromSnapshots(r)
}
```

This task assumes `parsePromSnapshots` exists in `prom.go` as the framing helper inside `ParsePromStream`. If `prom.go`'s framing is currently inline inside `ParsePromStream`, extract it to a private helper as part of this task and add a one-line test on `prom.go`'s side to confirm `ParsePromStream` still works.

- [ ] **Step 4: Run tests — both should pass**

```bash
go test ./benchmarking/aws/runner -run "TestBrokerMetrics_FrameSplit|TestParsePromStream" -v
```

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/brokermetrics.go benchmarking/aws/runner/brokermetrics_test.go benchmarking/aws/runner/prom.go
git commit -m "feat(bench/aws/brokermetrics): reuse promSnapshot framing for redpanda dumps"
```

---

### Task 2: Extract per-topic byte counters

**Files:**
- Modify: `benchmarking/aws/runner/brokermetrics.go`
- Modify: `benchmarking/aws/runner/brokermetrics_test.go`

- [ ] **Step 1: Write failing tests for label-aware counter extraction**

Append to `brokermetrics_test.go`:

```go
func TestBrokerMetrics_ExtractTopicBytes(t *testing.T) {
	const body = `# HELP redpanda_kafka_request_bytes_total ...
# TYPE redpanda_kafka_request_bytes_total counter
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",topic="bench_sess1_postgres_cdc_connect"} 1.234e+09
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="consume",topic="bench_sess1_postgres_cdc_connect"} 5e+08
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",topic="bench_sess1_postgres_cdc_kc.public.orders"} 9.87e+08
`
	bytesByTopic, err := extractTopicProduceBytes(body)
	if err != nil {
		t.Fatalf("extractTopicProduceBytes: %v", err)
	}
	if got := bytesByTopic["bench_sess1_postgres_cdc_connect"]; got != 1.234e9 {
		t.Errorf("connect produce bytes = %v, want 1.234e9", got)
	}
	if got := bytesByTopic["bench_sess1_postgres_cdc_kc.public.orders"]; got != 9.87e8 {
		t.Errorf("KC produce bytes = %v, want 9.87e8", got)
	}
	// Consume bytes must NOT be in the produce map.
	if _, ok := bytesByTopic["__consume__bench_sess1_postgres_cdc_connect"]; ok {
		t.Error("consume bytes leaked into produce map")
	}
}

func TestBrokerMetrics_ExtractTopicBytes_IgnoresInternal(t *testing.T) {
	const body = `redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="_kc_configs"} 4096
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_connect"} 1000
`
	bytesByTopic, err := extractTopicProduceBytes(body)
	if err != nil {
		t.Fatalf("extractTopicProduceBytes: %v", err)
	}
	if _, ok := bytesByTopic["_kc_configs"]; ok {
		t.Error("internal topic _kc_configs should not appear in attribution map")
	}
	if got := bytesByTopic["bench_sess1_postgres_cdc_connect"]; got != 1000 {
		t.Errorf("bench topic missing; got %v", got)
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics_ExtractTopicBytes -v
```

Expected: `undefined: extractTopicProduceBytes`.

- [ ] **Step 3: Implement label parsing**

Append to `brokermetrics.go`:

```go
import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

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
//   name{k1="v1",k2="v2",...} value
// into the labels map and the value substring.
func splitLabeledMetric(line string) (map[string]string, string, bool) {
	open := strings.Index(line, "{")
	close := strings.Index(line, "}")
	if open < 0 || close < 0 || close < open {
		return nil, "", false
	}
	labelsRaw := line[open+1 : close]
	rest := strings.TrimSpace(line[close+1:])
	valueStr := rest
	if sp := strings.IndexAny(rest, " \t"); sp >= 0 {
		valueStr = rest[:sp]
	}
	labels := map[string]string{}
	// Hand-roll the label split — Prometheus label values can contain
	// commas inside quotes, but the redpanda exporter doesn't currently
	// emit any. If that changes, switch to a real text-format parser.
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
```

- [ ] **Step 4: Run tests — both should pass**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics_ExtractTopicBytes -v
```

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/brokermetrics.go benchmarking/aws/runner/brokermetrics_test.go
git commit -m "feat(bench/aws/brokermetrics): extract per-topic produce bytes from /public_metrics"
```

---

### Task 3: Per-topic throughput time series

**Files:**
- Modify: `benchmarking/aws/runner/brokermetrics.go`
- Modify: `benchmarking/aws/runner/brokermetrics_test.go`

- [ ] **Step 1: Write failing tests**

Append to `brokermetrics_test.go`:

```go
func TestBrokerMetrics_TopicSeries_DeltasOverFrames(t *testing.T) {
	const body = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="t1"} 0
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="t1"} 10485760
###timestamp=1020
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="t1"} 20971520
`
	series, err := ParseTopicSeries(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseTopicSeries: %v", err)
	}
	t1 := series["t1"]
	if t1 == nil {
		t.Fatal("topic t1 missing from series map")
	}
	// 3 frames → 2 deltas. First delta covers T=10s (10 MiB / 10s = 1 MiB/s).
	if len(t1) != 2 {
		t.Fatalf("expected 2 series points (one per inter-frame delta); got %d", len(t1))
	}
	if want := 1.0; t1[0].MBPerSec < want-0.01 || t1[0].MBPerSec > want+0.01 {
		t.Errorf("first delta MB/s = %f, want ~%f", t1[0].MBPerSec, want)
	}
	if t1[0].T != 10 {
		t.Errorf("first sample T = %d, want 10 (seconds since first frame)", t1[0].T)
	}
}

func TestBrokerMetrics_TopicSeries_HandlesCounterReset(t *testing.T) {
	// If a counter goes BACKWARDS between frames (broker restart) the
	// delta is non-meaningful — skip rather than report a negative rate.
	const body = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="t1"} 1000000
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="t1"} 500
`
	series, err := ParseTopicSeries(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseTopicSeries: %v", err)
	}
	if len(series["t1"]) != 0 {
		t.Errorf("reset-detected delta should be skipped; got %+v", series["t1"])
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics_TopicSeries -v
```

- [ ] **Step 3: Implement `ParseTopicSeries`**

Append to `brokermetrics.go`:

```go
// TopicPoint is one inter-frame throughput sample for a single topic.
type TopicPoint struct {
	T          int     `json:"t"`     // seconds since first frame
	MBPerSec   float64 `json:"mb_per_sec"`
	IntervalSec int    `json:"-"`     // for debugging; not serialized
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
```

- [ ] **Step 4: Run all brokermetrics tests**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics -v
```

Expected: all 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/brokermetrics.go benchmarking/aws/runner/brokermetrics_test.go
git commit -m "feat(bench/aws/brokermetrics): per-topic throughput time series with counter-reset filtering"
```

---

### Task 4: Engine attribution helper

**Files:**
- Modify: `benchmarking/aws/runner/brokermetrics.go`
- Modify: `benchmarking/aws/runner/brokermetrics_test.go`

- [ ] **Step 1: Write failing tests**

Append:

```go
func TestBrokerMetrics_AttributeByEngine_Postgres(t *testing.T) {
	series := map[string][]TopicPoint{
		"bench_sess1_postgres_cdc_connect": {
			{T: 10, MBPerSec: 50}, {T: 20, MBPerSec: 52},
		},
		"bench_sess1_postgres_cdc_kc.public.orders": {
			{T: 10, MBPerSec: 30}, {T: 20, MBPerSec: 31},
		},
		"bench_sess1_postgres_cdc_kc.public.shipments": {
			{T: 10, MBPerSec: 7}, {T: 20, MBPerSec: 8},
		},
		"some_unrelated_topic": {
			{T: 10, MBPerSec: 999},
		},
	}
	got, err := AttributeByEngine(series, "sess1", "postgres_cdc")
	if err != nil {
		t.Fatalf("AttributeByEngine: %v", err)
	}
	if len(got["connect"]) != 2 {
		t.Errorf("connect should have 2 points; got %d", len(got["connect"]))
	}
	if got["connect"][0].MBPerSec != 50 {
		t.Errorf("connect t=10 = %f, want 50", got["connect"][0].MBPerSec)
	}
	// KC has TWO topics (orders + shipments). At T=10 the engine total
	// is 30 + 7 = 37. At T=20 it's 31 + 8 = 39.
	if len(got["kafka_connect"]) != 2 {
		t.Errorf("kafka_connect should have 2 points; got %d", len(got["kafka_connect"]))
	}
	if got["kafka_connect"][0].MBPerSec != 37 {
		t.Errorf("kc T=10 sum = %f, want 37", got["kafka_connect"][0].MBPerSec)
	}
	if got["kafka_connect"][1].MBPerSec != 39 {
		t.Errorf("kc T=20 sum = %f, want 39", got["kafka_connect"][1].MBPerSec)
	}
}

func TestBrokerMetrics_AttributeByEngine_UnrelatedTopicsIgnored(t *testing.T) {
	series := map[string][]TopicPoint{
		"unrelated":                   {{T: 10, MBPerSec: 999}},
		"bench_other_session_connect": {{T: 10, MBPerSec: 100}},
	}
	got, err := AttributeByEngine(series, "sess1", "postgres_cdc")
	if err != nil {
		t.Fatalf("AttributeByEngine: %v", err)
	}
	if len(got["connect"]) != 0 || len(got["kafka_connect"]) != 0 {
		t.Errorf("unrelated topics leaked into attribution; got %+v", got)
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics_AttributeByEngine -v
```

- [ ] **Step 3: Implement `AttributeByEngine`**

Append to `brokermetrics.go`:

```go
// AttributeByEngine groups per-topic series into per-engine series for a
// given bench session. The mapping rules mirror the topic-naming
// conventions baked into Plan 2:
//
//   Connect → exactly one topic:  bench_<session>_<connector>_connect
//   KC      → many topics:        bench_<session>_<connector>_kc.<schema>.<table>
//                                 (Debezium prepends topic.prefix to a
//                                 per-table topic)
//
// KC's per-table series are summed point-wise on matching T values; if
// the per-topic series have ragged T values, missing values count as
// zero. (In practice all Plan 2 KC topics scrape at the same cadence, so
// the merge is straightforward.)
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
	// Collect KC topics: bare prefix OR prefix+"."<rest>.
	kcTopics := []string{}
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
	tSet := map[int]struct{}{}
	for _, t := range topics {
		for _, p := range series[t] {
			byT[p.T] += p.MBPerSec
			tSet[p.T] = struct{}{}
		}
	}
	ts := make([]int, 0, len(tSet))
	for t := range tSet {
		ts = append(ts, t)
	}
	// Sort ts; for short series an O(n²) selection sort is fine — but
	// use sort.Ints to keep it idiomatic.
	sort.Ints(ts)
	out := make([]TopicPoint, len(ts))
	for i, t := range ts {
		out[i] = TopicPoint{T: t, MBPerSec: byT[t]}
	}
	return out
}
```

Add `"sort"` to the imports.

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestBrokerMetrics -v
```

All 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/brokermetrics.go benchmarking/aws/runner/brokermetrics_test.go
git commit -m "feat(bench/aws/brokermetrics): AttributeByEngine sums KC per-table topics, dedupes by session"
```

---

### Task 5: `PointResult.BrokerMBPerSec` field

**Files:**
- Modify: `benchmarking/aws/runner/render.go`
- Modify: `benchmarking/aws/runner/render_test.go`
- Modify: `benchmarking/aws/runner/matrix.go`

- [ ] **Step 1: Add fields to `PointResult` and `SweepPoint`**

Currently:

```go
type PointResult struct {
	VCPU      int         `json:"vcpu"`
	Engine    string      `json:"engine"`
	Samples   []Sample    `json:"samples"`
	Summary   Summary     `json:"summary"`
	Anomalies []Anomaly   `json:"anomalies,omitempty"`
	Prom      []PromPoint `json:"prom,omitempty"`
}
```

Replace with:

```go
type PointResult struct {
	VCPU      int         `json:"vcpu"`
	Engine    string      `json:"engine"`
	Samples   []Sample    `json:"samples"`
	Summary   Summary     `json:"summary"`
	Anomalies []Anomaly   `json:"anomalies,omitempty"`
	Prom      []PromPoint `json:"prom,omitempty"`

	// BrokerSeries is the broker-side throughput attributed to this engine
	// at this vCPU point. For Connect, it's a cross-check against the
	// rolling-stats-derived Summary. For KC (which has no rolling-stats
	// line to parse), Summary IS derived from this series — see Task 6.
	BrokerSeries []TopicPoint `json:"broker_series,omitempty"`
}
```

Do the same for `SweepPoint` in `matrix.go` (add `BrokerSeries []TopicPoint` after `Prom`).

- [ ] **Step 2: Update `render_test.go`'s `sampleResult` literal**

Existing literal already has `Engine: "connect"`. Don't change the field set here — `BrokerSeries` is optional with `omitempty`, so leaving it unset is correct for the existing fixture.

- [ ] **Step 3: Build + test**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go build ./benchmarking/aws/runner/...
go test ./benchmarking/aws/runner/...
gofmt -l benchmarking/aws/runner/
```

All clean.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/matrix.go
git commit -m "feat(bench/aws): PointResult/SweepPoint gain BrokerSeries field"
```

---

### Task 6: Fetch + parse `redpanda-<vcpu>.txt` in matrix runner

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go`
- Modify: `benchmarking/aws/runner/matrix_test.go`

The runner currently fetches `sweep-<vcpu>.log` for Connect and skips fetch for KC. After Task 6 it also fetches `redpanda-<vcpu>.txt` ONCE per vcpu (the file already contains both engines' bytes, since they share brokers) and slices it per engine.

- [ ] **Step 1: Write failing tests**

Append to `matrix_test.go`:

```go
func TestMatrixRun_PopulatesBrokerSeriesForBothEngines(t *testing.T) {
	const sessionID = "sess1"
	const connector = "postgres_cdc"

	// Three frames covering a 20s window: bytes rise on both topics so
	// both engines get non-empty BrokerSeries.
	const rpDump = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_connect"} 0
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_kc.public.orders"} 0
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_connect"} 524288000
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_kc.public.orders"} 314572800
###timestamp=1020
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_connect"} 1048576000
redpanda_kafka_request_bytes_total{redpanda_request="produce",topic="bench_sess1_postgres_cdc_kc.public.orders"} 629145600
`

	connectLog := makeLog(180, 50)
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID):    connectLog,
			fmt.Sprintf("runs/%s/redpanda-1.txt", sessionID): rpDump,
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:                   ssm,
		LogFetcher:            fetcher,
		RunnerInstance:        "i-runner",
		Bucket:                "b",
		SessionID:             sessionID,
		Engines:               []string{"connect", "kafka_connect"},
		KCConnectorName:       "bench_postgres_cdc",
		KCConnectorConfigJSON: `{"connector.class":"x"}`,
	}
	// Scenario connector is needed for AttributeByEngine.
	mr.ScenarioConnector = connector
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)

	connectPt := points[0]
	require.Equal(t, "connect", connectPt.Engine)
	require.NotEmpty(t, connectPt.BrokerSeries, "connect BrokerSeries must be populated from redpanda-1.txt")
	// Connect produced 500 MiB in 10s → 50 MiB/s
	require.InDelta(t, 50.0, connectPt.BrokerSeries[0].MBPerSec, 0.1)

	kcPt := points[1]
	require.Equal(t, "kafka_connect", kcPt.Engine)
	require.NotEmpty(t, kcPt.BrokerSeries, "kc BrokerSeries must be populated")
	// KC produced 300 MiB in 10s → 30 MiB/s
	require.InDelta(t, 30.0, kcPt.BrokerSeries[0].MBPerSec, 0.1)
}
```

- [ ] **Step 2: Add `ScenarioConnector` field to `MatrixRunner`**

The matrix runner needs to know the connector name (`postgres_cdc`, `mysql_cdc`) to call `AttributeByEngine`. Add the field:

```go
type MatrixRunner struct {
	// ... existing fields ...

	// ScenarioConnector is the connector name from the scenario YAML
	// (e.g. "postgres_cdc"). Passed through from runBench so the
	// broker-side attribution helper can identify per-engine topics.
	ScenarioConnector string
}
```

And in `runBench` (main.go around line 220-230 MatrixRunner literal):

```go
mr := &MatrixRunner{
    // ... existing ...
    ScenarioConnector:     s.Connector,
}
```

- [ ] **Step 3: Add the fetch + parse to `Run`**

In `matrix.go`, find the existing per-engine fetch block (around lines 138-155 after Plan 2). Replace it with:

```go
            var samples []Sample
            var rawLog []byte
            if engine == "connect" {
                var err error
                rawLog, err = m.fetchLog(ctx, n)
                if err != nil {
                    return nil, fmt.Errorf("fetch log at %d vCPU (%s): %w", n, engine, err)
                }
                samples = parseAndTrim(rawLog, warmup)
            }
            promPts := m.fetchProm(ctx, n)

            // Broker-side: fetched once per vCPU because the redpanda dump
            // covers all topics (both engines' bytes). Per-engine slicing
            // happens below via AttributeByEngine.
            var brokerSeries []TopicPoint
            if engine == "connect" {
                brokerByEngine, err := m.fetchBrokerSeries(ctx, n)
                if err == nil {
                    m.cachedBrokerByEngine = brokerByEngine
                }
            }
            if m.cachedBrokerByEngine != nil {
                brokerSeries = m.cachedBrokerByEngine[engine]
            }

            summary := Summarise(samples)
            // KC: derive summary from broker-side bytes since there's no
            // rolling-stats log to parse.
            if engine == "kafka_connect" {
                summary = SummariseTopicPoints(brokerSeries)
            }
            anomalies := DetectAnomaliesWithProm(samples, summary.MedianMBPerSec, promPts)
            out = append(out, SweepPoint{
                VCPU:         n,
                Engine:       engine,
                Samples:      samples,
                Summary:      summary,
                Anomalies:    anomalies,
                Prom:         promPts,
                BrokerSeries: brokerSeries,
            })
            // ... rest as before ...
```

Two new methods needed on `MatrixRunner`:

```go
// cachedBrokerByEngine is the engine-attributed broker series for the
// CURRENT vCPU iteration. It's stashed on the connect pass (which
// always runs first in any Engines list that includes it) and read
// back by the kafka_connect pass at the same vCPU. Reset to nil at
// the start of each new vCPU.
//
// This is a struct field rather than a method-local because the engine
// loop's per-engine iterations need to share the same fetched data.
cachedBrokerByEngine map[string][]TopicPoint

func (m *MatrixRunner) fetchBrokerSeries(ctx context.Context, vcpu int) (map[string][]TopicPoint, error) {
    if m.LogFetcher == nil {
        return nil, fmt.Errorf("LogFetcher not configured")
    }
    key := fmt.Sprintf("runs/%s/redpanda-%d.txt", m.SessionID, vcpu)
    body, err := m.LogFetcher.Fetch(ctx, m.Bucket, key)
    if err != nil {
        fmt.Fprintf(stdout, "[bench] fetch broker metrics (non-fatal): %v\n", err)
        return nil, err
    }
    defer body.Close()
    series, err := ParseTopicSeries(body)
    if err != nil {
        fmt.Fprintf(stdout, "[bench] parse broker metrics (non-fatal): %v\n", err)
        return nil, err
    }
    return AttributeByEngine(series, m.SessionID, m.ScenarioConnector)
}
```

Also: at the START of each vCPU outer-loop iteration (before the engine inner loop), reset:

```go
for _, n := range cpuPoints {
    m.cachedBrokerByEngine = nil
    for _, engine := range engines {
        // ... existing ...
    }
}
```

NOTE: If `Engines` is configured `["kafka_connect"]` only (no Connect pass), the broker fetch never fires. Adjust the fetch trigger so it fires on the FIRST engine of each vCPU iteration, not specifically on `connect`. A small refactor:

```go
if m.cachedBrokerByEngine == nil {
    brokerByEngine, err := m.fetchBrokerSeries(ctx, n)
    if err == nil {
        m.cachedBrokerByEngine = brokerByEngine
    }
}
```

- [ ] **Step 4: Implement `SummariseTopicPoints`**

In `stats.go`, append:

```go
// SummariseTopicPoints derives a Summary from a broker-side TopicPoint
// stream. Mirrors Summarise(samples) — median, p5, p95, peak — but
// works against the per-engine broker series for engines that don't
// emit a rolling-stats log we can parse.
func SummariseTopicPoints(pts []TopicPoint) Summary {
    if len(pts) == 0 {
        return Summary{}
    }
    rates := make([]float64, len(pts))
    for i, p := range pts {
        rates[i] = p.MBPerSec
    }
    sort.Float64s(rates)
    return Summary{
        MedianMBPerSec: percentile(rates, 0.50),
        P5MBPerSec:     percentile(rates, 0.05),
        P95MBPerSec:    percentile(rates, 0.95),
        PeakMBPerSec:   rates[len(rates)-1],
        // Msg/sec fields stay zero — broker bytes don't give us message
        // count without an additional metric.
    }
}
```

Use whatever `percentile` helper already exists in `stats.go`. If there isn't one, lift the logic out of `Summarise`'s body into a helper and reuse it.

- [ ] **Step 5: Run tests**

```bash
go test ./benchmarking/aws/runner/...
```

All pass. The new `TestMatrixRun_PopulatesBrokerSeriesForBothEngines` passes; existing matrix tests still pass.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go benchmarking/aws/runner/stats.go benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): fetch + slice broker metrics; KC Summary derived from broker bytes"
```

---

### Task 7: Verify Connect's broker number is a sanity-cross-check

**Files:**
- Modify: `benchmarking/aws/runner/render.go` (markdown view + template)
- Modify: `benchmarking/aws/runner/render_test.go`

After Task 6 Connect's `Summary.MedianMBPerSec` comes from rolling-stats (unchanged from Plan 1), and `BrokerSeries` is populated as a separate field. Task 7 surfaces the broker number alongside Connect's rolling-stats number so the operator can spot disagreements (e.g., Connect's benchmark processor counts post-batching bytes, broker counts wire bytes — should agree within a few percent).

- [ ] **Step 1: Add a `BrokerMedianMB` derived value to markdown rendering**

In `render.go`, where `AppendMarkdown` builds the rows, compute:

```go
var brokerMedian float64
if len(p.BrokerSeries) > 0 {
    rates := make([]float64, len(p.BrokerSeries))
    for i, b := range p.BrokerSeries {
        rates[i] = b.MBPerSec
    }
    sort.Float64s(rates)
    brokerMedian = rates[len(rates)/2]
}
rows[i] = markdownRow{
    VCPU:           p.VCPU,
    Engine:         p.Engine, // NEW
    MedianMB:       fmt.Sprintf("%12.0f", p.Summary.MedianMBPerSec),
    BrokerMedianMB: fmt.Sprintf("%12.0f", brokerMedian), // NEW
    // ... rest unchanged ...
}
```

- [ ] **Step 2: Update the markdown template**

Add an `engine` column and a `broker MB/s` column to `resultMarkdownTmpl`. Adjust the header row spacing accordingly. Reference the existing fixture in `TestAppendMarkdown` for the exact alignment style.

- [ ] **Step 3: Update fixture**

`render_test.go::sampleResult` already has `Engine: "connect"`. Add a second `PointResult{}` with `Engine: "kafka_connect"` so the markdown test exercises the dual-engine shape. Update the `require.Contains` assertions in `TestAppendMarkdown` to match the new row layout — at minimum, assert that both `"connect"` and `"kafka_connect"` labels appear in the rendered output.

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestAppendMarkdown -v
```

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/render_test.go
git commit -m "feat(bench/aws): markdown gains engine column + broker-MB/s cross-check"
```

---

### Task 8: Delta column for Connect-vs-KC comparison

**Files:**
- Modify: `benchmarking/aws/runner/render.go`
- Modify: `benchmarking/aws/runner/render_test.go`

After Task 7 the markdown has one row per `(vcpu, engine)`. Task 8 adds a Δ column that, for each `(vcpu, kafka_connect)` row, shows `(KC median - Connect median)` and `(KC median / Connect median)` so the reader can see the per-engine gap at a glance.

- [ ] **Step 1: Group points by vcpu before rendering**

In `AppendMarkdown`, replace the linear loop over `r.Points` with:

```go
type vcpuGroup struct {
    vcpu       int
    byEngine   map[string]PointResult
}
groups := map[int]*vcpuGroup{}
order := []int{}
for _, p := range r.Points {
    g, ok := groups[p.VCPU]
    if !ok {
        g = &vcpuGroup{vcpu: p.VCPU, byEngine: map[string]PointResult{}}
        groups[p.VCPU] = g
        order = append(order, p.VCPU)
    }
    g.byEngine[p.Engine] = p
}
```

Then for each vcpu group, emit one row per engine plus (when both engines present) a delta row OR a Δ column inline.

- [ ] **Step 2: Decide row vs column for the delta**

Two reasonable shapes — pick column for compactness:

```
| vCPU | engine        | median MB/s | broker MB/s | Δ vs Connect |
|------|---------------|-------------|-------------|--------------|
|    1 | connect       |          25 |          24 |              |
|    1 | kafka_connect |          18 |          18 |       -7 (-28%) |
```

Implementation: when rendering a `kafka_connect` row, look up the matching `connect` row from the group and compute the delta. When only one engine is present, the column is blank.

- [ ] **Step 3: Tests**

Update `TestAppendMarkdown` to:
- Use a 2-vcpu × 2-engine fixture
- Assert delta string appears in the rendered output (e.g. `strings.Contains(out, "-28%")`)
- Assert connect rows have empty delta column

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestAppendMarkdown -v
```

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/render_test.go
git commit -m "feat(bench/aws): markdown table groups by vCPU with per-engine delta column"
```

---

### Task 9: `SUMMARY.md` engine-aware row

**Files:**
- Modify: `benchmarking/aws/runner/summary.go`
- Modify: `benchmarking/aws/runner/summary_test.go`

`RefreshSummary` walks `docs/benchmark-results/results/**/<scenario>/<latest>.json` and writes a one-row-per-scenario summary into `docs/benchmark-results/SUMMARY.md`. After Plan 3 each result has TWO engines; the summary row should show both.

- [ ] **Step 1: Decide on row shape**

Reasonable:

```
| Scenario           | best vCPU (connect) | Connect MB/s | KC MB/s | gap |
```

Where "best vCPU" is the vCPU at which Connect's MedianMBPerSec is maximal in the most-recent JSON. The KC and gap columns reflect the same vCPU.

- [ ] **Step 2: Update walker**

In `summary.go`'s row-deriving logic (look for the function that turns one `Result` into a summary row), replace the single-engine point selection with engine-aware logic:

```go
// Split points by engine.
var connectPts, kcPts []PointResult
for _, p := range r.Points {
    switch p.Engine {
    case "connect":
        connectPts = append(connectPts, p)
    case "kafka_connect":
        kcPts = append(kcPts, p)
    }
}
// Best Connect point = max median across vCPUs (existing behavior).
var bestConnect PointResult
for _, p := range connectPts {
    if p.Summary.MedianMBPerSec > bestConnect.Summary.MedianMBPerSec {
        bestConnect = p
    }
}
// Matching KC point at the same vCPU (may be absent if KC didn't run).
var matchingKC PointResult
for _, p := range kcPts {
    if p.VCPU == bestConnect.VCPU {
        matchingKC = p
        break
    }
}
// Gap: positive when Connect is faster, blank when KC didn't run.
gapStr := ""
if matchingKC.Engine != "" {
    diff := bestConnect.Summary.MedianMBPerSec - matchingKC.Summary.MedianMBPerSec
    pct := 100.0 * diff / bestConnect.Summary.MedianMBPerSec
    gapStr = fmt.Sprintf("%+.0f MB/s (%+.0f%%)", diff, pct)
}
```

Then thread `bestConnect`, `matchingKC`, and `gapStr` into whatever row-rendering template the file currently uses.

- [ ] **Step 3: Update summary_test fixtures**

Each test fixture for `RefreshSummary` needs at least one KC-engine point alongside the existing Connect point.

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestRefreshSummary -v
```

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go
git commit -m "feat(bench/aws): SUMMARY.md row shows Connect AND KC, plus gap"
```

---

### Task 10: Reorder Result.Points so engine-aware markdown stays readable

**Files:**
- Modify: `benchmarking/aws/runner/main.go`

Currently `runBench` appends `SweepPoint` → `PointResult` in the order they were produced. With engine-inner loop the order is `(1, connect), (1, kc), (2, connect), (2, kc), ...`. That's already what Task 8's grouping expects. Verify and document; no code change needed unless the matrix runner's order changes.

- [ ] **Step 1: Add a regression test**

In `render_test.go`, add an assertion that `AppendMarkdown` accepts both `[connect, kc, connect, kc]` and `[connect, connect, kc, kc]` orderings (since groupBy uses a map, order shouldn't matter, but pin it).

```go
func TestAppendMarkdown_PointOrderingIsRobust(t *testing.T) {
    // Same data, two orderings — should produce the same body.
    r1 := sampleDualEngineResult()
    r2 := sampleDualEngineResult()
    // Swap r2 so it's [connect_1, connect_2, kc_1, kc_2] instead of interleaved.
    sort.SliceStable(r2.Points, func(i, j int) bool {
        return r2.Points[i].Engine < r2.Points[j].Engine
    })

    target1 := t.TempDir() + "/r1.md"
    target2 := t.TempDir() + "/r2.md"
    require.NoError(t, AppendMarkdown(target1, r1, "desc"))
    require.NoError(t, AppendMarkdown(target2, r2, "desc"))
    b1, _ := os.ReadFile(target1)
    b2, _ := os.ReadFile(target2)
    require.Equal(t, string(b1), string(b2),
        "markdown output should be ordering-independent within a vCPU group")
}
```

- [ ] **Step 2: Run + commit**

```bash
go test ./benchmarking/aws/runner -run TestAppendMarkdown_PointOrderingIsRobust -v
git add benchmarking/aws/runner/render_test.go
git commit -m "test(bench/aws): markdown rendering robust against point-order permutations"
```

---

### Task 11: `DetectCrossEngineAnomalies`

**Files:**
- Create: `benchmarking/aws/runner/crossengine.go`
- Create: `benchmarking/aws/runner/crossengine_test.go`

Cross-engine anomaly = at a given vCPU, the two engines' medians differ by more than a configurable ratio. This is in ADDITION to the existing per-engine anomaly detector (which flags within-pass dips).

- [ ] **Step 1: Write tests**

Create `benchmarking/aws/runner/crossengine_test.go`:

```go
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
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./benchmarking/aws/runner -run TestDetectCrossEngineAnomalies -v
```

Expected: `undefined: DetectCrossEngineAnomalies`, `undefined: CrossEngineAnomaly`.

- [ ] **Step 3: Implement**

Create `benchmarking/aws/runner/crossengine.go`:

```go
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
```

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestDetectCrossEngineAnomalies -v
```

All 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/crossengine.go benchmarking/aws/runner/crossengine_test.go
git commit -m "feat(bench/aws): DetectCrossEngineAnomalies flags >threshold engine divergence per vCPU"
```

---

### Task 12: Surface cross-engine anomalies in markdown

**Files:**
- Modify: `benchmarking/aws/runner/render.go`
- Modify: `benchmarking/aws/runner/render_test.go`
- Modify: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Add `CrossEngineAnomalies` to `Result`**

In `render.go`:

```go
type Result struct {
    // ... existing fields ...
    CrossEngineAnomalies []CrossEngineAnomaly `json:"cross_engine_anomalies,omitempty"`
}
```

- [ ] **Step 2: Populate it in `runBench`**

In `main.go`, after both engines' points are gathered:

```go
var connectPts, kcPts []PointResult
for _, p := range result.Points {
    switch p.Engine {
    case "connect":
        connectPts = append(connectPts, p)
    case "kafka_connect":
        kcPts = append(kcPts, p)
    }
}
result.CrossEngineAnomalies = DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
```

Threshold `2.0` is hardcoded — operators who want a different threshold can post-process the result JSON.

- [ ] **Step 3: Surface in the markdown template**

Append a section to `resultMarkdownTmpl` (after the anomalies-per-engine table that already exists):

```markdown
{{ if .CrossEngineAnomalies }}
### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
{{ range .CrossEngineAnomalies }}| {{ .VCPU }} | {{ .FasterEngine }} | {{ .SlowerEngine }} | {{ printf "%.2fx" .Ratio }} | {{ printf "%.0f" .FasterMBPerS }} | {{ printf "%.0f" .SlowerMBPerS }} |
{{ end }}
{{ end }}
```

The `{{ if .CrossEngineAnomalies }}` gate means single-engine runs (e.g. `--engines=connect`) don't get an empty divergence section.

- [ ] **Step 4: Tests**

In `render_test.go`, add a fixture with one cross-engine anomaly and assert the markdown contains its rendered row:

```go
func TestAppendMarkdown_RendersCrossEngineDivergence(t *testing.T) {
    r := sampleDualEngineResult()
    r.CrossEngineAnomalies = []CrossEngineAnomaly{{
        VCPU: 4, FasterEngine: "connect", SlowerEngine: "kafka_connect",
        Ratio: 3.2, FasterMBPerS: 128, SlowerMBPerS: 40,
    }}
    target := t.TempDir() + "/r.md"
    require.NoError(t, AppendMarkdown(target, r, "desc"))
    body, _ := os.ReadFile(target)
    require.Contains(t, string(body), "Cross-engine divergence")
    require.Contains(t, string(body), "3.20x")
}
```

- [ ] **Step 5: Run + commit**

```bash
go test ./benchmarking/aws/runner -run TestAppendMarkdown -v
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/render_test.go benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): surface cross-engine divergence in markdown report"
```

---

### Task 13: Cross-engine anomalies in `SUMMARY.md`

**Files:**
- Modify: `benchmarking/aws/runner/summary.go`
- Modify: `benchmarking/aws/runner/summary_test.go`

If a scenario's latest result has cross-engine anomalies, `SUMMARY.md` should flag the scenario row visually (e.g. append " ⚠" or `**(diverges)**` to the scenario name) so a reader scanning the summary can tell at a glance.

- [ ] **Step 1: Add the divergence indicator to row rendering**

In `summary.go`, in the same row-building function from Task 9, after `gapStr` is computed:

```go
// Plan 3: append a ⚠ marker if the latest result flagged divergence.
scenarioLabel := r.Scenario
if len(r.CrossEngineAnomalies) > 0 {
    scenarioLabel += " ⚠"
}
```

Use `scenarioLabel` (instead of the raw `r.Scenario`) in the rendered row.

- [ ] **Step 2: Test fixture**

Append to `summary_test.go`:

```go
func TestRefreshSummary_FlagsDivergentScenarios(t *testing.T) {
    dir := t.TempDir()
    // Write a fake result.json containing cross_engine_anomalies.
    resultPath := filepath.Join(dir, "postgres_cdc", "postgres-orders-cdc", "2026-05-19T14-02-11Z.json")
    require.NoError(t, os.MkdirAll(filepath.Dir(resultPath), 0o755))
    raw, _ := json.Marshal(&Result{
        Scenario:    "postgres/orders-cdc",
        StartedAt:   time.Now(),
        FinishedAt:  time.Now(),
        Points: []PointResult{
            {VCPU: 1, Engine: "connect",       Summary: Summary{MedianMBPerSec: 100}},
            {VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 30}},
        },
        CrossEngineAnomalies: []CrossEngineAnomaly{{VCPU: 1, FasterEngine: "connect", SlowerEngine: "kafka_connect", Ratio: 3.33}},
    })
    require.NoError(t, os.WriteFile(resultPath, raw, 0o644))

    summaryPath := filepath.Join(dir, "SUMMARY.md")
    require.NoError(t, os.WriteFile(summaryPath, []byte("existing content\n"), 0o644))
    require.NoError(t, RefreshSummary(summaryPath, dir, time.Now()))

    body, _ := os.ReadFile(summaryPath)
    require.Contains(t, string(body), "postgres/orders-cdc ⚠",
        "divergent scenario should be marked with ⚠")
}
```

- [ ] **Step 3: Commit**

```bash
go test ./benchmarking/aws/runner -run TestRefreshSummary -v
git add benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go
git commit -m "feat(bench/aws): SUMMARY.md flags scenarios with cross-engine divergence"
```

---

### Task 14: Scheduler parity — drop `chrt --fifo 50` from Connect

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go` (`renderBenchScript`)
- Modify: `benchmarking/aws/runner/matrix_test.go`

Plan 2 dropped `chrt --fifo 50` from KC's bench script (it deadlocked the JVM under single-core taskset). For fairness, Connect should drop it too — otherwise Connect runs under a different scheduling policy than KC and the comparison isn't apples-to-apples.

- [ ] **Step 1: Edit `renderBenchScript`**

In `matrix.go`, find:

```go
fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s >"$LOG" 2>&1 &`,
    cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
```

Replace with:

```go
// chrt removed for scheduler parity with KC (Plan 2 dropped --fifo on
// the KC side because it deadlocked the JVM under single-core taskset).
// taskset alone gives us CPU isolation; SCHED_OTHER is what KC uses.
// See docs/superpowers/plans/2026-05-22-kafka-connect-plan-3-reporting.md.
fmt.Sprintf(`taskset -c 2-%d env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s >"$LOG" 2>&1 &`,
    cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
```

- [ ] **Step 2: Update tests**

`matrix_test.go` may assert that the script contains `chrt --fifo 50`. Find and remove that assertion (or invert: assert that the script does NOT contain `chrt`). Existing tests asserting `taskset -c 2-N` still pass.

- [ ] **Step 3: Run + commit**

```bash
go test ./benchmarking/aws/runner -run TestRenderBenchScript -v
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go
git commit -m "fix(bench/aws): drop chrt --fifo 50 from Connect for scheduler parity with KC"
```

---

### Task 15: Memory parity — KC heap as fraction of memLimit

**Files:**
- Modify: `benchmarking/aws/runner/kcscript.go`
- Modify: `benchmarking/aws/runner/kcscript_test.go`

Connect's `GOMEMLIMIT=<N>GiB` is a soft total-memory cap on the Go runtime. KC's `-Xmx<N>g` is heap only — JVM Metaspace, code cache, direct buffers, and native memory live outside `-Xmx`. At equal `N`, KC actually uses ~1.3-1.5× as much RSS as Connect, which biases the comparison.

The cheap fix: set KC's `-Xmx` to a fraction of the budget so total RSS stays in the same envelope.

- [ ] **Step 1: Decide the fraction**

Use 0.75. JVM overhead for KC + Debezium + Aiven plugins is ~25% of total RSS in practice. Operators who want a different fraction can edit `kcscript.go` (or, if we want, plumb a flag — but a constant is fine for now).

- [ ] **Step 2: Edit `kcscript.go`**

Find:

```go
fmt.Sprintf(`taskset -c 2-%d env KAFKA_HEAP_OPTS=-Xmx%dg /opt/kafka/bin/connect-distributed.sh /opt/kafka-connect/worker.properties >"$KC_LOG" 2>&1 &`,
    cpusetHi, a.MemLimitGiB),
```

Replace with:

```go
// KC's -Xmx caps heap only; JVM overhead (Metaspace, code cache,
// direct buffers, native code) typically adds ~25-33% on top. Set
// heap to floor(0.75 * memLimit) so the total RSS budget matches
// Connect's GOMEMLIMIT. If MemLimitGiB is 1, heap floors to 1 GiB
// (too aggressive a discount makes the JVM thrash).
kcHeapGiB := a.MemLimitGiB * 3 / 4
if kcHeapGiB < 1 {
    kcHeapGiB = 1
}
```

Then update the format string:

```go
fmt.Sprintf(`taskset -c 2-%d env KAFKA_HEAP_OPTS=-Xmx%dg /opt/kafka/bin/connect-distributed.sh /opt/kafka-connect/worker.properties >"$KC_LOG" 2>&1 &`,
    cpusetHi, kcHeapGiB),
```

`kcHeapGiB` needs to be declared before the `lines := []string{...}` block (since the lines are computed top-to-bottom).

- [ ] **Step 3: Update tests**

`kcscript_test.go::TestRenderKCBenchScript_PinsCoresAndHeap` currently asserts `-Xmx4g` for `MemLimitGiB: 4`. Update to assert `-Xmx3g` (4 × 3/4 = 3) and add a test for `MemLimitGiB: 2` asserting `-Xmx1g`.

- [ ] **Step 4: Run + commit**

```bash
go test ./benchmarking/aws/runner -run TestRenderKCBenchScript -v
git add benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/kcscript_test.go
git commit -m "fix(bench/aws/kc): heap = 0.75 × memLimit so total JVM RSS matches Connect GOMEMLIMIT"
```

---

### Task 16: README Plan 3 section

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Append a section**

Append to the bottom of `benchmarking/aws/README.md`:

```markdown
## Reporting + cross-engine anomalies (Plan 3, 2026-05-22)

KC's throughput now comes from broker-side metrics (Plan 1 captures
`redpanda-<vcpu>.txt` once per sweep point; Plan 3 parses it). The
markdown report writes one row per `(vcpu, engine)` and a Δ column
showing the per-engine gap; the SUMMARY.md row shows both engines'
medians at the best Connect vCPU. Scenarios where the two engines
diverge by more than 2× at any vCPU get a "Cross-engine divergence"
section listing the offending vCPUs.

**Scheduler/memory fairness fixes:**

- Connect no longer runs under `chrt --fifo 50`. The KC side dropped
  it in Plan 2 (it deadlocked the JVM under single-core taskset);
  Plan 3 drops it from Connect too so both engines run under
  SCHED_OTHER. Numbers from Plan 1/2 are NOT comparable to Plan 3+
  numbers because of this scheduler change.
- KC's `-Xmx` is set to 0.75 × memLimitGiB (floor 1 GiB) so JVM
  overhead (Metaspace, code cache, direct buffers) fits inside the
  memory budget the way Connect's `GOMEMLIMIT` already does. Equal-
  budget runs now have equal-RSS engines.

**Result-JSON shape change:** `PointResult` gains `broker_series`
(per-engine attributed throughput from Redpanda's metrics). `Result`
gains `cross_engine_anomalies` (per-vCPU divergence flags).
```

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): document Plan 3 (broker-side attribution + fairness fixes)"
```

---

### Task 17: AWS smoke test (operator-driven)

**Files:** (no code changes; manual verification)

The smoke gate for Plan 3 — same shape as Plan 2's Task 15, but verifies the new reporting end-to-end.

- [ ] **Step 1: Validate the scenario YAML**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws
unset AWS_PROFILE && aws-vault exec AWSAdministratorAccess-605419575229 -- task aws:validate scenario=postgres/orders-cdc
```

Expected: `scenario postgres-orders-cdc OK`.

- [ ] **Step 2: Run a small smoke (1 vCPU, 15m window) with both engines**

Reuse `/tmp/plan2-smoke.yaml` (left from Plan 2 work) or recreate. Run:

```bash
unset AWS_PROFILE && aws-vault exec AWSAdministratorAccess-605419575229 -- \
  env REDPANDA_LICENSE_FILEPATH=/Users/prakhar.garg/Documents/connect_prakhar/rpcn.license \
  go run ./benchmarking/aws/runner bench \
  --scenario=/tmp/plan2-smoke.yaml --repo-root=. --keep-on-fail
```

Expected wall-clock: ~25-35 min for two engines at 1 vCPU. Cost: ~$1.50.

- [ ] **Step 3: Verify the result JSON has the new shape**

```bash
LATEST=$(ls -t benchmarking/aws/results/postgres_cdc/postgres-orders-cdc-plan2-smoke/ | head -1)
jq '{
  points: [.points[] | {vcpu, engine, median: .summary.median_mb_per_sec, has_broker_series: (.broker_series | length > 0)}],
  cross_engine_count: (.cross_engine_anomalies | length // 0)
}' benchmarking/aws/results/postgres_cdc/postgres-orders-cdc-plan2-smoke/$LATEST
```

Expected:
- 2 points at vCPU 1: one connect, one kafka_connect
- Both points have non-empty `broker_series`
- KC's `median_mb_per_sec` is non-zero (this is the headline Plan 3 win)
- `cross_engine_count` is either 0 or 1 depending on whether the engines actually diverged

- [ ] **Step 4: Verify the markdown**

```bash
grep -A 20 "Cross-engine divergence\|engine.*MB/s.*broker" docs/benchmark-results/postgres.md | tail -30
```

Expected: the dual-engine table is present, the Δ column shows a delta number, and (if divergent) the divergence section is rendered.

- [ ] **Step 5: Tear down**

```bash
unset AWS_PROFILE && aws-vault exec AWSAdministratorAccess-605419575229 -- \
  go run ./benchmarking/aws/runner down --scenario=/tmp/plan2-smoke.yaml --repo-root=.
```

(With the `bench_session_id="""` default from the Plan 2 punch list, this should work without -var passing.)

- [ ] **Step 6: Commit a brief note to memory or the README** with the session id, headline numbers, and any surprise.

If steps 3-4 FAIL: diagnose by re-running with `--keep-on-fail` and SSM-probing the runner. Common Plan 3 failure modes:

- **Broker scrape file missing:** Plan 1 wrote it conditionally on `RedpandaMetricsEndpoint`. Verify that's set in the bench's MatrixRunner config.
- **AttributeByEngine returns empty maps:** topic naming drifted. Confirm `bench_session_id` is correctly substituted (Plan 2 punch list #6 made this robust).
- **Markdown template parse error:** the new template uses `{{ printf ... }}` blocks — verify all closing tags balance.

---

## Verification checklist (Plan 3 acceptance)

- [ ] `go build ./benchmarking/aws/runner/...` clean
- [ ] `go test ./benchmarking/aws/runner/...` green
- [ ] `task aws:validate scenario=postgres/orders-cdc` passes
- [ ] Smoke (Task 17) produces a result JSON where KC's `Summary.MedianMBPerSec > 0` and both engines have `broker_series` populated
- [ ] Markdown report shows both engines with a Δ column AND a cross-engine divergence section when applicable
- [ ] SUMMARY.md row shows Connect AND KC medians side-by-side
- [ ] Scheduler change documented in README; Plan 1/2 numbers explicitly marked as not-comparable in the doc

---

## Out of scope (Plan 4 territory)

- Sink-side benchmarks (Iceberg, Confluent S3, JDBC sink). Plan 4.
- Multi-vCPU sweeps beyond 1/2/4/8 — choosing the canonical CPU points is a design discussion separate from the reporting machinery.
- Message-count-based fairness (KC's throughput uses byte counters because Redpanda exposes those cheaply; if we want msg/sec for KC we need a different metric).
- HDR-level histograms or latency tracking — Plan 3 reports throughput only.
