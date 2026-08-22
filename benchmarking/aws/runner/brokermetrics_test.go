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
redpanda_kafka_request_bytes_total{redpanda_topic="t1",redpanda_request="produce"} 1024
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_topic="t1",redpanda_request="produce"} 2048
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

func TestBrokerMetrics_ExtractTopicBytes(t *testing.T) {
	const body = `# HELP redpanda_kafka_request_bytes_total ...
# TYPE redpanda_kafka_request_bytes_total counter
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 1.234e+09
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="consume",redpanda_topic="bench_sess1_postgres_cdc_connect"} 5e+08
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 9.87e+08
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
	// Consume bytes must NOT be in the produce map (we deliberately don't
	// scrape consume-side; the bench attributes throughput to the engine
	// that wrote the bytes).
	if len(bytesByTopic) != 2 {
		t.Errorf("expected exactly 2 topics (both produce-side); got %d: %+v", len(bytesByTopic), bytesByTopic)
	}
}

func TestBrokerMetrics_SumsAcrossBrokers(t *testing.T) {
	// Simulates a single frame where all 3 brokers reported their bytes
	// for the same topic. Broker 0 leads partition 0 (1 MiB), broker 1
	// leads partition 1 (2 MiB), broker 2 leads partition 2 (3 MiB).
	// Total cluster produce bytes for the topic = 6 MiB. The scraper
	// concatenates each broker's /public_metrics output into one frame
	// body, so the same topic line appears three times.
	const body = `redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="t1"} 1048576
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="t1"} 2097152
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="t1"} 3145728
`
	bytesByTopic, err := extractTopicProduceBytes(body)
	if err != nil {
		t.Fatalf("extractTopicProduceBytes: %v", err)
	}
	want := 1048576.0 + 2097152.0 + 3145728.0
	if got := bytesByTopic["t1"]; got != want {
		t.Errorf("multi-broker sum = %v, want %v", got, want)
	}
}

func TestBrokerMetrics_ExtractTopicBytes_IgnoresInternal(t *testing.T) {
	const body = `redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="_kc_configs"} 4096
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="bench_sess1_postgres_cdc_connect"} 1000
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

func TestBrokerMetrics_TopicSeries_DeltasOverFrames(t *testing.T) {
	const body = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="t1"} 0
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="t1"} 10000000
###timestamp=1020
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="t1"} 20000000
`
	series, err := ParseTopicSeries(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseTopicSeries: %v", err)
	}
	t1 := series["t1"]
	if t1 == nil {
		t.Fatal("topic t1 missing from series map")
	}
	// 3 frames → 2 deltas. Each delta covers a 10s interval at
	// 10 MB / 10s = 1 MB/s (decimal — see bytesPerMB).
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
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="t1"} 1000000
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_topic="t1"} 500
`
	series, err := ParseTopicSeries(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseTopicSeries: %v", err)
	}
	if len(series["t1"]) != 0 {
		t.Errorf("reset-detected delta should be skipped; got %+v", series["t1"])
	}
}

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

// TestBrokerMetrics_ExtractTopicRecords pins parsing of
// redpanda_kafka_records_produced_total.
//
// Why this metric matters: byte-based throughput is NOT comparable between
// engines. Connect's headline number comes from its own rolling-stats log
// (uncompressed logical bytes) while KC's comes from broker produce-request
// bytes (compressed on the wire), and the two producers don't even use the
// same compression settings. On the 2026-08-07 SQL Server run the same point
// read 10 MB/s from Connect's log and 0.93 MB/s from the broker — a 14x gap
// that is entirely batch compression, because the seeder reuses one identical
// row payload for every insert. Records/sec is compression-independent and is
// the only basis on which the two engines can be fairly compared.
func TestBrokerMetrics_ExtractTopicRecords(t *testing.T) {
	// Label shape copied verbatim from a live Redpanda /public_metrics scrape.
	const body = `# TYPE redpanda_kafka_records_produced_total counter
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="bench_sess1_postgres_cdc_connect"} 10002181
redpanda_kafka_records_fetched_total{redpanda_namespace="kafka",redpanda_topic="bench_sess1_postgres_cdc_connect"} 0
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="bench_sess1_postgres_cdc_kc.public.orders"} 500
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="_kc_offsets"} 999
`
	got, err := extractTopicProduceRecords(body)
	if err != nil {
		t.Fatalf("extractTopicProduceRecords: %v", err)
	}
	if got["bench_sess1_postgres_cdc_connect"] != 10002181 {
		t.Errorf("connect records = %v, want 10002181", got["bench_sess1_postgres_cdc_connect"])
	}
	if got["bench_sess1_postgres_cdc_kc.public.orders"] != 500 {
		t.Errorf("KC records = %v, want 500", got["bench_sess1_postgres_cdc_kc.public.orders"])
	}
	// records_fetched is consume-side and must not leak in; _kc_* are worker
	// bookkeeping topics and are excluded like everywhere else.
	if len(got) != 2 {
		t.Errorf("expected exactly 2 topics, got %d: %+v", len(got), got)
	}
}

// TestParseTopicSeries_PopulatesMsgPerSec is the regression test for the bug
// that made every KC point report median_msg_s = 0: TopicPoint.MsgPerSec was
// declared and serialized but never populated, so the one compression-
// independent metric was silently always zero.
func TestParseTopicSeries_PopulatesMsgPerSec(t *testing.T) {
	const body = `###timestamp=1000
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="t1"} 1000000
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="t1"} 1000
###timestamp=1010
redpanda_kafka_request_bytes_total{redpanda_namespace="kafka",redpanda_request="produce",redpanda_topic="t1"} 11000000
redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="t1"} 21000
`
	series, err := ParseTopicSeries(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseTopicSeries: %v", err)
	}
	pts := series["t1"]
	if len(pts) != 1 {
		t.Fatalf("want 1 point, got %d", len(pts))
	}
	// 20000 records over a 10s interval.
	if pts[0].MsgPerSec != 2000 {
		t.Errorf("MsgPerSec = %v, want 2000", pts[0].MsgPerSec)
	}
	// 10 MB over 10s = 1 MB/s (decimal), proving bytes still work alongside records.
	if pts[0].MBPerSec != 1 {
		t.Errorf("MBPerSec = %v, want 1", pts[0].MBPerSec)
	}
}

// TestAttributeByEngine_MergesRecordsAcrossKCTopics covers the KC-specific
// path: Debezium writes one topic per table, so KC's series are always merged.
// Summing bytes but not records there would zero out the compression-
// independent metric for the very engine it exists to compare.
func TestAttributeByEngine_MergesRecordsAcrossKCTopics(t *testing.T) {
	series := map[string][]TopicPoint{
		"bench_sess1_mysql_cdc_kc.benchdb.orders":   {{T: 10, MBPerSec: 2, MsgPerSec: 1000}},
		"bench_sess1_mysql_cdc_kc.benchdb.payments": {{T: 10, MBPerSec: 3, MsgPerSec: 1500}},
	}
	out, err := AttributeByEngine(series, "sess1", "mysql_cdc")
	if err != nil {
		t.Fatalf("AttributeByEngine: %v", err)
	}
	kc := out["kafka_connect"]
	if len(kc) != 1 {
		t.Fatalf("want 1 merged point, got %d", len(kc))
	}
	if kc[0].MBPerSec != 5 {
		t.Errorf("merged MBPerSec = %v, want 5", kc[0].MBPerSec)
	}
	if kc[0].MsgPerSec != 2500 {
		t.Errorf("merged MsgPerSec = %v, want 2500 (records must merge, not just bytes)", kc[0].MsgPerSec)
	}
}
