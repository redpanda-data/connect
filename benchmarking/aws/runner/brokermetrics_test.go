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
	// Consume bytes must NOT be in the produce map (we deliberately don't
	// scrape consume-side; the bench attributes throughput to the engine
	// that wrote the bytes).
	if len(bytesByTopic) != 2 {
		t.Errorf("expected exactly 2 topics (both produce-side); got %d: %+v", len(bytesByTopic), bytesByTopic)
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
	// 3 frames → 2 deltas. Each delta covers a 10s interval at
	// 10 MiB / 10s = 1 MiB/s.
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
