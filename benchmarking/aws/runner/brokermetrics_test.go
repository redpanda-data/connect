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
