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
