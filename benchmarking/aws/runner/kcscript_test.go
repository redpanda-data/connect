// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func TestRenderKCBenchScript_PinsCoresAndHeap(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                4,
		MemLimitGiB:         4,
		WarmupSec:           60,
		DurationSec:         900,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{"connector.class":"x"}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-xyz",
	})
	if !strings.Contains(script, "taskset -c 2-5") {
		t.Errorf("expected core mask 2-5 for vcpu=4; got:\n%s", script)
	}
	if !strings.Contains(script, "-Xmx3g") {
		t.Errorf("expected -Xmx3g heap (floor(0.75*4)=3); got:\n%s", script)
	}
	if !strings.Contains(script, "systemctl stop kafka-connect") {
		t.Errorf("expected stop of systemd-launched worker before our taskset-pinned spawn; got:\n%s", script)
	}
	if !strings.Contains(script, "connect-distributed.sh") {
		t.Errorf("expected connect-distributed.sh invocation; got:\n%s", script)
	}
	if !strings.Contains(script, "bench_pg") {
		t.Errorf("expected connector name in script; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_UploadsKCLog(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                2,
		MemLimitGiB:         2,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_my",
		ConnectorConfigJSON: `{}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-abc",
	})
	if !strings.Contains(script, `aws s3 cp "$KC_LOG" "s3://results-bucket/runs/sess-abc/kc-2.log"`) {
		t.Errorf("expected KC log upload; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_SubmitsConnectorViaCurl(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                1,
		MemLimitGiB:         2,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-abc",
	})
	if !strings.Contains(script, "curl") {
		t.Errorf("expected curl submission; got:\n%s", script)
	}
	if !strings.Contains(script, "/connectors/bench_pg/config") {
		t.Errorf("expected REST path; got:\n%s", script)
	}
	if !strings.Contains(script, "PUT") {
		t.Errorf("expected PUT method; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_HeapFloor(t *testing.T) {
	// MemLimitGiB=1 → 1*3/4 = 0 → floor at 1.
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                1,
		MemLimitGiB:         1,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-x",
	})
	if !strings.Contains(script, "-Xmx1g") {
		t.Errorf("expected -Xmx1g when MemLimitGiB=1 (3/4 floors to 1); got:\n%s", script)
	}
}
