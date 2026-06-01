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

func TestRenderKCBenchScript_ScrapesBrokerMetrics(t *testing.T) {
	// Back-compat path: only the singular field is set. The scraper
	// loop must still wrap that single endpoint in the IFS-split shell
	// construct so the script shape is the same as the multi-broker
	// case (one less branch to maintain at runtime).
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "kafka_connect", VCPU: 1,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{"redpanda_metrics_endpoint": "10.42.0.10:9644"},
	})
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                    1,
		MemLimitGiB:             2,
		WarmupSec:               60,
		DurationSec:             600,
		ConnectorName:           "bench_pg",
		ConnectorConfigJSON:     `{}`,
		Bucket:                  "results-bucket",
		SessionID:               "sess-abc",
		RedpandaMetricsEndpoint: "10.42.0.10:9644",
		ScrapeSetup:             sc.Setup,
		ScrapeUpload:            sc.Upload,
	})
	if !strings.Contains(script, "public_metrics") {
		t.Errorf("expected broker /public_metrics scrape; got:\n%s", script)
	}
	if !strings.Contains(script, "redpanda-1-kc.txt") {
		t.Errorf("expected per-engine upload filename; got:\n%s", script)
	}
	if !strings.Contains(script, "10.42.0.10:9644") {
		t.Errorf("expected broker endpoint in script; got:\n%s", script)
	}
	if !strings.Contains(script, "IFS=,") {
		t.Errorf("expected IFS=, multi-endpoint split even with one endpoint; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_ScrapesAllBrokers(t *testing.T) {
	// Plan 3 fix: Redpanda's per-topic byte counter is per-broker, so
	// the scraper must iterate over ALL brokers each interval. The
	// scenario from the 2026-05-29 postgres bench was: KC's Debezium
	// topic was led by broker 1 or 2, but we only scraped broker 0 —
	// the topic showed 0 bytes despite Debezium having written 2.9M
	// records. This test asserts the script visits all three brokers.
	endpoints := "10.42.0.10:9644,10.42.1.10:9644,10.42.0.11:9644"
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "kafka_connect", VCPU: 1,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{"redpanda_metrics_endpoints": endpoints},
	})
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                     1,
		MemLimitGiB:              2,
		WarmupSec:                60,
		DurationSec:              600,
		ConnectorName:            "bench_pg",
		ConnectorConfigJSON:      `{}`,
		Bucket:                   "results-bucket",
		SessionID:                "sess-abc",
		RedpandaMetricsEndpoints: endpoints,
		ScrapeSetup:              sc.Setup,
		ScrapeUpload:             sc.Upload,
	})
	if !strings.Contains(script, "IFS=,") {
		t.Errorf("expected IFS=, splitter for multi-endpoint scrape; got:\n%s", script)
	}
	for _, ep := range []string{"10.42.0.10:9644", "10.42.1.10:9644", "10.42.0.11:9644"} {
		if !strings.Contains(script, ep) {
			t.Errorf("expected endpoint %q in script; got:\n%s", ep, script)
		}
	}
	// Per-engine upload filename for KC.
	if !strings.Contains(script, "redpanda-1-kc.txt") {
		t.Errorf("expected per-engine upload filename; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_PluralEndpointsTakePrecedence(t *testing.T) {
	// If both the legacy singular and the new plural fields are set
	// (transition period), the plural wins so we get the cluster-wide
	// scrape behavior. The legacy endpoint must NOT be the only one
	// emitted to the script.
	sc := sourceTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "kafka_connect", VCPU: 1,
		Bucket: "results-bucket", SessionID: "sess-abc",
		Outs: map[string]string{
			"redpanda_metrics_endpoint":  "10.42.0.10:9644",
			"redpanda_metrics_endpoints": "10.42.0.10:9644,10.42.1.10:9644,10.42.0.11:9644",
		},
	})
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                     1,
		MemLimitGiB:              2,
		WarmupSec:                60,
		DurationSec:              600,
		ConnectorName:            "bench_pg",
		ConnectorConfigJSON:      `{}`,
		Bucket:                   "results-bucket",
		SessionID:                "sess-abc",
		RedpandaMetricsEndpoint:  "10.42.0.10:9644",
		RedpandaMetricsEndpoints: "10.42.0.10:9644,10.42.1.10:9644,10.42.0.11:9644",
		ScrapeSetup:              sc.Setup,
		ScrapeUpload:             sc.Upload,
	})
	if !strings.Contains(script, "10.42.1.10:9644") || !strings.Contains(script, "10.42.0.11:9644") {
		t.Errorf("plural endpoints must override singular; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_NoScrapeWhenEndpointEmpty(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                1,
		MemLimitGiB:         2,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-abc",
		// RedpandaMetricsEndpoint: "" — omitted
	})
	if strings.Contains(script, "public_metrics") {
		t.Errorf("scrape should be omitted when endpoint is empty; got:\n%s", script)
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
