// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
)

type kcBenchScriptArgs struct {
	VCPU                int
	MemLimitGiB         int
	WarmupSec           int
	DurationSec         int
	ConnectorName       string
	ConnectorConfigJSON string // rendered JSON to PUT to /connectors/<name>/config
	Bucket              string
	SessionID           string
	// RedpandaMetricsEndpoint is the legacy single-broker host:port pair
	// (e.g. "10.42.10.10:9644"). Empty disables the scraper.
	//
	// Deprecated: prefer RedpandaMetricsEndpoints. Redpanda's per-topic
	// byte counters are per-broker; a single-broker scrape silently
	// misses topics whose partition leader is on a different broker.
	RedpandaMetricsEndpoint string
	// RedpandaMetricsEndpoints is a comma-separated list of all broker
	// host:9644 endpoints. The scraper iterates over all of them every
	// 10s; the parser sums per-topic values across the brokers. If both
	// fields are set, Endpoints wins.
	RedpandaMetricsEndpoints string
	// ScrapeSetup launches the metric poller; ScrapeUpload copies the artifact
	// to S3. Both come from Topology.MetricSidecar.
	ScrapeSetup  string
	ScrapeUpload string
}

// renderKCBenchScript produces the shell script executed on the runner EC2
// for one KC sweep point. Unlike Connect's script:
//
//  1. The cloud-init-launched systemd kafka-connect.service is stopped
//     first, so we can spawn the JVM directly with taskset + chrt + Xmx
//     matching the current sweep point's vCPU/memory budget.
//  2. The connector is submitted via curl to the REST API once the JVM is
//     up. Submission is idempotent (PUT /connectors/<name>/config).
//  3. The connector's tasks do the actual CDC work; we sleep warmup+window
//     then SIGTERM the JVM, capture the log, upload, restart the systemd
//     unit so the next sweep point starts from a clean baseline.
func renderKCBenchScript(a kcBenchScriptArgs) string {
	cpusetHi := 1 + a.VCPU
	totalSec := a.WarmupSec + a.DurationSec
	// KC's -Xmx caps heap only; JVM overhead (Metaspace, code cache,
	// direct buffers, native code) typically adds ~25-33% on top. Set
	// heap to floor(0.75 * memLimit) so the total RSS budget matches
	// Connect's GOMEMLIMIT. Floor 1 GiB — too aggressive a discount
	// makes the JVM thrash on smaller budgets.
	kcHeapGiB := a.MemLimitGiB * 3 / 4
	if kcHeapGiB < 1 {
		kcHeapGiB = 1
	}
	// Escape single quotes inside the JSON body for the heredoc.
	cfgJSON := strings.ReplaceAll(a.ConnectorConfigJSON, "'", `'"'"'`)

	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting kc bench: %d vCPU, %d GiB heap, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`KC_LOG=/tmp/kc-%d.log`, a.VCPU),
		`: > "$KC_LOG"`,
		// Always upload kc-N.log to S3 even when the script aborts under set -e,
		// so operators can debug a startup failure post-mortem (the JVM log is
		// the only place that records connect-distributed.sh's actual error).
		fmt.Sprintf(`trap 'rc=$?; aws s3 cp "$KC_LOG" "s3://%s/runs/%s/kc-%d.log" --only-show-errors 2>/dev/null || true; exit $rc' EXIT`,
			a.Bucket, a.SessionID, a.VCPU),
		// Stop the cloud-init-launched worker so we can spawn under taskset.
		`sudo systemctl stop kafka-connect || true`,
		`sleep 2`,
		// Spawn the JVM directly. Equivalent to the systemd unit's ExecStart
		// but with vCPU + heap pinned for this sweep point.
		//
		// NOTE: Connect's bench script uses `chrt --fifo 50` for jitter
		// reduction, but it deadlocks the JVM under single-core taskset
		// (verified on 2026-05-28): JVM internal threads stall under
		// SCHED_FIFO when all bound to one core. Plan 3 will revisit
		// scheduler parity between the two engines.
		fmt.Sprintf(`taskset -c 2-%d env KAFKA_HEAP_OPTS=-Xmx%dg /opt/kafka/bin/connect-distributed.sh /opt/kafka-connect/worker.properties >"$KC_LOG" 2>&1 &`,
			cpusetHi, kcHeapGiB),
		`PID=$!`,
	}
	// Broker-side scrape: the sidecar is computed by Topology.MetricSidecar
	// and passed in via ScrapeSetup. It defines $RP and ends with
	// RP_SCRAPER=$!, written to a per-engine file so the runner can attribute
	// throughput per engine without merging across windows. Appended after
	// $PID is live and before the bench window; empty when the topology has
	// no scrape (or no endpoints).
	if a.ScrapeSetup != "" {
		lines = append(lines, a.ScrapeSetup)
	}
	lines = append(lines,
		// Wait until the REST API answers. KC + Debezium plugins is heavy; on a
		// small runner the JVM can take 90-150s before /connectors responds.
		`for i in $(seq 1 180); do
  if curl -fsS http://localhost:8083/ >/dev/null 2>&1; then echo "[kc] worker REST API up after ${i}s"; break; fi
  if ! kill -0 "$PID" 2>/dev/null; then echo "[kc] JVM died before REST API came up; see kc-log on S3"; exit 1; fi
  sleep 1
done`,
		// Submit the connector. Body comes from the heredoc below.
		fmt.Sprintf(`cat > /tmp/kc-cfg-%d.json <<'KCCFG'
%s
KCCFG`, a.VCPU, cfgJSON),
		// Capture both response body and HTTP status code so we can print
		// the worker's error message when the connector rejects validation
		// (e.g. missing plugin, bad DSN, slot collision). curl --fail
		// suppresses the body which makes 4xx/5xx invisible to the operator.
		fmt.Sprintf(`HTTP_CODE=$(curl -sS -o /tmp/kc-submit-resp.json -w '%%{http_code}' -X PUT -H 'Content-Type: application/json' --data-binary @/tmp/kc-cfg-%d.json http://localhost:8083/connectors/%s/config)
if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "[kc] connector submit failed with HTTP $HTTP_CODE; worker response:"
  cat /tmp/kc-submit-resp.json
  echo
  exit 1
fi
echo "[kc] connector submitted (HTTP $HTTP_CODE)"`, a.VCPU, a.ConnectorName),
		// Wait for RUNNING status (up to 60s).
		fmt.Sprintf(`for i in $(seq 1 60); do
  STATE=$(curl -fsS http://localhost:8083/connectors/%s/status | jq -r '.tasks[0].state // "unknown"' 2>/dev/null || echo "unknown")
  if [ "$STATE" = "RUNNING" ]; then break; fi
  if [ "$STATE" = "FAILED" ]; then echo "task FAILED before warmup; aborting"; exit 1; fi
  sleep 1
done`, a.ConnectorName),
		// Heartbeat — every 60s, print last lines of KC_LOG so SSM has signal.
		`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep 60
    LATEST="$(tail -n 1 "$KC_LOG" 2>/dev/null | tr -d '\n' || true)"
    echo "[kc-heartbeat] ${LATEST:-no output yet}"
  done
) &`,
		`HEARTBEAT=$!`,
		fmt.Sprintf(`sleep %d`, totalSec),
		// Tear down the connector + the JVM.
		fmt.Sprintf(`curl -fsS -X DELETE http://localhost:8083/connectors/%s || true`, a.ConnectorName),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
	)
	if a.ScrapeSetup != "" {
		lines = append(lines, `kill "$RP_SCRAPER" 2>/dev/null || true`)
	}
	lines = append(lines,
		`echo "kc bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$KC_LOG" "s3://%s/runs/%s/kc-%d.log" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
	)
	if a.ScrapeUpload != "" {
		lines = append(lines, a.ScrapeUpload)
	}
	lines = append(lines,
		`echo "kc log uploaded"`,
		// Restart the systemd unit so the host is ready for the next point.
		`sudo systemctl start kafka-connect || true`,
	)
	return strings.Join(lines, "\n")
}
