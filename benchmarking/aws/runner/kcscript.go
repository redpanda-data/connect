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
	// Worker customises the Kafka Connect worker for this point (per-point
	// worker.properties copy, JVM performance flags, environment). The zero
	// value renders the historical script unchanged.
	Worker kcWorkerOverrides
	// ScanWorkerLog adds the end-of-window connector-status and worker-log
	// error scan (kcConnectorSpec.ScanWorkerLog).
	ScanWorkerLog bool
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
	// MemLimitGiB scales linearly with vCPU count (matrix.go:
	// memLimitPerVCPU * n), which has no relationship to the runner
	// instance's actual physical RAM. That's fine at low vCPU counts, but
	// at the high end it can request a heap far larger than the box has:
	// confirmed live on a c8g.4xlarge (16 vCPU / 32 GiB RAM) at the
	// 8-vCPU sweep point with go_mem_limit_per_vcpu=8, MemLimitGiB=64 ->
	// kcHeapGiB=48, a 48 GiB heap on a 32 GiB-RAM machine. The JVM was
	// OOM-killed by the OS ~46s after start. Cap the heap at a ceiling
	// that leaves headroom for the OS plus JVM non-heap overhead
	// (Metaspace, code cache, direct buffers) on the smallest runner
	// instance we currently use for KC sweeps.
	const kcHeapCeilingGiB = 20
	if kcHeapGiB > kcHeapCeilingGiB {
		kcHeapGiB = kcHeapCeilingGiB
	}
	// The body goes into a QUOTED heredoc (<<'KCCFG'), which the shell takes
	// literally: no escaping is needed, and escaping single quotes here would
	// corrupt them into '"'"' inside the JSON (breaking values such as the
	// Confluent TimeBasedPartitioner path.format).
	cfgJSON := a.ConnectorConfigJSON

	workerProps := "/opt/kafka-connect/worker.properties"
	if len(a.Worker.Props) > 0 {
		workerProps = fmt.Sprintf("/tmp/kc-worker-%d.properties", a.VCPU)
	}
	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting kc bench: %d vCPU, %d GiB heap, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		// The Debezium plugin tarballs are downloaded by cloud-init, which can
		// still be running when a KC-only sweep reaches its first point (~7 min
		// after boot; the plugin set takes ~15 min to fetch from Maven Central).
		// Without this gate the worker scans an empty plugin dir and the
		// connector submit 500s with "Failed to find any class". Combined
		// (connect-first) sweeps never hit this because the Connect points give
		// cloud-init a ~25 min head start.
		`echo "[kc] waiting for cloud-init (plugin downloads) to finish..."
sudo cloud-init status --wait >/dev/null 2>&1 || true
echo "[kc] cloud-init done"`,
		fmt.Sprintf(`KC_LOG=/tmp/kc-%d.log`, a.VCPU),
		`: > "$KC_LOG"`,
		// Always upload kc-N.log to S3 even when the script aborts under set -e,
		// so operators can debug a startup failure post-mortem (the JVM log is
		// the only place that records connect-distributed.sh's actual error).
		// Also reap our background children (taskset JVM, broker scraper,
		// heartbeat) on the failure paths: an orphaned JVM keeps the SSM
		// command's process tree alive, so instead of failing in seconds the
		// invocation sits InProgress until the 3600s executionTimeout SIGKILL —
		// observed 2026-08-12, it turned a 2-minute submit failure into an
		// hour-long hang. On the happy path these kills are no-ops.
		fmt.Sprintf(`trap 'rc=$?; kill -TERM ${PID:-} ${RP_SCRAPER:-} ${HEARTBEAT:-} 2>/dev/null || true; aws s3 cp "$KC_LOG" "s3://%s/runs/%s/kc-%d.log" --only-show-errors 2>/dev/null || true; exit $rc' EXIT`,
			a.Bucket, a.SessionID, a.VCPU),
		// Stop the cloud-init-launched worker so we can spawn under taskset.
		//
		// A plain `stop` + `sleep 2` is NOT enough: under load the worker's
		// JVM can hold the REST listen socket (:8083) past the point systemd
		// considers the unit stopped, and the unit's `Restart=on-failure` can
		// relaunch it mid-handoff. If port 8083 is still owned when our
		// taskset JVM (spawned just below) tries to bind, it dies with
		// "Address already in use" (exit 2). Worse, if the systemd unit is the
		// one that loses the race later, it crash-loops on BindException
		// hundreds of times until the other JVM finally exits.
		//
		// So: stop the unit, kill any lingering connect-distributed JVM, and
		// poll until :8083 is actually free before spawning ours. Re-issue the
		// stop each iteration in case `Restart=on-failure` relaunched it.
		`sudo systemctl stop kafka-connect || true`,
		`sudo pkill -f connect-distributed 2>/dev/null || true`,
		`for i in $(seq 1 60); do
  sudo ss -ltn 2>/dev/null | grep -q ':8083 ' || break
  echo "[kc] waiting for port 8083 to free before spawn (${i}s)..."
  sudo systemctl stop kafka-connect >/dev/null 2>&1 || true
  sudo pkill -f connect-distributed 2>/dev/null || true
  sleep 1
done`,
	}
	if len(a.Worker.Props) > 0 {
		lines = append(lines, renderKCWorkerPropsCopy(a.VCPU, a.Worker.Props))
	}
	envArgs := fmt.Sprintf("KAFKA_HEAP_OPTS=-Xmx%dg", kcHeapGiB)
	if a.Worker.JVMPerfOpts != "" {
		envArgs += " " + shellSingleQuote("KAFKA_JVM_PERFORMANCE_OPTS="+a.Worker.JVMPerfOpts)
	}
	for _, kv := range a.Worker.Env {
		envArgs += " " + shellSingleQuote(kv.Key+"="+kv.Value)
	}
	// Spawn the JVM directly. Equivalent to the systemd unit's ExecStart
	// but with vCPU + heap pinned for this sweep point.
	//
	// NOTE: Connect's bench script uses `chrt --fifo 50` for jitter
	// reduction, but it deadlocks the JVM under single-core taskset
	// (verified on 2026-05-28): JVM internal threads stall under
	// SCHED_FIFO when all bound to one core. Plan 3 will revisit
	// scheduler parity between the two engines.
	lines = append(lines,
		fmt.Sprintf(`taskset -c 2-%d env %s /opt/kafka/bin/connect-distributed.sh %s >"$KC_LOG" 2>&1 &`,
			cpusetHi, envArgs, workerProps),
		`PID=$!`,
	)
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
		// Kafka Connect persists connector configs in its internal config
		// topic, not in this JVM's heap. A prior sweep point's connector
		// that wasn't cleanly deleted before its JVM was killed (or whose
		// own ResetScript deleted a mismatched name) stays registered in
		// that topic and gets resurrected by every subsequent point's
		// fresh JVM, running concurrently with the new point's connector.
		// Confirmed live: an OOM whose heartbeat thread names referenced
		// both "connect-bench_s3_v2" and "connect-bench_s3_v8" at once.
		// This worker belongs to a fresh, session-scoped cluster for this
		// one sweep point, so anything already registered on it is stale —
		// wipe it before submitting ours.
		`echo "[kc] clearing any stale connectors left over from prior sweep points..."
for c in $(curl -fsS http://localhost:8083/connectors 2>/dev/null | jq -r '.[]' 2>/dev/null); do
  echo "[kc] deleting stale connector: $c"
  curl -fsS -X DELETE "http://localhost:8083/connectors/$c" 2>/dev/null || true
done
for i in $(seq 1 30); do
  REMAINING=$(curl -fsS http://localhost:8083/connectors 2>/dev/null | jq -r 'length' 2>/dev/null || echo "0")
  if [ "$REMAINING" = "0" ] || [ -z "$REMAINING" ]; then echo "[kc] no stale connectors remain"; break; fi
  echo "[kc] waiting for $REMAINING stale connector(s) to clear (${i}s)..."
  sleep 1
done`,
		// Submit the connector. Body comes from the heredoc below.
		fmt.Sprintf(`cat > /tmp/kc-cfg-%d.json <<'KCCFG'
%s
KCCFG`, a.VCPU, cfgJSON),
		// A large connector plugin (e.g. the iceberg-kafka-connect runtime) can
		// still be scanning the plugin path when the REST API first answers,
		// so an immediate submit hits "Failed to find any class ... available
		// connectors are: [<only built-in>]". Poll /connector-plugins for the
		// class declared in the cfg file (already written above) until it
		// registers, with a bounded timeout, before submitting.
		fmt.Sprintf(`CLASS=$(sed -n 's/.*"connector.class"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' /tmp/kc-cfg-%d.json | head -1)
echo "[kc] waiting for connector class $CLASS to register..."
for i in $(seq 1 60); do
  if curl -s localhost:8083/connector-plugins | grep -q "$CLASS"; then
    echo "[kc] connector class registered after $((i*2))s"
    break
  fi
  sleep 2
done`, a.VCPU),
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
	)
	if a.ScanWorkerLog {
		lines = append(lines, renderKCWorkerLogScan(a.ConnectorName))
	}
	lines = append(lines,
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
		// Our taskset JVM was SIGTERM'd + waited on above, but under load it
		// can take several seconds to release the :8083 listen socket. Wait
		// for it before handing the port back to the systemd unit, otherwise
		// the unit crash-loops on BindException (Restart=on-failure) until our
		// JVM finally exits — which is the failure that previously stranded
		// the full sweep at the kafka_connect points.
		`for i in $(seq 1 60); do
  sudo ss -ltn 2>/dev/null | grep -q ':8083 ' || break
  sleep 1
done`,
		// Restart the systemd unit so the host is ready for the next point.
		`sudo systemctl start kafka-connect || true`,
	)
	return strings.Join(lines, "\n")
}

// renderKCWorkerPropsCopy writes the per-point worker.properties: the
// cloud-init file with every overridden key removed, followed by the
// overrides. Properties.load keeps the LAST duplicate anyway; dropping the
// originals as well keeps the file unambiguous for a human reading it. The
// cloud-init file itself (used by the systemd unit between points) is never
// modified, so scenarios without overrides are unaffected.
func renderKCWorkerPropsCopy(vcpu int, props []kcKV) string {
	var sb strings.Builder
	over := fmt.Sprintf("/tmp/kc-worker-overrides-%d.properties", vcpu)
	dst := fmt.Sprintf("/tmp/kc-worker-%d.properties", vcpu)
	fmt.Fprintf(&sb, "cat > %s <<'KCWORKER'\n", over)
	for _, kv := range props {
		fmt.Fprintf(&sb, "%s=%s\n", kv.Key, kv.Value)
	}
	sb.WriteString("KCWORKER\n")
	fmt.Fprintf(&sb, "awk -F= 'NR==FNR{o[$1]=1;next} !($1 in o)' %s /opt/kafka-connect/worker.properties > %s\n", over, dst)
	fmt.Fprintf(&sb, "cat %s >> %s\n", over, dst)
	fmt.Fprintf(&sb, `echo "[kc] worker.properties: %d override(s) applied on top of the cloud-init file (%s)"`, len(props), dst)
	return sb.String()
}

// renderKCWorkerLogScan prints ###WARN lines (stdout, so they reach the SSM
// stream the operator reads) when the connector or any task is not RUNNING at
// the end of the window, or when the worker log contains error lines. It
// never fails the script: the S3 byte count and the sidecar's consumer-group
// offsets stay the measurement, this only explains a suspicious one.
func renderKCWorkerLogScan(connector string) string {
	return fmt.Sprintf(`echo "[kc] end-of-window diagnostics..."
KC_STATUS=$(curl -fsS http://localhost:8083/connectors/%[1]s/status 2>/dev/null || echo '{}')
KC_NOT_RUNNING=$(echo "$KC_STATUS" | jq -r '[.connector.state, (.tasks[]?.state)] | map(select(. != "RUNNING")) | length' 2>/dev/null || echo "unknown")
if [ "$KC_NOT_RUNNING" != "0" ]; then
  echo "###WARN kc connector/task state not all RUNNING at end of window (non-RUNNING count: $KC_NOT_RUNNING): $(echo "$KC_STATUS" | jq -c '{connector: .connector.state, tasks: [.tasks[]? | {id, state}]}' 2>/dev/null || echo "$KC_STATUS")"
fi
KC_TOLERATED=$(grep -c 'Error encountered in task' "$KC_LOG" 2>/dev/null || true)
KC_ERRORS=$(grep -cE ' ERROR |Exception' "$KC_LOG" 2>/dev/null || true)
echo "[kc] worker log: ${KC_TOLERATED:-0} tolerated-error lines, ${KC_ERRORS:-0} ERROR/Exception lines"
if [ "${KC_TOLERATED:-0}" -gt 0 ] || [ "${KC_ERRORS:-0}" -gt 0 ]; then
  echo "###WARN kc worker log has errors (errors.tolerance=all can silently skip records while consumer offsets still advance; trust S3 bytes, not offsets). Most frequent:"
  { grep -E 'Error encountered in task| ERROR |Exception' "$KC_LOG" | cut -c1-240 | sed -E 's/^\[[^]]*\] //' | sort | uniq -c | sort -rn | head -5; } 2>/dev/null || true
fi`, connector)
}
