# Iceberg Sink Bench — Plan 2A: Runner / Go (no AWS)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish moving the bench runner's source-assumptions behind the `Topology` interface (metric-scrape sidecar, KC-config rendering, `Validate`) and add the Iceberg `sinkTopology` + `json-orders` seeder + Iceberg-snapshot metric parser — all pure Go, unit-tested, with NO AWS dependency. After this plan the runner compiles, the iceberg sink scenario validates, and all unit tests pass; only live infra (Plan 2B) is missing.

**Architecture:** Two phases in one plan. **Phase 1 (orchestration generalization)** is behavior-preserving: it grows the `Topology` interface with three seams (`Validate`, `MetricSidecar`/`MetricArtifact`, `KCConfig`) and routes `runBench`/`MatrixRunner`/the bench-script renderers through them; `sourceTopology` keeps the CDC path byte-identical and the existing runner unit tests are the regression net. **Phase 2 (sink implementation)** adds `sinkTopology`, a `sinkSpecs` registry, the `kcConnectorSpecs["iceberg"]` entry, the Iceberg-snapshot metric parser, and the `json-orders` producer seeder — all filling the now-complete interface. Both engines target AWS Glue via the **Iceberg REST endpoint + SigV4** (symmetric).

**Tech Stack:** Go (stdlib, `text/template`, `gopkg.in/yaml.v3`, `github.com/twmb/franz-go/pkg/kgo` — already in the root go.mod). Connect `iceberg` output (REST catalog), Tabular `io.tabular.iceberg.connect.IcebergSinkConnector`. Tests: the package's existing `_test.go` contains-style assertions.

---

## Context the implementer needs (verified against live code)

- **Connect `iceberg` output config** (`internal/impl/iceberg/output_iceberg.go`, `config.go`): required `catalog.url`, `namespace`, `table`, one of `storage.{aws_s3|...}`. Glue is reached via REST: `catalog.url: https://glue.<region>.amazonaws.com/iceberg`, `catalog.auth.aws_sigv4: { region: <region>, service: "glue" }`, `catalog.warehouse: <aws-account-id>`, `storage.aws_s3: { bucket, region }`, and `schema_evolution.table_location: s3://<bucket>/<prefix>/` (required for Glue). It maps structured JSON → columns automatically. Has a `batching` field.
- **KC Iceberg sink** reference: `internal/impl/iceberg/bench/kafka-connector/connector.json` uses `io.tabular.iceberg.connect.IcebergSinkConnector`. For Glue-via-REST + SigV4: `iceberg.catalog.type=rest`, `iceberg.catalog.uri=https://glue.<region>.amazonaws.com/iceberg`, `iceberg.catalog.warehouse=<account-id>`, `iceberg.catalog.rest.sigv4-enabled=true`, `iceberg.catalog.rest.signing-name=glue`, `iceberg.catalog.rest.signing-region=<region>`, `iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO`, `iceberg.tables=<ns>.<table>`, `iceberg.tables.auto-create-enabled=true`, `value.converter=org.apache.kafka.connect.json.JsonConverter`, `value.converter.schemas.enable=false`.
- **Metric scrape today** (`matrix.go` `renderBenchScript`, `kcscript.go` `renderKCBenchScript`): a hardcoded bash sidecar polls broker `/public_metrics` every 10s into `/tmp/redpanda-<vcpu>-<suffix>.txt` (suffix `connect`/`kc`), frames each interval under `###timestamp=$(date +%s)`, then `aws s3 cp` to `runs/<sess>/redpanda-<vcpu>-<suffix>.txt`. `fetchBrokerSeriesForEngine` (`matrix.go`) downloads that exact key and calls `m.Topology.EngineSeries`.
- **KC render today** (`main.go` `runBench` ~241-268): gated on `needsKC`; calls `engineSpecFor(s.Connector)` (DB engineSpec), `buildKCRenderInputs` (DB host/port/user/pass), `renderKCConfig`, sets `kcConnectorName = "bench_"+s.Connector`.
- **Seeders**: live in `benchmarking/aws/seeders/<name>/`, root go.mod (no separate module), built by `runSeeder` with `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, staged to `/opt/bench/<name>`, run on the load-gen host. `franz-go` is `github.com/twmb/franz-go` (import `kgo`). Broker list flows via `outs["redpanda_broker_endpoints"]`.
- **Plan 1 left**: `Topology` has 5 methods (no `Validate`); `BenchNames{SessionID,Connector}` with `ConnectTopic()`/`KCTopicPrefix()`; `topologyFor` errors on `DirectionSink`. `Scenario.Validate` holds the engineSpec-existence check.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `runner/topology.go` (**modify**) | Grow interface: `Validate`, `MetricSidecar`/`MetricArtifact`, `KCConfig`; add `MetricSidecar`/`MetricSidecarArgs`/`KCRenderResult` types; extend `BenchNames` for sink. |
| `runner/topology_source.go` (**modify**) | `sourceTopology` implements the 3 new methods (behavior-identical). |
| `runner/topology_sink.go` (**create**) | `sinkTopology` — full sink implementation of all interface methods. |
| `runner/sinkspecs.go` (**create**) | `sinkSpecs` registry (iceberg connector specifics) + `sinkSpecFor`. |
| `runner/icebergmetrics.go` (**create**) | `ParseIcebergSeries` — poller-dump → `[]TopicPoint`. |
| `runner/scenario.go` (**modify**) | Move engineSpec check into `sourceTopology.Validate`; `Validate` dispatches to `topologyFor(...).Validate`. |
| `runner/kcconnectors.go` (**modify**) | Add `kcConnectorSpecs["iceberg"]` (kcSink). |
| `runner/matrix.go` (**modify**) | `MatrixRunner` gains `Outs`; bench-script renderers splice in the topology's `MetricSidecar`; `fetchBrokerSeriesForEngine` uses `MetricArtifact`. |
| `runner/kcscript.go` (**modify**) | `renderKCBenchScript` splices in the topology's `MetricSidecar` (same as connect). |
| `runner/main.go` (**modify**) | `runBench` routes KC render through `topo.KCConfig`; sets `MatrixRunner.Outs`. |
| `seeders/json-orders/main.go` + `produce.go` (**create**) | franz-go JSON producer seeder. |
| Tests: `topology_test.go`, `topology_sink_test.go`, `sinkspecs_test.go`, `icebergmetrics_test.go`, `kcconnectors_test.go` (**modify/create**) | Unit coverage. |

---

# PHASE 1 — Orchestration generalization (behavior-preserving)

## Task 1: `Topology.Validate` seam

**Files:** `runner/topology.go`, `runner/topology_source.go`, `runner/scenario.go`, `runner/topology_test.go`, `runner/scenario_test.go`

- [ ] **Step 1: Write failing tests** — add to `runner/topology_test.go`:

```go
func TestSourceTopology_Validate_RejectsUnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "nope_cdc", Direction: DirectionSource}
	if err := (sourceTopology{}).Validate(s); err == nil {
		t.Fatal("expected error for connector with no engineSpec")
	}
}

func TestSourceTopology_Validate_AcceptsKnown(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Direction: DirectionSource}
	if err := (sourceTopology{}).Validate(s); err != nil {
		t.Fatalf("postgres_cdc must validate, got %v", err)
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`Validate` not on `sourceTopology`): `go test ./benchmarking/aws/runner/ -run TestSourceTopology_Validate -v`

- [ ] **Step 3: Add `Validate` to the `Topology` interface** in `runner/topology.go` (first method, above `Pipeline`):

```go
	// Validate checks direction-specific scenario fields beyond the generic
	// checks in Scenario.Validate.
	Validate(s *Scenario) error
```

- [ ] **Step 4: Implement `sourceTopology.Validate`** in `runner/topology_source.go` (this is the engineSpec check moved out of `Scenario.Validate`):

```go
func (sourceTopology) Validate(s *Scenario) error {
	if _, ok := engineSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no engineSpec entry; add one to engineSpecs in scenario.go", s.Connector)
	}
	return nil
}
```

- [ ] **Step 5: Update `Scenario.Validate`** in `runner/scenario.go` — REMOVE the existing engineSpec block:

```go
	if _, ok := engineSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no engineSpec entry; add one to engineSpecs in scenario.go", s.Connector)
	}
```

and REPLACE it (same location, after the direction switch) with a topology dispatch:

```go
	topo, err := topologyFor(s.Direction)
	if err != nil {
		return err
	}
	if err := topo.Validate(s); err != nil {
		return err
	}
```

> Note: the existing `TestValidate_RejectsNonExistentEngine` test asserts the same error string for `postgres`-shaped scenarios — it still passes because `sourceTopology.Validate` emits the identical message. Run it to confirm. If `topologyFor` returns the "sink not implemented" error for a `DirectionSink` scenario during validate, that's expected until Task 9 (no sink scenario exists yet).

- [ ] **Step 6: Run** `go test ./benchmarking/aws/runner/ -run 'TestSourceTopology_Validate|TestValidate' -v` — expect PASS. Then full suite `go test ./benchmarking/aws/runner/` — expect PASS.

- [ ] **Step 7: Commit**

```bash
gofmt -w benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/scenario.go benchmarking/aws/runner/topology_test.go
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/scenario.go benchmarking/aws/runner/topology_test.go
git commit -m "refactor(bench): add Topology.Validate seam (source engineSpec check moves behind it)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Metric-sidecar seam — types + `sourceTopology` impl

**Files:** `runner/topology.go`, `runner/topology_source.go`, `runner/topology_test.go`

The bench-script renderers currently hardcode the broker `/public_metrics` scrape. This task introduces a `MetricSidecar` the topology owns; `sourceTopology` reproduces today's broker scrape byte-for-byte. (Tasks 3-4 wire the renderers to it.)

- [ ] **Step 1: Write failing tests** — add to `runner/topology_test.go`:

```go
func TestSourceTopology_MetricArtifact(t *testing.T) {
	if got := (sourceTopology{}).MetricArtifact("connect", 2); got != "redpanda-2-connect.txt" {
		t.Errorf("connect artifact = %q", got)
	}
	if got := (sourceTopology{}).MetricArtifact("kafka_connect", 4); got != "redpanda-4-kc.txt" {
		t.Errorf("kc artifact = %q", got)
	}
}

func TestSourceTopology_MetricSidecar_BrokerScrape(t *testing.T) {
	args := MetricSidecarArgs{
		Engine: "connect", VCPU: 2,
		Outs:  map[string]string{"redpanda_metrics_endpoints": "10.0.0.1:9644,10.0.0.2:9644"},
		Names: newBenchNames("sess", "postgres_cdc"),
	}
	sc := (sourceTopology{}).MetricSidecar(args)
	if !strings.Contains(sc.Setup, "/public_metrics") {
		t.Errorf("source sidecar must scrape /public_metrics; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "10.0.0.1:9644,10.0.0.2:9644") {
		t.Errorf("source sidecar must embed broker endpoints; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, `RP=/tmp/redpanda-2-connect.txt`) {
		t.Errorf("source sidecar must write the connect artifact; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "redpanda-2-connect.txt") {
		t.Errorf("upload must reference the artifact; got:\n%s", sc.Upload)
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`MetricSidecar`/`MetricSidecarArgs`/`MetricArtifact` undefined).

- [ ] **Step 3: Add types + interface methods** to `runner/topology.go`. Add to the `Topology` interface (after `EngineSeries`):

```go
	// MetricArtifact is the per-engine, per-vCPU metrics dump basename that the
	// bench script uploads and EngineSeries later parses.
	MetricArtifact(engine string, vcpu int) string
	// MetricSidecar returns the bash that samples throughput during a bench
	// window. Setup launches a background poller (polling $PID every interval,
	// framing samples under "###timestamp=<unix>" into $RP) and ends by setting
	// RP_SCRAPER=$!. Upload copies $RP to S3 after the bench process exits.
	MetricSidecar(args MetricSidecarArgs) MetricSidecar
```

And the supporting types (near `MetricInputs`):

```go
// MetricSidecarArgs is the render context for MetricSidecar. Bucket/SessionID
// locate the S3 upload; Outs carries TF outputs (broker endpoints for source,
// Glue/warehouse for sink); Names supplies per-engine resource names.
type MetricSidecarArgs struct {
	Engine    string
	VCPU      int
	Bucket    string
	SessionID string
	Outs      map[string]string
	Names     BenchNames
}

// MetricSidecar is the bash a bench script splices in to sample throughput.
type MetricSidecar struct {
	Setup  string // background poller; defines $RP, ends with RP_SCRAPER=$!
	Upload string // aws s3 cp of $RP after the run
}
```

- [ ] **Step 4: Implement on `sourceTopology`** in `runner/topology_source.go`. This MUST emit the same broker scrape `renderBenchScript` builds today (read `matrix.go` `renderBenchScript` lines ~411-453 to confirm exact strings before finalizing):

```go
func (sourceTopology) MetricArtifact(engine string, vcpu int) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("redpanda-%d-%s.txt", vcpu, suffix)
}

func (t sourceTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.VCPU)
	endpoints := args.Outs["redpanda_metrics_endpoints"]
	if endpoints == "" {
		endpoints = args.Outs["redpanda_metrics_endpoint"]
	}
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
ENDPOINTS=%q
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      IFS=, read -ra EPS <<< "$ENDPOINTS"
      for EP in "${EPS[@]}"; do
        curl -s --max-time 5 "http://$EP/public_metrics" || echo "###scrape_error_$EP"
      done
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, endpoints)
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: setup, Upload: upload}
}
```

> CRITICAL: the variable names `$RP`, `$PID`, `$RP_SCRAPER` and the `###timestamp=` / `###scrape_error_` framing must match what `renderBenchScript`/`renderKCBenchScript` and the `parseSnapshots` parser expect. After writing, diff `sc.Setup` against the current inline block in `matrix.go` — they must be character-identical (modulo the `%%s`→`%s` escaping in `fmt.Sprintf`). Adjust to match the live code if it differs.

- [ ] **Step 5: Run** `go test ./benchmarking/aws/runner/ -run 'TestSourceTopology_Metric' -v` — expect PASS. Full suite — expect PASS.

- [ ] **Step 6: Commit**

```bash
gofmt -w benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/topology_test.go
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/topology_test.go
git commit -m "refactor(bench): add MetricSidecar/MetricArtifact seam (sourceTopology = broker scrape)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Wire `fetchBrokerSeriesForEngine` to `MetricArtifact`

**Files:** `runner/matrix.go`

- [ ] **Step 1:** Read `fetchBrokerSeriesForEngine` (`matrix.go` ~277-305). It builds `key := fmt.Sprintf("runs/%s/redpanda-%d-%s.txt", m.SessionID, vcpu, suffix)` with an inline `suffix` switch.

- [ ] **Step 2: Replace the inline suffix + key construction** with a `MetricArtifact` call. Replace:

```go
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	key := fmt.Sprintf("runs/%s/redpanda-%d-%s.txt", m.SessionID, vcpu, suffix)
```

with:

```go
	if m.Topology == nil {
		fmt.Fprintf(stdout, "[bench] no Topology configured; metric fetch skipped\n")
		return nil
	}
	key := fmt.Sprintf("runs/%s/%s", m.SessionID, m.Topology.MetricArtifact(engine, vcpu))
```

(Leave the rest — the `LogFetcher.Fetch`, the existing `m.Topology == nil` guard before `EngineSeries` becomes redundant with the new one; remove the second nil check to avoid duplication. Keep the `EngineSeries` call and error handling.)

- [ ] **Step 3:** Build + full suite: `go build ./benchmarking/aws/runner/ && go test ./benchmarking/aws/runner/ -count=1` — expect PASS (the source artifact key is unchanged: `redpanda-<vcpu>-<suffix>.txt`).

- [ ] **Step 4: Commit**

```bash
gofmt -w benchmarking/aws/runner/matrix.go
git add benchmarking/aws/runner/matrix.go
git commit -m "refactor(bench): fetchBrokerSeriesForEngine uses Topology.MetricArtifact

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Splice the topology sidecar into the bench-script renderers

**Files:** `runner/matrix.go` (`benchScriptArgs`, `renderBenchScript`, `MatrixRunner.Run`, `MatrixRunner` struct), `runner/kcscript.go` (`kcBenchScriptArgs`, `renderKCBenchScript`)

This is the integration-risk task. `MatrixRunner.Run` will compute the sidecar from the topology and pass it into the renderers; the renderers splice it where the hardcoded broker scrape used to be. Source output must stay byte-identical.

- [ ] **Step 1:** Add `Outs map[string]string` to the `MatrixRunner` struct (after `Names BenchNames`), with a comment: `// Outs is the TF output map, passed into Topology.MetricSidecar.`

- [ ] **Step 2:** Add two fields to `benchScriptArgs` (`matrix.go`) and `kcBenchScriptArgs` (`kcscript.go`):

```go
	// ScrapeSetup launches the metric poller; ScrapeUpload copies the artifact
	// to S3. Both come from Topology.MetricSidecar.
	ScrapeSetup  string
	ScrapeUpload string
```

- [ ] **Step 3:** In `renderBenchScript` (`matrix.go`), DELETE the inline `if endpoints != "" { ... RP=/tmp/redpanda-... scrape loop ... RP_SCRAPER=$! }` block and the inline `if endpoints != "" { aws s3 cp "$RP" ... }` upload block. Replace the scrape block with appending `a.ScrapeSetup` (only if non-empty) at the same position, and replace the upload block with appending `a.ScrapeUpload` (only if non-empty) at the same position:

```go
	if a.ScrapeSetup != "" {
		lines = append(lines, a.ScrapeSetup)
	}
```
…and where the upload was…
```go
	if a.ScrapeUpload != "" {
		lines = append(lines, a.ScrapeUpload)
	}
```

Do the IDENTICAL transformation in `renderKCBenchScript` (`kcscript.go`), deleting its broker-scrape + upload blocks in favor of `a.ScrapeSetup`/`a.ScrapeUpload`. The `RedpandaMetricsEndpoint(s)` fields on both arg structs become unused by the renderers — leave the struct fields (callers still set them; harmless) OR remove them if `go vet`/build is clean without the callers setting them. Prefer leaving them to minimize churn; report which.

- [ ] **Step 4:** In `MatrixRunner.Run`, before the `switch engine` that renders the script, compute the sidecar once:

```go
		sidecar := m.Topology.MetricSidecar(MetricSidecarArgs{
			Engine:    engine,
			VCPU:      n,
			Bucket:    m.Bucket,
			SessionID: m.SessionID,
			Outs:      m.Outs,
			Names:     m.Names,
		})
```

Then in BOTH the `case "connect"` and `case "kafka_connect"` branches, add `ScrapeSetup: sidecar.Setup, ScrapeUpload: sidecar.Upload,` to the `benchScriptArgs`/`kcBenchScriptArgs` literals.

- [ ] **Step 5:** In `main.go` `runBench`, set `Outs: sharedOuts,` in the `mr := &MatrixRunner{...}` literal (after `Names: names,`).

- [ ] **Step 6: Verify source byte-identical.** Build + full suite: `go build ./benchmarking/aws/runner/ && go test ./benchmarking/aws/runner/ -count=1` — expect PASS. The bench-script tests (search `render_test.go`/`kcscript_test.go` for tests asserting the scrape block) must still pass; if a test asserted the inline scrape that's now sourced from `sourceTopology.MetricSidecar`, the produced string is identical so the assertion holds. If any such test constructs `benchScriptArgs` directly without `ScrapeSetup`, it now renders no scrape block — update that test to pass `ScrapeSetup`/`ScrapeUpload` from `sourceTopology{}.MetricSidecar(...)` (mechanical; preserve its assertions). Report any test you adjusted.

- [ ] **Step 7: Commit**

```bash
gofmt -w benchmarking/aws/runner/matrix.go benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/main.go
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/main.go
git commit -m "refactor(bench): bench scripts splice in Topology.MetricSidecar

Source path byte-identical (sourceTopology emits the same broker scrape);
sink will supply an Iceberg-snapshot poller instead.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `KCConfig` seam

**Files:** `runner/topology.go`, `runner/topology_source.go`, `runner/main.go`, `runner/topology_test.go`

`runBench` builds the KC connector config inline via DB-specific `buildKCRenderInputs`. Move it behind the topology so a sink can render an Iceberg connector instead.

- [ ] **Step 1:** Add the result type + interface method to `runner/topology.go`. Near `MetricSidecar`:

```go
// KCRenderResult is the Kafka Connect connector to submit for an engine run.
type KCRenderResult struct {
	ConnectorName string // e.g. bench_postgres_cdc
	ConfigJSON    string // rendered connector config posted to the KC REST API
}
```

Add to the `Topology` interface:

```go
	// KCConfig renders the Kafka Connect connector (name + JSON) for this
	// scenario, or returns ok=false when the direction has no KC counterpart.
	KCConfig(s *Scenario, outs map[string]string, n BenchNames) (res KCRenderResult, ok bool, err error)
```

- [ ] **Step 2: Write failing test** in `runner/topology_test.go`:

```go
func TestSourceTopology_KCConfig_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline:  map[string]any{"input": map[string]any{"postgres_cdc": map[string]any{"schema": "public", "tables": []any{"orders"}}}},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	res, ok, err := (sourceTopology{}).KCConfig(s, outs, newBenchNames("sess", "postgres_cdc"))
	if err != nil || !ok {
		t.Fatalf("KCConfig: ok=%v err=%v", ok, err)
	}
	if res.ConnectorName != "bench_postgres_cdc" {
		t.Errorf("connector name = %q", res.ConnectorName)
	}
	if !strings.Contains(res.ConfigJSON, "io.debezium.connector.postgresql.PostgresConnector") {
		t.Errorf("config must be the Debezium postgres connector; got:\n%s", res.ConfigJSON)
	}
}
```

- [ ] **Step 3: Run — expect FAIL.**

- [ ] **Step 4: Implement `sourceTopology.KCConfig`** in `runner/topology_source.go` — lift the body from `runBench`'s KC block:

```go
func (sourceTopology) KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, bool, error) {
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return KCRenderResult{}, false, fmt.Errorf("no engineSpec for %q", s.Connector)
	}
	in, err := buildKCRenderInputs(s, es, outs, n.SessionID)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("build KC render inputs: %w", err)
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("render KC config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return KCRenderResult{}, false, err
	}
	return KCRenderResult{
		ConnectorName: fmt.Sprintf("bench_%s", s.Connector),
		ConfigJSON:    string(raw),
	}, true, nil
}
```

Add `"encoding/json"` to `topology_source.go` imports.

- [ ] **Step 5: Rewrite `runBench`'s KC block** (`main.go` ~235-262). Replace the `needsKC` block that calls `engineSpecFor`/`buildKCRenderInputs`/`renderKCConfig` with:

```go
	var kcConnectorName, kcConfigJSON string
	needsKC := false
	for _, e := range opts.engines {
		if e == "kafka_connect" {
			needsKC = true
			break
		}
	}
	if needsKC {
		res, ok, err := topo.KCConfig(s, sharedOuts, names)
		if err != nil {
			return fmt.Errorf("KC config: %w", err)
		}
		if !ok {
			return fmt.Errorf("engine list includes kafka_connect but direction %q has no KC counterpart", s.Direction)
		}
		kcConnectorName = res.ConnectorName
		kcConfigJSON = res.ConfigJSON
	}
```

- [ ] **Step 6:** Build + full suite — expect PASS. (`buildKCRenderInputs`/`renderKCConfig` are now called only from `sourceTopology.KCConfig`; confirm with grep that nothing else references them.)

- [ ] **Step 7: Commit**

```bash
gofmt -w benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/main.go benchmarking/aws/runner/topology_test.go
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/main.go benchmarking/aws/runner/topology_test.go
git commit -m "refactor(bench): route KC connector render through Topology.KCConfig

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

> After Task 5, Phase 1 is complete: every source-assumption the carryover named is behind the interface, and the full source suite is green. The `topologyFor(DirectionSink)` branch still errors — Phase 2 replaces it.

---

# PHASE 2 — Sink implementation

## Task 6: `BenchNames` sink extensions

**Files:** `runner/topology.go`, `runner/topology_test.go`

- [ ] **Step 1: Write failing test** in `topology_test.go`:

```go
func TestBenchNames_SinkConventions(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg")
	if got := n.SourceTopic(); got != "bench_sess-x_iceberg_src" {
		t.Errorf("SourceTopic = %q", got)
	}
	if got := n.IcebergTable("connect"); got != "bench_sess_x_iceberg_connect" {
		t.Errorf("IcebergTable(connect) = %q (dashes must become underscores for SQL identifiers)", got)
	}
	if got := n.ConsumerGroup("kafka_connect"); got != "bench_sess-x_iceberg_kafka_connect" {
		t.Errorf("ConsumerGroup = %q", got)
	}
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Add methods** to `BenchNames` in `topology.go`. Iceberg/Glue table names must be valid SQL identifiers (no dashes), so `IcebergTable` sanitizes the session id:

```go
import "strings" // add to topology.go imports if not present

// SourceTopic is the pre-seeded Redpanda topic a sink bench consumes.
func (n BenchNames) SourceTopic() string {
	return fmt.Sprintf("bench_%s_%s_src", n.SessionID, n.Connector)
}

// IcebergTable is the per-engine Glue table name. Glue/SQL identifiers can't
// contain '-', so the session id's dashes are converted to underscores.
func (n BenchNames) IcebergTable(engine string) string {
	safe := strings.ReplaceAll(n.SessionID, "-", "_")
	return fmt.Sprintf("bench_%s_%s_%s", safe, n.Connector, engine)
}

// ConsumerGroup is the per-engine consumer group reading SourceTopic.
func (n BenchNames) ConsumerGroup(engine string) string {
	return fmt.Sprintf("bench_%s_%s_%s", n.SessionID, n.Connector, engine)
}
```

- [ ] **Step 4: Run** the new test + full suite — expect PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_test.go
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): BenchNames sink helpers (source topic, per-engine table, consumer group)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Iceberg-snapshot metric parser

**Files:** `runner/icebergmetrics.go` (create), `runner/icebergmetrics_test.go` (create)

The sink poller writes frames of `###timestamp=<unix>` followed by `total_files_size_bytes <N>`. This parser turns that into `[]TopicPoint` (inter-frame MB/s deltas), mirroring `ParseTopicSeries`.

- [ ] **Step 1: Write failing test** — create `runner/icebergmetrics_test.go`:

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

func TestParseIcebergSeries_DeltaThroughput(t *testing.T) {
	dump := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 0",
		"###timestamp=1010",
		"total_files_size_bytes 104857600", // +100 MiB over 10s = 10 MiB/s
	}, "\n")
	pts, err := ParseIcebergSeries(strings.NewReader(dump))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if len(pts) != 1 {
		t.Fatalf("want 1 point, got %d (%#v)", len(pts), pts)
	}
	if pts[0].MBPerSec < 9.9 || pts[0].MBPerSec > 10.1 {
		t.Errorf("want ~10 MB/s, got %v", pts[0].MBPerSec)
	}
	if pts[0].T != 10 {
		t.Errorf("T = %d, want 10", pts[0].T)
	}
}

func TestParseIcebergSeries_SkipsCounterReset(t *testing.T) {
	dump := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 1048576",
		"###timestamp=1010",
		"total_files_size_bytes 0", // table dropped/recreated → reset; skip
	}, "\n")
	pts, err := ParseIcebergSeries(strings.NewReader(dump))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if len(pts) != 0 {
		t.Errorf("counter reset must yield no points, got %#v", pts)
	}
}

func TestParseIcebergSeries_Empty(t *testing.T) {
	pts, err := ParseIcebergSeries(strings.NewReader(""))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(pts) != 0 {
		t.Errorf("empty input → no points, got %#v", pts)
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`ParseIcebergSeries` undefined).

- [ ] **Step 3: Implement** — create `runner/icebergmetrics.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseIcebergSeries reads a sink metric dump and returns a throughput series
// derived from the Iceberg table's committed-bytes counter.
//
// Dump format (one frame per poll interval, written by sinkTopology.MetricSidecar):
//
//	###timestamp=<unix-seconds>
//	total_files_size_bytes <cumulative-bytes>
//
// Throughput for frame i is (bytes[i]-bytes[i-1]) / (t[i]-t[i-1]) / MiB. Counter
// resets (current < previous, e.g. the table was dropped between sweep points)
// are skipped, mirroring ParseTopicSeries.
func ParseIcebergSeries(r io.Reader) ([]TopicPoint, error) {
	type frame struct {
		t     int64
		bytes float64
		hasB  bool
	}
	var frames []frame
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "###timestamp="):
			ts, err := strconv.ParseInt(strings.TrimPrefix(line, "###timestamp="), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp %q: %w", line, err)
			}
			frames = append(frames, frame{t: ts})
		case strings.HasPrefix(line, "total_files_size_bytes "):
			if len(frames) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "total_files_size_bytes ")), 64)
			if err != nil {
				return nil, fmt.Errorf("parse bytes %q: %w", line, err)
			}
			frames[len(frames)-1].bytes = v
			frames[len(frames)-1].hasB = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(frames) == 0 {
		return nil, nil
	}
	baseT := frames[0].t
	var out []TopicPoint
	for i := 1; i < len(frames); i++ {
		prev, cur := frames[i-1], frames[i]
		if !prev.hasB || !cur.hasB {
			continue
		}
		interval := cur.t - prev.t
		delta := cur.bytes - prev.bytes
		if interval <= 0 || delta < 0 {
			continue // out-of-order or counter reset
		}
		out = append(out, TopicPoint{
			T:           int(cur.t - baseT),
			MBPerSec:    delta / float64(interval) / (1 << 20),
			IntervalSec: int(interval),
		})
	}
	return out, nil
}
```

- [ ] **Step 4: Run** the iceberg tests + full suite — expect PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w benchmarking/aws/runner/icebergmetrics.go benchmarking/aws/runner/icebergmetrics_test.go
git add benchmarking/aws/runner/icebergmetrics.go benchmarking/aws/runner/icebergmetrics_test.go
git commit -m "feat(bench): ParseIcebergSeries (Iceberg total-files-size → throughput)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `sinkSpecs` registry + `kcConnectorSpecs["iceberg"]`

**Files:** `runner/sinkspecs.go` (create), `runner/sinkspecs_test.go` (create), `runner/kcconnectors.go`, `runner/kcconnectors_test.go`

- [ ] **Step 1: Write failing test** — create `runner/sinkspecs_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestSinkSpecFor_Iceberg(t *testing.T) {
	sp, ok := sinkSpecFor("iceberg")
	if !ok {
		t.Fatal("iceberg sinkSpec must be registered")
	}
	if sp.OutputComponent != "iceberg" {
		t.Errorf("OutputComponent = %q", sp.OutputComponent)
	}
	if sp.Namespace == "" {
		t.Error("Namespace must be set")
	}
}

func TestSinkSpecFor_Unknown(t *testing.T) {
	if _, ok := sinkSpecFor("nope"); ok {
		t.Error("unknown sink must not resolve")
	}
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — create `runner/sinkspecs.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// sinkSpec captures the per-connector wiring for a sink bench, analogous to
// engineSpec for sources. Add a sink connector by adding one entry to
// sinkSpecs; touch no switch statements.
type sinkSpec struct {
	// OutputComponent is the Redpanda Connect output component key
	// (e.g. "iceberg") placed under pipeline.output.
	OutputComponent string
	// Namespace is the Iceberg namespace (Glue database) both engines write to.
	Namespace string
}

var sinkSpecs = map[string]sinkSpec{
	"iceberg": {
		OutputComponent: "iceberg",
		Namespace:       "bench",
	},
}

func sinkSpecFor(connector string) (sinkSpec, bool) {
	sp, ok := sinkSpecs[connector]
	return sp, ok
}
```

- [ ] **Step 4: Add the KC iceberg entry.** In `runner/kcconnectors.go`, add to the `kcConnectorSpecs` map. The `PropsTemplate` targets Glue via the Iceberg REST catalog + SigV4 (symmetric with Connect). Render inputs (`{{.Warehouse}}`, `{{.GlueRESTURI}}`, `{{.Region}}`, `{{.Namespace}}`, `{{.Table}}`, `{{.Topic}}`, `{{.ConsumerGroup}}`) are supplied by `sinkTopology.KCConfig` in Task 9:

```go
	"iceberg": {
		Class:     "io.tabular.iceberg.connect.IcebergSinkConnector",
		Direction: kcSink,
		PropsTemplate: `{
  "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "1",
  "topics": "{{.Topic}}",
  "iceberg.catalog.type": "rest",
  "iceberg.catalog.uri": "{{.GlueRESTURI}}",
  "iceberg.catalog.warehouse": "{{.Warehouse}}",
  "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "iceberg.catalog.rest.sigv4-enabled": "true",
  "iceberg.catalog.rest.signing-name": "glue",
  "iceberg.catalog.rest.signing-region": "{{.Region}}",
  "iceberg.catalog.client.region": "{{.Region}}",
  "iceberg.tables": "{{.Namespace}}.{{.Table}}",
  "iceberg.tables.auto-create-enabled": "true",
  "iceberg.control.commit.interval-ms": "10000",
  "consumer.override.group.id": "{{.ConsumerGroup}}",
  "consumer.override.auto.offset.reset": "earliest",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"iceberg-kafka-connect*"},
	},
```

- [ ] **Step 5: Note the `kcRenderInputs` gap.** The existing `kcRenderInputs` struct (`kcconnectors.go`) has DB fields (`Host`/`Port`/...) but not `GlueRESTURI`/`Warehouse`/`Region`/`Namespace`/`Table`/`Topic`/`ConsumerGroup`. Add those fields to `kcRenderInputs`:

```go
	// Sink (iceberg) render inputs. Empty for source connectors.
	GlueRESTURI   string
	Warehouse     string
	Region        string
	Namespace     string
	Table         string
	Topic         string
	ConsumerGroup string
```

- [ ] **Step 6: Write a KC iceberg render test** in `runner/kcconnectors_test.go`:

```go
func TestRenderKCConfig_Iceberg(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	in := kcRenderInputs{
		GlueRESTURI:   "https://glue.us-east-2.amazonaws.com/iceberg",
		Warehouse:     "123456789012",
		Region:        "us-east-2",
		Namespace:     "bench",
		Table:         "bench_sess_iceberg_kafka_connect",
		Topic:         "bench_sess_iceberg_src",
		ConsumerGroup: "bench_sess_iceberg_kafka_connect",
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["connector.class"] != "io.tabular.iceberg.connect.IcebergSinkConnector" {
		t.Errorf("class = %v", cfg["connector.class"])
	}
	if cfg["iceberg.catalog.rest.sigv4-enabled"] != "true" {
		t.Errorf("sigv4 must be enabled for Glue REST")
	}
	if cfg["iceberg.tables"] != "bench.bench_sess_iceberg_kafka_connect" {
		t.Errorf("iceberg.tables = %v", cfg["iceberg.tables"])
	}
}
```

- [ ] **Step 7: Run** `go test ./benchmarking/aws/runner/ -run 'TestSinkSpec|TestRenderKCConfig_Iceberg' -v` and full suite — expect PASS. (`renderKCConfig` already renders any registered template; no code change needed there beyond the new struct fields.)

- [ ] **Step 8: Commit**

```bash
gofmt -w benchmarking/aws/runner/sinkspecs.go benchmarking/aws/runner/sinkspecs_test.go benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git add benchmarking/aws/runner/sinkspecs.go benchmarking/aws/runner/sinkspecs_test.go benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench): sinkSpecs registry + kcConnectorSpecs[iceberg] (Glue REST + SigV4)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: `sinkTopology`

**Files:** `runner/topology_sink.go` (create), `runner/topology_sink_test.go` (create), `runner/topology.go` (`topologyFor`)

Implements all 8 interface methods for the Iceberg sink. Reads TF outputs: `glue_rest_uri`, `warehouse_account_id`, `warehouse_s3_uri`, `s3_bucket`, `aws_region`, `redpanda_broker_endpoints` (all produced by Plan 2B's stack).

- [ ] **Step 1: Write failing tests** — create `runner/topology_sink_test.go`:

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

func sinkOuts() map[string]string {
	return map[string]string{
		"glue_rest_uri":             "https://glue.us-east-2.amazonaws.com/iceberg",
		"warehouse_account_id":      "123456789012",
		"warehouse_s3_uri":          "s3://rpcn-bench-ice/wh",
		"s3_bucket":                 "rpcn-bench-ice",
		"aws_region":                "us-east-2",
		"redpanda_broker_endpoints": "10.0.0.1:9092",
		"results_bucket":            "rpcn-bench-results",
	}
}

func TestSinkTopology_Validate(t *testing.T) {
	if err := (sinkTopology{}).Validate(&Scenario{Connector: "iceberg", Direction: DirectionSink}); err != nil {
		t.Fatalf("iceberg must validate: %v", err)
	}
	if err := (sinkTopology{}).Validate(&Scenario{Connector: "nope", Direction: DirectionSink}); err == nil {
		t.Fatal("unknown sink connector must fail validation")
	}
}

func TestSinkTopology_Pipeline_RedpandaInIcebergOut(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink, Pipeline: map[string]any{
		"output": map[string]any{"iceberg": map[string]any{"batching": map[string]any{"count": 5000}}},
	}}
	in, out, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	rp, ok := in["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("input must be redpanda; got %#v", in)
	}
	if topics, _ := rp["topics"].([]any); len(topics) != 1 || topics[0] != "bench_${BENCH_SESSION_ID}_iceberg_src" {
		t.Errorf("input topics = %#v", rp["topics"])
	}
	if _, ok := out["iceberg"]; !ok {
		t.Errorf("output must be iceberg; got %#v", out)
	}
}

func TestSinkTopology_MetricArtifact(t *testing.T) {
	if got := (sinkTopology{}).MetricArtifact("connect", 4); got != "iceberg-4-connect.txt" {
		t.Errorf("artifact = %q", got)
	}
}

func TestSinkTopology_MetricSidecar_GluePoll(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 1, Bucket: "rpcn-bench-results", SessionID: "sess",
		Outs: sinkOuts(), Names: newBenchNames("sess", "iceberg"),
	})
	if !strings.Contains(sc.Setup, "aws glue get-table") {
		t.Errorf("sink sidecar must poll Glue; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "total_files_size_bytes") {
		t.Errorf("sink sidecar must emit total_files_size_bytes; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "iceberg-1-connect.txt") {
		t.Errorf("sink sidecar must write the iceberg artifact; got:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "iceberg-1-connect.txt") {
		t.Errorf("upload must reference the artifact; got:\n%s", sc.Upload)
	}
}

func TestSinkTopology_KCConfig_Iceberg(t *testing.T) {
	res, ok, err := (sinkTopology{}).KCConfig(&Scenario{Connector: "iceberg", Direction: DirectionSink}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil || !ok {
		t.Fatalf("KCConfig: ok=%v err=%v", ok, err)
	}
	if res.ConnectorName != "bench_iceberg" {
		t.Errorf("connector name = %q", res.ConnectorName)
	}
	if !strings.Contains(res.ConfigJSON, "io.tabular.iceberg.connect.IcebergSinkConnector") {
		t.Errorf("config must be the iceberg sink; got:\n%s", res.ConfigJSON)
	}
}

func TestSinkTopology_ResetScript_DropsTableAndResetsOffset(t *testing.T) {
	sc, err := (sinkTopology{}).ResetScript(&Scenario{Connector: "iceberg", Direction: DirectionSink}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	if !strings.Contains(sc, "aws glue delete-table") {
		t.Errorf("reset must drop the per-engine Glue table; got:\n%s", sc)
	}
}

func TestSinkTopology_WorkloadScript_Empty(t *testing.T) {
	got, err := (sinkTopology{}).WorkloadScript(&Scenario{Connector: "iceberg"}, sinkOuts(), newBenchNames("sess", "iceberg"))
	if err != nil || got != "" {
		t.Fatalf("bounded sink has no workload: got %q err %v", got, err)
	}
}

func TestSinkTopology_EngineSeries_ParsesIcebergDump(t *testing.T) {
	dump := "###timestamp=1000\ntotal_files_size_bytes 0\n###timestamp=1010\ntotal_files_size_bytes 104857600\n"
	pts, err := (sinkTopology{}).EngineSeries(MetricInputs{Body: strings.NewReader(dump), Names: newBenchNames("sess", "iceberg")}, "connect")
	if err != nil {
		t.Fatalf("EngineSeries: %v", err)
	}
	if len(pts) != 1 || pts[0].MBPerSec < 9.9 {
		t.Errorf("want ~10 MB/s point, got %#v", pts)
	}
}

func TestTopologyFor_SinkResolves(t *testing.T) {
	topo, err := topologyFor(DirectionSink)
	if err != nil {
		t.Fatalf("sink must resolve now: %v", err)
	}
	if _, ok := topo.(sinkTopology); !ok {
		t.Errorf("DirectionSink must yield sinkTopology, got %T", topo)
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`sinkTopology` undefined).

- [ ] **Step 3: Implement** — create `runner/topology_sink.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"fmt"
)

// sinkTopology is the sink bench path: the connector-under-test reads a
// pre-seeded Redpanda topic and writes into an external system (Iceberg/Glue).
// Throughput is the Iceberg table's committed-bytes growth, polled from Glue.
type sinkTopology struct{}

func (sinkTopology) Validate(s *Scenario) error {
	if _, ok := sinkSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no sinkSpec entry; add one to sinkSpecs in sinkspecs.go", s.Connector)
	}
	return nil
}

// Pipeline injects the redpanda INPUT (consuming the pre-seeded topic) and the
// scenario-supplied OUTPUT component, with catalog/warehouse/table filled from
// TF outputs is deferred to runBench's placeholder substitution. The topic and
// table names use the ${BENCH_SESSION_ID} placeholder so substitutePlaceholders
// resolves them at render time (matching the source path's convention).
func (sinkTopology) Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return nil, nil, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	out, ok := s.Pipeline["output"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output must be a map", s.Connector)
	}
	icfg, ok := out[sp.OutputComponent].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("sink scenario %q: pipeline.output.%s must be a map", s.Connector, sp.OutputComponent)
	}
	// Fill the catalog/storage/table from TF-output placeholders (resolved by
	// substitutePlaceholders) + BenchNames. Connect reaches Glue via the REST
	// catalog + SigV4 (service=glue) — symmetric with the KC sink.
	icfg["catalog"] = map[string]any{
		"url":       "${GLUE_REST_URI}",
		"warehouse": "${WAREHOUSE_ACCOUNT_ID}",
		"auth": map[string]any{
			"aws_sigv4": map[string]any{
				"region":  "${AWS_REGION}",
				"service": "glue",
			},
		},
	}
	icfg["namespace"] = sp.Namespace
	icfg["table"] = fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_connect", s.Connector)
	icfg["storage"] = map[string]any{
		"aws_s3": map[string]any{
			"bucket": "${S3_BUCKET}",
			"region": "${AWS_REGION}",
		},
	}
	icfg["schema_evolution"] = map[string]any{
		"enabled":        true,
		"table_location": "${WAREHOUSE_S3_URI}/",
	}
	input = map[string]any{
		"redpanda": map[string]any{
			"seed_brokers":  []string{"${REDPANDA_BROKER_ENDPOINTS}"},
			"topics":        []any{fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_src", s.Connector)},
			"consumer_group": fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_connect", s.Connector),
			"start_from_oldest": true,
		},
	}
	output = map[string]any{sp.OutputComponent: icfg}
	return input, output, nil
}

func (sinkTopology) SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	// Producer seeder: brokers + topic via env/flags (no DSN). Stages the
	// json-orders binary like the source path, then produces the bounded dataset.
	key := "stage/" + s.Dataset.Seeder
	return fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/%s /opt/bench/%s
chmod +x /opt/bench/%s
REDPANDA_BROKERS=%q /opt/bench/%s seed \
  --topic=%s --rows=%d --row-size=%d
`,
		outs["results_bucket"], key, s.Dataset.Seeder, s.Dataset.Seeder,
		outs["redpanda_broker_endpoints"], s.Dataset.Seeder,
		n.SourceTopic(), s.Dataset.InitialRows, s.Dataset.RowSizeBytes,
	), nil
}

func (sinkTopology) WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return "", nil // bounded pre-seeded topic; no sustained workload
}

func (sinkTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	sp, _ := sinkSpecFor(s.Connector)
	region := outs["aws_region"]
	db := sp.Namespace
	brokers := outs["redpanda_broker_endpoints"]
	var b []byte
	w := func(format string, a ...any) { b = append(b, []byte(fmt.Sprintf(format, a...)+"\n")...) }
	w("set -euo pipefail")
	// Drop both engines' per-engine Iceberg tables so total-files-size restarts
	// at 0; the connector recreates on first write. Non-fatal if absent.
	for _, eng := range []string{"connect", "kafka_connect"} {
		w(`aws glue delete-table --region %q --database-name %q --name %q 2>/dev/null || true`,
			region, db, n.IcebergTable(eng))
		// Reset the per-engine consumer group to re-read the whole topic.
		w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
			brokers, n.ConsumerGroup(eng))
	}
	// Idempotent KC connector delete (per-vCPU names are deleted by the bench
	// script; this clears any base name).
	w(`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`, s.Connector)
	return string(b), nil
}

func (sinkTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	return ParseIcebergSeries(in.Body)
}

func (sinkTopology) MetricArtifact(engine string, vcpu int) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("iceberg-%d-%s.txt", vcpu, suffix)
}

func (t sinkTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.VCPU)
	sp, _ := sinkSpecFor(args.Names.Connector)
	region := args.Outs["aws_region"]
	db := sp.Namespace
	table := args.Names.IcebergTable(args.Engine)
	// Poll Glue every 10s: read the table's current metadata_location, fetch the
	// metadata JSON from S3, sum the latest snapshot's total-files-size. Missing
	// table (not yet created) → emit 0. jq + awscli are present on the runner.
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      META=$(aws glue get-table --region %q --database-name %q --name %q \
              --query 'Table.Parameters.metadata_location' --output text 2>/dev/null || echo "")
      if [ -n "$META" ] && [ "$META" != "None" ]; then
        SIZE=$(aws s3 cp "$META" - 2>/dev/null \
                | jq -r '[.snapshots[]?."summary"."total-files-size" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
      else
        SIZE=0
      fi
      echo "total_files_size_bytes ${SIZE:-0}"
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, region, db, table)
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: setup, Upload: upload}
}

func (sinkTopology) KCConfig(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, bool, error) {
	sp, ok := sinkSpecFor(s.Connector)
	if !ok {
		return KCRenderResult{}, false, fmt.Errorf("no sinkSpec for %q", s.Connector)
	}
	in := kcRenderInputs{
		GlueRESTURI:   outs["glue_rest_uri"],
		Warehouse:     outs["warehouse_account_id"],
		Region:        outs["aws_region"],
		Namespace:     sp.Namespace,
		Table:         n.IcebergTable("kafka_connect"),
		Topic:         n.SourceTopic(),
		ConsumerGroup: n.ConsumerGroup("kafka_connect"),
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return KCRenderResult{}, false, fmt.Errorf("render KC iceberg config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return KCRenderResult{}, false, err
	}
	return KCRenderResult{ConnectorName: fmt.Sprintf("bench_%s", s.Connector), ConfigJSON: string(raw)}, true, nil
}
```

- [ ] **Step 4: Flip `topologyFor`** in `runner/topology.go` — replace the `DirectionSink` error branch:

```go
	case DirectionSink:
		return nil, fmt.Errorf("sink topology not yet implemented (lands in Plan 2)")
```

with:

```go
	case DirectionSink:
		return sinkTopology{}, nil
```

- [ ] **Step 5: Add the sink TF-output placeholders to `runBench`.** Plan 1's `substitutePlaceholders` uppercases TF output keys; the sink Pipeline references `${GLUE_REST_URI}`, `${WAREHOUSE_ACCOUNT_ID}`, `${WAREHOUSE_S3_URI}`, `${S3_BUCKET}`, `${AWS_REGION}` — these resolve automatically from `sharedOuts` once Plan 2B's stack emits `glue_rest_uri`/`warehouse_account_id`/`warehouse_s3_uri`/`s3_bucket`/`aws_region`. No code change needed (substitution is generic). Confirm by reading `substitutePlaceholders` in `main.go`.

- [ ] **Step 6: Run** the sink tests + full suite: `go test ./benchmarking/aws/runner/ -count=1` — expect PASS.

> Note on the `redpanda` input field names (`topics`, `consumer_group`, `start_from_oldest`, `seed_brokers`): verify these against the live `redpanda` *input* component schema (search `internal/impl/kafka`/`internal/impl/redpanda` for the input's `service.NewStringListField`/field constants). If a field name differs (e.g. `consumer_group` vs `group`), correct `sinkTopology.Pipeline` to match — the unit test asserts `topics` + the topic value, so adjust both the impl and the test field name together if needed. This is the one place to validate against the real input schema.

- [ ] **Step 7: Commit**

```bash
gofmt -w benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_sink_test.go benchmarking/aws/runner/topology.go
git add benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_sink_test.go benchmarking/aws/runner/topology.go
git commit -m "feat(bench): sinkTopology (iceberg) + topologyFor wires DirectionSink

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: `json-orders` producer seeder

**Files:** `seeders/json-orders/main.go` (create), `seeders/json-orders/produce.go` (create)

A franz-go producer that fills `--topic` with `--rows` flat JSON records of ~`--row-size` bytes. CLI mirrors `cdc-rows` (a `seed` subcommand) so `runSeeder` builds/stages it unchanged. Connection is `REDPANDA_BROKERS` env (set by `sinkTopology.SeedScript`).

- [ ] **Step 1: Create `seeders/json-orders/main.go`:**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: json-orders seed [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		topic := fs.String("topic", "bench-orders", "destination topic")
		rows := fs.Int64("rows", 1_000_000, "records to produce")
		rowSize := fs.Int("row-size", 1200, "approximate record size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), *topic, *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		os.Exit(2)
	}
}
```

- [ ] **Step 2: Create `seeders/json-orders/produce.go`:**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// seed produces `rows` flat JSON records (~rowSize bytes each) into `topic`.
// Brokers come from REDPANDA_BROKERS (comma-separated host:port).
func seed(ctx context.Context, topic string, rows int64, rowSize int) error {
	brokers := os.Getenv("REDPANDA_BROKERS")
	if brokers == "" {
		return fmt.Errorf("REDPANDA_BROKERS env var is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		return fmt.Errorf("kgo client: %w", err)
	}
	defer cl.Close()

	// A flat record whose `payload` filler pads to ~rowSize bytes. Field shape is
	// fixed so both engines infer identical Iceberg columns.
	const fixedOverhead = 120 // id/ts/region/amount/status + JSON punctuation
	padLen := rowSize - fixedOverhead
	if padLen < 0 {
		padLen = 0
	}
	pad := strings.Repeat("x", padLen)

	var produced, failed int64
	for i := int64(0); i < rows; i++ {
		rec := map[string]any{
			"id":      i,
			"ts":      time.Now().UTC().Format(time.RFC3339Nano),
			"region":  "us-east-2",
			"amount":  float64(i%100000) / 100.0,
			"status":  "NEW",
			"payload": pad,
		}
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		cl.Produce(ctx, &kgo.Record{Value: val}, func(_ *kgo.Record, err error) {
			if err != nil {
				atomic.AddInt64(&failed, 1)
			} else {
				atomic.AddInt64(&produced, 1)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if f := atomic.LoadInt64(&failed); f > 0 {
		return fmt.Errorf("%d records failed to produce", f)
	}
	fmt.Printf("json-orders: produced %d records to %s\n", atomic.LoadInt64(&produced), topic)
	return nil
}
```

- [ ] **Step 3: Verify it builds for the runner target** (the runner cross-compiles seeders with `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`):

Run: `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/json-orders ./benchmarking/aws/seeders/json-orders`
Expected: builds with no error.

- [ ] **Step 4:** Run the full runner suite once more (the seeder is a separate `main` package, but confirm nothing in the module broke): `go test ./benchmarking/aws/runner/ -count=1` — expect PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w benchmarking/aws/seeders/json-orders/main.go benchmarking/aws/seeders/json-orders/produce.go
git add benchmarking/aws/seeders/json-orders/
git commit -m "feat(bench): json-orders producer seeder (franz-go → Redpanda topic)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Full-suite + vet gate (no AWS)

**Files:** none (verification only)

- [ ] **Step 1:** `go build ./benchmarking/aws/runner/ ./benchmarking/aws/seeders/json-orders/` — expect success.
- [ ] **Step 2:** `go test ./benchmarking/aws/runner/ -count=1` — expect PASS.
- [ ] **Step 3:** `go vet ./benchmarking/aws/runner/` — expect clean.
- [ ] **Step 4:** Confirm the existing CDC scenarios still validate end-to-end: `go run ./benchmarking/aws/runner validate --scenario=benchmarking/aws/scenarios/postgres/orders-cdc.yaml` and the mysql one — both expect `... OK`.
- [ ] **Step 5:** `gofmt -l benchmarking/aws/runner/ benchmarking/aws/seeders/json-orders/` — expect no output (ignore the pre-existing `ssm.go` if it appears; do not touch it).

(No commit — verification only. A sink scenario YAML doesn't exist until Plan 2B, so a sink `validate` is exercised in 2B.)

---

## Self-Review

**Spec coverage (Plan 2A scope = all runner/Go for the sink, no AWS):**
- Topology generalization seams (`Validate`, `MetricSidecar`/`MetricArtifact`, `KCConfig`) — Tasks 1, 2-4, 5. Addresses carryover items #1-#5. ✓
- `sinkTopology` (Pipeline redpanda→iceberg, ResetScript drop-table+offset-reset, EngineSeries, MetricSidecar Glue poll, KCConfig) — Task 9. ✓
- `sinkSpecs` registry + `kcConnectorSpecs["iceberg"]` (Tabular, Glue REST + SigV4, JSON converter) — Task 8. ✓
- Iceberg-committed-bytes metric (`total-files-size` deltas) — Task 7 (`ParseIcebergSeries`) + Task 9 (`MetricSidecar` poller emitting the matching format). Same `[]TopicPoint` shape → downstream untouched. ✓
- Schemaless JSON — Task 10 seeder produces flat JSON; KC uses `JsonConverter schemas.enable=false` (Task 8); Connect `iceberg` output infers columns. ✓
- `json-orders` seeder via franz-go — Task 10. ✓
- Symmetric Glue access — both engines via `${GLUE_REST_URI}` + SigV4 (Task 8 KC props, Task 9 Connect Pipeline catalog). ✓

**Deferred to Plan 2B (correctly out of scope here):** the `terraform/modules/glue-iceberg` + `stacks/iceberg` (which must emit `glue_rest_uri`/`warehouse_account_id`/`warehouse_s3_uri`/`s3_bucket`/`aws_region`), the runner IAM Glue actions, the `iceberg-kafka-connect` plugin install in `runner-user-data.tftpl`, the `scenarios/iceberg/orders-sink.yaml` + `-smoke.yaml`, and the live 1-vCPU smoke.

**Placeholder scan:** none — every code step has complete code. The three "Note"/"verify against live code" callouts (Task 2 scrape-string parity, Task 4 bench-test adjustment, Task 9 redpanda-input field names) are verification instructions against real code, mirroring Plan 1's accepted pattern, not deferred work.

**Type consistency:** `Topology` interface methods (`Validate`, `Pipeline`, `SeedScript`, `WorkloadScript`, `ResetScript`, `EngineSeries`, `MetricArtifact`, `MetricSidecar`, `KCConfig`), `MetricSidecar{Setup,Upload}`, `MetricSidecarArgs{Engine,VCPU,Bucket,SessionID,Outs,Names}`, `KCRenderResult{ConnectorName,ConfigJSON}`, `BenchNames` methods (`SourceTopic`, `IcebergTable`, `ConsumerGroup`), `sinkSpec{OutputComponent,Namespace}`/`sinkSpecFor`, `ParseIcebergSeries`, `kcRenderInputs` sink fields — all used consistently across Tasks 1-10. Both `sourceTopology` and `sinkTopology` implement the identical 9-method set (compile-checked by `topologyFor` returning them as `Topology`).

---

## Plan 2B preview (not yet written)

Plan 2B — `Terraform + smoke` — will add: `terraform/modules/glue-iceberg/` (S3 warehouse bucket + `aws_glue_catalog_database` + IAM policy granting the runner `glue:*`/`s3:*` on the warehouse), `terraform/stacks/iceberg/` (mirrors `stacks/postgres/`), the `iceberg-kafka-connect` plugin install in `terraform/shared/runner-user-data.tftpl`, `scenarios/iceberg/orders-sink.yaml` + `orders-sink-smoke.yaml`, and the 1-vCPU smoke acceptance (two `PointResult`s, both `MedianMBPerSec>0`, series populated). It validates the one real integration risk: KC's REST-catalog-to-Glue SigV4 config.

### TF-output contract Plan 2A established (Plan 2B MUST satisfy)

After Plan 2A (commits `77ecd3ad8`..`d6f9c884f` on `benchmarking`), the runner's sink path hard-depends on these TF output keys (the runner uppercases each to a `${...}` placeholder and also reads them directly in `sinkTopology`). The `stacks/iceberg` `outputs.tf` MUST emit, with these exact lowercase names:

| TF output key | Used by | Notes |
|---|---|---|
| `glue_rest_uri` | Connect `catalog.url`, KC `iceberg.catalog.uri` | Glue Iceberg REST endpoint, e.g. `https://glue.<region>.amazonaws.com/iceberg` |
| `warehouse_account_id` | Connect `catalog.warehouse`, KC `iceberg.catalog.warehouse` | AWS account id (Glue catalog identifier) |
| `warehouse_s3_uri` | Connect `schema_evolution.table_location` | Base S3 URI **without** a trailing slash — the runner appends `/` (`topology_sink.go`) |
| `s3_bucket` | Connect `storage.aws_s3.bucket` | warehouse bucket name |
| `redpanda_broker_endpoints`, `results_bucket` | input/seed/upload | already emitted by the shared stack — reused unchanged |

**`aws_region` is NOT a TF output** — Plan 2A injects it into the outputs map in `runBench` from `opts.region` (`main.go`: `sharedOuts["aws_region"] = opts.region`). Plan 2B should NOT add an `aws_region` output; it's already handled.

Smoke watch-items (from Plan 2A final review): (a) the runner host needs `jq` + an `aws` CLI with **Glue** permissions on the sink path (the source path never needed Glue); (b) confirm the `MetricSidecar` chain `aws glue get-table → metadata_location → aws s3 cp → jq '.snapshots[]?.summary."total-files-size"'` returns a real cumulative size for BOTH engines' auto-created tables, since `ParseIcebergSeries` depends on the exact `total_files_size_bytes <n>` line shape; (c) the KC Tabular sink's REST-catalog-to-Glue SigV4 config (`iceberg.catalog.rest.sigv4-enabled` + `signing-name=glue`) is the highest integration risk — validate it first.
