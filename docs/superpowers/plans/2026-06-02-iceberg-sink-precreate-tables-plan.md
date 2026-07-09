# Iceberg Sink — Pre-created Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. **Tasks 1-5 are free (Go + config, unit-tested). Task 6 is an OPERATOR-RUN smoke (real AWS ~$1, aws-vault + supervision) — NOT a subagent task.**

**Goal:** Unblock the KC Tabular Iceberg sink against the AWS Glue REST catalog by pre-creating both engines' per-engine Iceberg tables (with an explicit S3 location + fixed schema) and disabling connector-side auto-create — so the Connect-vs-KC iceberg sink smoke passes.

**Architecture:** A new `iceberg-tablegen` Go tool (reusing the repo's `internal/impl/iceberg/catalogx` REST-catalog client over `apache/iceberg-go`) creates an empty Iceberg table with a location. It's staged on the runner host (which has Glue IAM), and `sinkTopology.ResetScript` invokes it for both engines right after dropping the tables, so each sweep point starts with fresh empty tables. KC's connector gets `auto-create-enabled=false`; Connect writes to the existing table. A bundled fix makes the KC bench script wait for its connector class to be registered before submitting.

**Tech Stack:** Go, `internal/impl/iceberg/catalogx`, `github.com/apache/iceberg-go` (+ `/catalog`), AWS Glue Iceberg REST catalog + SigV4, the existing bench runner.

---

## Context (verified against live code)

- **`catalogx` API** (`internal/impl/iceberg/catalogx/catalog.go`): `func NewCatalogClient(ctx, cfg Config, namespace []string) (*Client, error)`; `Config{URL, Warehouse, AuthType, SigV4Region, SigV4Service, ...}`; `(*Client).CreateTable(ctx, tableName string, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error)`; `(*Client).CheckTableExists(ctx, tableName) (bool, error)`. For Glue: `AuthType:"sigv4"`, `SigV4Service:"glue"`, `SigV4Region:<region>`, `Warehouse:<account-id>`, `URL:<glue rest uri>`, namespace `[]string{"bench"}`.
- **Schema build** (`apache/iceberg-go`): `iceberg.NewSchema(0, fields...)` with `iceberg.NestedField{ID:int, Name:string, Type: iceberg.PrimitiveTypes.Int64 | .String | .Float64, Required:bool}`. `catalog.WithLocation(loc)` from `github.com/apache/iceberg-go/catalog`.
- **json-orders record shape** (`benchmarking/aws/seeders/json-orders/produce.go`): `id` int64, `ts` string, `region` string, `amount` float64, `status` string, `payload` string.
- **Schema field nullability:** use `Required: false` (optional) for all fields — this matches what Connect's iceberg output *infers* (`router.go:457` sets `Required:false`), so a pre-created table is schema-compatible with what Connect would otherwise create (symmetry), and is more forgiving for the JSON converters.
- **`sinkTopology.ResetScript`** (`benchmarking/aws/runner/topology_sink.go`): builds a bash script with a `w(format, ...)` helper; currently per-engine `aws glue delete-table` + `kafka-consumer-groups.sh --reset-offsets`; then KC connector DELETE. Runs on the **runner** host via SSM.
- **Runner staging** (`main.go`): `stageArtefacts` uploads `redpanda-connect`/config/license to `s3://<results_bucket>/stage/...` and runs an SSM script on the runner that `aws s3 cp`s them to `/opt/bench/`. `runSeeder` shows the build pattern: `go build -o <dist> ./benchmarking/aws/seeders/<name>` with `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, then upload to `stage/<name>`.
- **KC props** (`kcconnectors.go` `kcConnectorSpecs["iceberg"]`): has `"iceberg.tables.auto-create-enabled": "true"`.
- **KC bench script** (`kcscript.go renderKCBenchScript`): waits for `localhost:8083` REST up, writes `/tmp/kc-cfg-<vcpu>.json`, then PUTs to `/connectors/<name>/config`.

---

## File Structure

| File | Responsibility |
|---|---|
| `benchmarking/aws/seeders/iceberg-tablegen/main.go` (**create**) | CLI: flags → create one Iceberg table with the fixed schema + location via `catalogx`. |
| `benchmarking/aws/seeders/iceberg-tablegen/schema.go` (**create**) | The fixed 6-field `*iceberg.Schema` builder (+ unit-tested). |
| `benchmarking/aws/seeders/iceberg-tablegen/schema_test.go` (**create**) | Unit test for the schema builder. |
| `benchmarking/aws/runner/main.go` (**modify**) | Build + stage `iceberg-tablegen` on the runner for sink scenarios. |
| `benchmarking/aws/runner/topology_sink.go` (**modify**) | `ResetScript` creates both tables after dropping them. |
| `benchmarking/aws/runner/topology_sink_test.go` (**modify**) | Assert ResetScript emits the create commands. |
| `benchmarking/aws/runner/kcconnectors.go` (**modify**) | `auto-create-enabled` → `false`. |
| `benchmarking/aws/runner/kcconnectors_test.go` (**modify**) | Assert the flag is false. |
| `benchmarking/aws/runner/kcscript.go` (**modify**) | Poll `/connector-plugins` for the class before submit. |
| `benchmarking/aws/runner/kcscript_test.go` (**modify**) | Assert the poll snippet is rendered. |

---

## Task 1: `iceberg-tablegen` tool

**Files:** create `benchmarking/aws/seeders/iceberg-tablegen/{main.go,schema.go,schema_test.go}`

- [ ] **Step 1: Write the failing schema test** — create `benchmarking/aws/seeders/iceberg-tablegen/schema_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestOrdersSchema_SixFields(t *testing.T) {
	s := ordersSchema()
	want := []string{"id", "ts", "region", "amount", "status", "payload"}
	if got := len(s.Fields()); got != len(want) {
		t.Fatalf("schema has %d fields, want %d", got, len(want))
	}
	for i, name := range want {
		if s.Field(i).Name != name {
			t.Errorf("field %d = %q, want %q", i, s.Field(i).Name, name)
		}
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`ordersSchema` undefined): `go test ./benchmarking/aws/seeders/iceberg-tablegen/ -run TestOrdersSchema -v`

- [ ] **Step 3: Create `benchmarking/aws/seeders/iceberg-tablegen/schema.go`:**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "github.com/apache/iceberg-go"

// ordersSchema is the fixed Iceberg schema for the json-orders bench dataset.
// It MUST track the record shape produced by seeders/json-orders/produce.go
// (id, ts, region, amount, status, payload). Fields are Optional to match what
// Connect's iceberg output infers for the same JSON, so a pre-created table is
// schema-compatible with both engines.
func ordersSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 5, Name: "status", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 6, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}
```

> Verify the exact identifiers against the vendored `apache/iceberg-go`: `iceberg.PrimitiveTypes.Int64`/`.String`/`.Float64` (used in `internal/impl/iceberg/icebergx/parquet_test.go`), `iceberg.NestedField{ID,Name,Type,Required}`, `iceberg.NewSchema(0, fields...)`, and that `*iceberg.Schema` has `.Fields()` and `.Field(i)`. If `.Fields()`/`.Field(i)` differ, adjust the test to the real accessor (e.g. range over `s.Fields()`), keeping the assertion intent (6 fields, names in order).

- [ ] **Step 4: Run — expect PASS:** `go test ./benchmarking/aws/seeders/iceberg-tablegen/ -run TestOrdersSchema -v`

- [ ] **Step 5: Create `benchmarking/aws/seeders/iceberg-tablegen/main.go`:**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/apache/iceberg-go/catalog"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

// iceberg-tablegen creates a single empty Iceberg table (with an explicit S3
// location) in a REST catalog — used by the sink bench to pre-create both
// engines' tables, because the AWS Glue REST catalog requires a location on
// create and the KC Tabular sink does not supply one.
func main() {
	catalogURI := flag.String("catalog-uri", "", "Iceberg REST catalog URI (e.g. Glue REST endpoint)")
	warehouse := flag.String("warehouse", "", "catalog warehouse (AWS account id for Glue)")
	region := flag.String("region", "us-east-2", "AWS region for SigV4 signing")
	namespace := flag.String("namespace", "bench", "catalog namespace / Glue database")
	tableName := flag.String("table", "", "table name to create")
	location := flag.String("location", "", "explicit S3 location for the table")
	flag.Parse()

	if *catalogURI == "" || *warehouse == "" || *tableName == "" || *location == "" {
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: --catalog-uri, --warehouse, --table, --location are required")
		os.Exit(2)
	}

	ctx := context.Background()
	cl, err := catalogx.NewCatalogClient(ctx, catalogx.Config{
		URL:          *catalogURI,
		Warehouse:    *warehouse,
		AuthType:     "sigv4",
		SigV4Region:  *region,
		SigV4Service: "glue",
	}, []string{*namespace})
	if err != nil {
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: catalog client:", err)
		os.Exit(1)
	}

	// Idempotent: if the table already exists, nothing to do.
	if exists, err := cl.CheckTableExists(ctx, *tableName); err == nil && exists {
		fmt.Printf("iceberg-tablegen: table %s.%s already exists\n", *namespace, *tableName)
		return
	}

	_, err = cl.CreateTable(ctx, *tableName, ordersSchema(), catalog.WithLocation(*location))
	if err != nil {
		// Tolerate a race where the table was created concurrently.
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			fmt.Printf("iceberg-tablegen: table %s.%s already exists (race)\n", *namespace, *tableName)
			return
		}
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: create table:", err)
		os.Exit(1)
	}
	fmt.Printf("iceberg-tablegen: created %s.%s at %s\n", *namespace, *tableName, *location)
}
```

> Verify against the vendored libs: (a) the module path `github.com/redpanda-data/connect/v4` (check `go.mod` line 1); (b) `catalog.ErrTableAlreadyExists` exists in `apache/iceberg-go/catalog` — if not, drop that `errors.Is` branch and rely on `CheckTableExists` alone; (c) `catalogx.Config` field names (`URL`, `Warehouse`, `AuthType`, `SigV4Region`, `SigV4Service`) per `catalogx/catalog.go`.

- [ ] **Step 6: Build for host + runner target:**

```bash
go build ./benchmarking/aws/seeders/iceberg-tablegen/
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/iceberg-tablegen ./benchmarking/aws/seeders/iceberg-tablegen/ && rm -f /tmp/iceberg-tablegen
```
Expected: both succeed. Remove any stray `iceberg-tablegen` binary left in the repo root by the bare `go build`. Run `go test ./benchmarking/aws/seeders/iceberg-tablegen/` — PASS.

- [ ] **Step 7: Commit:**

```bash
gofmt -w benchmarking/aws/seeders/iceberg-tablegen/
git add benchmarking/aws/seeders/iceberg-tablegen/
git commit -m "feat(bench): iceberg-tablegen tool (pre-create Iceberg table w/ location via catalogx)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Stage `iceberg-tablegen` on the runner (sink only)

**Files:** `benchmarking/aws/runner/main.go`

The reset script (which will invoke the tool) runs on the runner, which has Glue IAM. Build + upload the tool for sink scenarios, and download it on the runner in `stageArtefacts`.

- [ ] **Step 1: Read** `main.go` `stageArtefacts` (the function that uploads `redpanda-connect`/config/license and runs the SSM `aws s3 cp` script on the runner) and `runBench` (where `stageArtefacts` is called, and where `s.Direction` / `buildConnect` / the S3 uploader are available).

- [ ] **Step 2: Add a build+upload helper** in `main.go` (near `runSeeder`):

```go
// stageTableGenForSink builds the iceberg-tablegen binary and uploads it to
// s3://<bucket>/stage/iceberg-tablegen for sink scenarios. The runner downloads
// it in stageArtefacts; sinkTopology.ResetScript invokes it to pre-create tables.
func stageTableGenForSink(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string) error {
	if s.Direction != DirectionSink {
		return nil
	}
	dist := filepath.Join(opts.repoRoot, "benchmarking/aws/seeders/dist")
	_ = os.MkdirAll(dist, 0o755)
	binOut := filepath.Join(dist, "iceberg-tablegen")
	cmd := exec.Command("go", "build", "-o", binOut, "./benchmarking/aws/seeders/iceberg-tablegen")
	cmd.Dir = opts.repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build iceberg-tablegen: %w", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	f, err := os.Open(binOut)
	if err != nil {
		return err
	}
	defer f.Close()
	key := "stage/iceberg-tablegen"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: f})
	return err
}
```

- [ ] **Step 3: Call it in `runBench`** right after `stageArtefacts(...)` succeeds (before `runSeeder`):

```go
	if err := stageTableGenForSink(ctx, opts, s, sharedOuts); err != nil {
		return fmt.Errorf("stage iceberg-tablegen: %w", err)
	}
```

- [ ] **Step 4: Download it on the runner.** In `stageArtefacts`, the SSM script that `aws s3 cp`s the staged files: append a best-effort download of the tablegen binary (present only for sink; `|| true` keeps source benches unaffected). Add to the script string:

```go
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect
aws s3 cp s3://%s/stage/config.yaml /opt/bench/config.yaml
aws s3 cp s3://%s/stage/license.jwt /opt/bench/license.jwt
chmod +x /opt/bench/redpanda-connect
chmod 0600 /opt/bench/license.jwt
aws s3 cp s3://%s/stage/iceberg-tablegen /opt/bench/iceberg-tablegen 2>/dev/null && chmod +x /opt/bench/iceberg-tablegen || true
`, bucket, bucket, bucket, bucket)
```

(Match the EXACT current `script` content in `stageArtefacts` — read it first, then add only the final `iceberg-tablegen` line and the matching `%s`/`bucket` arg. The `2>/dev/null ... || true` makes it a no-op for source benches where the object doesn't exist.)

- [ ] **Step 5:** Build + suite: `go build ./benchmarking/aws/runner/ && go test ./benchmarking/aws/runner/ -count=1` — PASS. `go vet` clean.

- [ ] **Step 6: Commit:**

```bash
gofmt -w benchmarking/aws/runner/main.go
git add benchmarking/aws/runner/main.go
git commit -m "feat(bench): build+stage iceberg-tablegen on the runner for sink scenarios

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `ResetScript` pre-creates both tables

**Files:** `benchmarking/aws/runner/topology_sink.go`, `benchmarking/aws/runner/topology_sink_test.go`

- [ ] **Step 1: Update the test** `TestSinkTopology_ResetScript_DropsTableAndResetsOffset` in `topology_sink_test.go` — add assertions that it now ALSO creates both tables. Append to that test:

```go
	if !strings.Contains(sc, "/opt/bench/iceberg-tablegen") {
		t.Errorf("reset must invoke iceberg-tablegen to pre-create tables; got:\n%s", sc)
	}
	if !strings.Contains(sc, "--table=bench_sess_iceberg_connect") || !strings.Contains(sc, "--table=bench_sess_iceberg_kafka_connect") {
		t.Errorf("reset must pre-create both per-engine tables; got:\n%s", sc)
	}
	if !strings.Contains(sc, "s3://rpcn-bench-ice/wh/bench/bench_sess_iceberg_connect") {
		t.Errorf("reset must pass the per-table S3 location; got:\n%s", sc)
	}
```

(The existing test uses `sinkOuts()` which sets `warehouse_s3_uri=s3://rpcn-bench-ice/wh` and a `newBenchNames("sess","iceberg")` → `IcebergTable("connect")=bench_sess_iceberg_connect`.)

- [ ] **Step 2: Run — expect FAIL:** `go test ./benchmarking/aws/runner/ -run TestSinkTopology_ResetScript -v`

- [ ] **Step 3: Update `sinkTopology.ResetScript`** in `topology_sink.go`. After the per-engine drop + offset-reset loop, add a create step per engine. The full method becomes:

```go
func (sinkTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	sp, _ := sinkSpecFor(s.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	region := outs["aws_region"]
	db := sp.Namespace
	brokers := outs["redpanda_broker_endpoints"]
	catalogURI := outs["glue_rest_uri"]
	warehouse := outs["warehouse_account_id"]
	whBase := outs["warehouse_s3_uri"] // no trailing slash
	var sb strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&sb, format+"\n", a...) }
	w("set -euo pipefail")
	for _, eng := range []string{"connect", "kafka_connect"} {
		table := n.IcebergTable(eng)
		// Drop the per-engine table so total-files-size restarts at 0.
		w(`aws glue delete-table --region %q --database-name %q --name %q 2>/dev/null || true`,
			region, db, table)
		// Pre-create the table with an explicit location: the Glue REST catalog
		// requires one on create and the KC Tabular sink does not supply it.
		w(`/opt/bench/iceberg-tablegen --catalog-uri=%q --warehouse=%q --region=%q --namespace=%q --table=%q --location=%q`,
			catalogURI, warehouse, region, db, table, fmt.Sprintf("%s/%s/%s", whBase, db, table))
		// Reset the per-engine consumer group to re-read the whole topic.
		w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
			brokers, n.ConsumerGroup(eng))
	}
	w(`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`, s.Connector)
	return sb.String(), nil
}
```

> Location format: `<warehouse_s3_uri>/<namespace>/<table>` (matches Connect's router.go pattern `TableLocation + namespace/.../table`). The test expects `s3://rpcn-bench-ice/wh/bench/bench_sess_iceberg_connect`.

- [ ] **Step 4: Run — expect PASS:** `go test ./benchmarking/aws/runner/ -run TestSinkTopology_ResetScript -v`, then full suite `go test ./benchmarking/aws/runner/ -count=1`.

- [ ] **Step 5: Commit:**

```bash
gofmt -w benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_sink_test.go
git add benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_sink_test.go
git commit -m "feat(bench): sink ResetScript pre-creates both engines' iceberg tables w/ location

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: KC connector `auto-create-enabled=false`

**Files:** `benchmarking/aws/runner/kcconnectors.go`, `benchmarking/aws/runner/kcconnectors_test.go`

- [ ] **Step 1: Update the test** `TestRenderKCConfig_Iceberg` in `kcconnectors_test.go` — add:

```go
	if cfg["iceberg.tables.auto-create-enabled"] != "false" {
		t.Errorf("auto-create must be false (tables are pre-created); got %v", cfg["iceberg.tables.auto-create-enabled"])
	}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Edit `kcConnectorSpecs["iceberg"]`** in `kcconnectors.go`: change the line `"iceberg.tables.auto-create-enabled": "true",` to `"iceberg.tables.auto-create-enabled": "false",`.

- [ ] **Step 4: Run — expect PASS:** `go test ./benchmarking/aws/runner/ -run TestRenderKCConfig_Iceberg -v`, then full suite.

- [ ] **Step 5: Commit:**

```bash
gofmt -w benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git add benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench): KC iceberg connector auto-create-enabled=false (tables pre-created)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: KC bench script waits for the connector class

**Files:** `benchmarking/aws/runner/kcscript.go`, `benchmarking/aws/runner/kcscript_test.go`

The large iceberg plugin can still be scanning when the REST API reports up; submitting then yields a spurious "class not found". After REST-up, poll `/connector-plugins` for the connector's class (read from the config JSON) before submitting.

- [ ] **Step 1: Read** `renderKCBenchScript` in `kcscript.go` — find the lines between the "REST API up" wait and the `cat > /tmp/kc-cfg-<vcpu>.json` / `curl ... PUT .../config` submit. Note how the script is assembled (a `lines` slice joined, or `fmt.Sprintf`), and the `a.VCPU` substitution.

- [ ] **Step 2: Write the failing test** in `kcscript_test.go`:

```go
func TestRenderKCBenchScript_WaitsForConnectorClass(t *testing.T) {
	out := renderKCBenchScript(kcBenchScriptArgs{
		VCPU: 1, MemLimitGiB: 2, WarmupSec: 0, DurationSec: 900,
		ConnectorName: "bench_iceberg_v1",
		ConnectorConfigJSON: `{"connector.class":"io.tabular.iceberg.connect.IcebergSinkConnector"}`,
		Bucket: "b", SessionID: "s",
		ScrapeSetup: "", ScrapeUpload: "",
	})
	if !strings.Contains(out, "connector-plugins") {
		t.Errorf("KC bench script must poll /connector-plugins before submitting; got:\n%s", out)
	}
}
```

(Match the real `kcBenchScriptArgs` field set — read the struct; include whatever required fields exist. If `renderKCBenchScript` already had a `ScrapeSetup`/`ScrapeUpload`, set them empty as shown.)

- [ ] **Step 3: Run — expect FAIL.**

- [ ] **Step 4: Add the poll** in `renderKCBenchScript`, AFTER the existing REST-API-up wait and BEFORE the connector submit. Insert this bash (adapt to the script's assembly style — if it's a `lines = append(lines, ...)` slice, append these as elements; if it's one big `fmt.Sprintf`, splice into it). The config file is written at `/tmp/kc-cfg-<vcpu>.json` (it must be written before this poll, since we read the class from it — if the current script writes the cfg AFTER the REST wait, move the `cat > /tmp/kc-cfg...` heredoc to before this poll):

```bash
CLASS=$(sed -n 's/.*"connector.class"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' /tmp/kc-cfg-%[1]d.json | head -1)
echo "[kc] waiting for connector class $CLASS to be registered..."
for i in $(seq 1 60); do
  if curl -s localhost:8083/connector-plugins | grep -q "$CLASS"; then
    echo "[kc] connector class registered after $((i*2))s"
    break
  fi
  sleep 2
done
```

(`%[1]d` = `a.VCPU`; adjust the format-arg style to match how the function builds strings. Ensure the `/tmp/kc-cfg-<vcpu>.json` heredoc is written before this block.)

- [ ] **Step 5: Run — expect PASS:** `go test ./benchmarking/aws/runner/ -run TestRenderKCBenchScript -v` (the new test + existing KC-script tests), then full suite. `go vet` clean, `gofmt -l` clean (ignore pre-existing `ssm.go`).

- [ ] **Step 6: Commit:**

```bash
gofmt -w benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/kcscript_test.go
git add benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/kcscript_test.go
git commit -m "fix(bench): KC bench script waits for connector class before submit

Avoids a spurious 'class not found' when a large plugin (iceberg) is still
scanning at REST-API-up.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Re-run the 1-vCPU smoke — OPERATOR-RUN (real AWS ~$1)

**Not a subagent task.** Needs aws-vault + license + supervision.

- [ ] **Step 1: Pre-flight** (controller): on `benchmarking`, latest commits; `rpcn.license` at repo root; no concurrent bench (`aws ec2 describe-instances --filters tag:Project=redpanda-connect-bench state=running` → empty); runner builds clean.

- [ ] **Step 2: Run with `--keep-on-fail`** (so any residual issue is inspectable):

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/rpcn.license \
  go run ./benchmarking/aws/runner bench \
    --scenario=benchmarking/aws/scenarios/iceberg/orders-sink-smoke.yaml \
    --repo-root=. --keep-on-fail
```

- [ ] **Step 3: Acceptance** — result JSON at `benchmarking/aws/results/iceberg/iceberg-orders-sink-smoke/<ts>.json`: TWO `PointResult`s at vCPU 1 (connect + kafka_connect), BOTH `Summary.MedianMBPerSec > 0`, no KC task failure. `docs/benchmark-results/iceberg.md` appended.

- [ ] **Step 4: If KC still fails**, the infra is kept — SSM into the runner, `curl localhost:8083/connectors/bench_iceberg_v1/status` for the task trace, inspect the pre-created table (`aws glue get-table --database-name bench --name bench_<sess>_iceberg_kafka_connect`). Likely residual: location format mismatch, or schema incompatibility between the pre-created table and the JSON records. Tear down after: `go run ./benchmarking/aws/runner down --scenario=benchmarking/aws/scenarios/iceberg/orders-sink-smoke.yaml --repo-root=.`.

- [ ] **Step 5: On success**, tune `expected_peak_mb_s` in both iceberg scenarios to the observed medians; commit.

---

## Self-Review

**Spec coverage:** `iceberg-tablegen` tool via catalogx → Task 1. Staged on runner → Task 2. ResetScript pre-creates both tables w/ location → Task 3. KC auto-create off → Task 4. KC plugin-class wait → Task 5. Smoke validation → Task 6. Symmetry (both engines, same schema/location, optional fields matching Connect's inference) → Tasks 1+3+4. ✓

**Placeholder scan:** none — full code in every code step. The "Verify against vendored lib" notes (Task 1 type identifiers + `catalog.ErrTableAlreadyExists`; Task 2 exact `stageArtefacts` script; Task 5 script-assembly style) are verification-against-real-code instructions, mirroring the accepted pattern from earlier plans, not deferred work.

**Type/name consistency:** `ordersSchema`, `catalogx.NewCatalogClient`/`Config`/`CreateTable`/`CheckTableExists`, `catalog.WithLocation`, `stageTableGenForSink`, `/opt/bench/iceberg-tablegen`, location `<warehouse_s3_uri>/<namespace>/<table>`, `iceberg.tables.auto-create-enabled=false` — consistent across Tasks 1-4 and the test assertions. `n.IcebergTable(eng)` / `n.ConsumerGroup(eng)` / `sinkSpecFor(...).Namespace` match Plan 2A's `BenchNames`/`sinkSpecs`.

---

## After this plan

The iceberg sink bench is unblocked for both engines. Remaining (separate): tune `expected_peak_mb_s`, run the full 4-point sweep, and the Avro+SR variant. Update the bench-framework skill's sink row.
