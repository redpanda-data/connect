# Iceberg Sink Bench — Plan 2B: Terraform + Smoke

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Tasks 1-4 are free (no AWS). Task 5 is an OPERATOR-RUN smoke that spends real AWS (~$1) and needs aws-vault + a license — it is NOT a subagent task.**

**Goal:** Provision the AWS Glue + S3 warehouse infrastructure the Plan 2A runner already expects, install the Kafka Connect Iceberg sink plugin on the runner, add the iceberg sink scenarios, and run a 1-vCPU smoke validating the Connect-vs-KC Iceberg head-to-head end-to-end.

**Architecture:** A new `terraform/modules/glue-iceberg` (S3 warehouse bucket + `aws_glue_catalog_database` named `bench`) and a thin `terraform/stacks/iceberg` that emits the exact TF outputs Plan 2A's `sinkTopology` consumes (`glue_rest_uri`, `warehouse_account_id`, `warehouse_s3_uri`, `s3_bucket`). Glue/S3 IAM is added to the shared runner role (additive), and the `iceberg-kafka-connect` plugin is added to the shared runner cloud-init. Two scenarios (full sweep + smoke) drive it. No Go changes — the runner is direction-complete after Plan 2A.

**Tech Stack:** Terraform (`hashicorp/aws ~> 5.70`), AWS Glue Iceberg REST catalog + S3, the Tabular/Databricks `iceberg-kafka-connect-runtime` v0.6.19 plugin, the existing bench runner + Taskfile.

---

## Context the implementer needs (verified against live code)

- **TF-output contract Plan 2A established** (the runner reads these; uppercased to `${...}` placeholders + read directly in `sinkTopology`): `glue_rest_uri`, `warehouse_account_id`, `warehouse_s3_uri` (base URI, runner appends `/`), `s3_bucket`. `aws_region` is injected by `runBench` from `opts.region` — **NOT** a TF output. `redpanda_broker_endpoints` + `results_bucket` already come from the shared stack.
- **Sink stack vars:** `translateInfraSource(s.Infra.Source, opts.region)` returns `{"region": <region>}` when the scenario has no `infra.source:` block (`main.go`). So `stacks/iceberg` needs only a `region` variable. `bench_session_id` goes to the *shared* stack only — the iceberg stack does NOT need it (the Glue DB is fixed-name `bench`; per-engine tables are auto-created by the engines at runtime, named via `BenchNames.IcebergTable`).
- **Stack/module shape** (from `stacks/postgres/` + `modules/rds-postgres/`): the **stack** declares its own `terraform{}` + `provider "aws"` + `backend "s3" {}`; the **module** declares neither (inherits the provider). The runner merges stack outputs over shared outputs.
- **Shared IAM** (`shared/iam.tf`): `aws_iam_role.bench_host` + inline policy `bench_host_extra` granting `s3:*`-ish + `secretsmanager` on `*`. It does NOT grant Glue. Instance profile `aws_iam_instance_profile.bench_host`.
- **Cloud-init** (`shared/runner-user-data.tftpl`): KC plugins install under `/opt/kafka-connect/plugins/`, gated by `%{ if install_kc ~}`. The Aiven JDBC entry uses the `.zip` + `unzip -q ... -d /opt/kafka-connect/plugins/` pattern — mirror it.
- **KC Iceberg plugin artifact** (the repo's own `internal/impl/iceberg/bench/kafka-connector/Dockerfile` uses it): `https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/iceberg-kafka-connect-runtime-0.6.19.zip` — ships connector class `io.tabular.iceberg.connect.IcebergSinkConnector` (matches `kcConnectorSpecs["iceberg"]`).
- **Bounded-dataset validation** (`scenario.go`): with no `workload:`, requires `dataset.expected_peak_mb_s` and `(InitialRows*RowSizeBytes/1MiB)/expected_peak_mb_s ≥ 900s` (15 min).
- **`task aws:validate scenario=<stack>/<name>`** runs the runner's `validate` (YAML + topology + bounded check) — free, no AWS. `task aws:bench scenario=<...>` runs the full provision→sweep→teardown — real AWS.
- **Free TF gate:** `terraform init -backend=false && terraform validate` checks HCL syntax + provider schema with no AWS creds/state. Use it as the per-task "test".

---

## File Structure

| File | Responsibility |
|------|----------------|
| `terraform/modules/glue-iceberg/variables.tf` (**create**) | `name_prefix`, `region`, `database_name` inputs. |
| `terraform/modules/glue-iceberg/main.tf` (**create**) | `data.aws_caller_identity`, S3 warehouse bucket (`force_destroy`), `aws_glue_catalog_database`. |
| `terraform/modules/glue-iceberg/outputs.tf` (**create**) | `glue_rest_uri`, `warehouse_account_id`, `warehouse_s3_uri`, `s3_bucket`, `glue_database`. |
| `terraform/stacks/iceberg/main.tf` (**create**) | terraform+provider+backend; instantiates the module. |
| `terraform/stacks/iceberg/variables.tf` (**create**) | `region`. |
| `terraform/stacks/iceberg/outputs.tf` (**create**) | re-exports the module outputs. |
| `terraform/shared/iam.tf` (**modify**) | add `glue:*` to the runner policy. |
| `terraform/shared/runner-user-data.tftpl` (**modify**) | install the iceberg-kafka-connect plugin. |
| `scenarios/iceberg/orders-sink.yaml` (**create**) | full 4-point sweep. |
| `scenarios/iceberg/orders-sink-smoke.yaml` (**create**) | 1-vCPU smoke. |

> **Coordination note (Prakhar runs parallel benches on this branch):** Task 3 edits the *shared* stack (IAM + cloud-init). Re-applying shared (which every `task aws:bench` does at startup) will recreate the runner EC2 instance with the iceberg plugin. This is additive and serialized by the DDB state lock, so it won't corrupt a parallel run — but don't run the iceberg smoke while another bench is mid-apply. Keep all commits scoped with explicit `git add` (the working tree carries unrelated parallel work).

---

## Task 1: `glue-iceberg` Terraform module

**Files:** create `terraform/modules/glue-iceberg/{variables.tf,main.tf,outputs.tf}`

- [ ] **Step 1: Create `terraform/modules/glue-iceberg/variables.tf`:**

```hcl
variable "name_prefix" {
  type        = string
  description = "Prefix for the warehouse bucket name (made globally unique with the account id)."
}

variable "region" {
  type        = string
  description = "AWS region; used to construct the Glue Iceberg REST endpoint."
}

variable "database_name" {
  type        = string
  default     = "bench"
  description = "Glue catalog database (Iceberg namespace) both engines write to. Must match sinkSpecs[\"iceberg\"].Namespace in the runner."
}
```

- [ ] **Step 2: Create `terraform/modules/glue-iceberg/main.tf`:**

```hcl
data "aws_caller_identity" "current" {}

# Iceberg warehouse: data + metadata files. force_destroy so `terraform destroy`
# can reclaim it even though the engines write objects into it during a run.
resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

# Iceberg namespace. Per-engine tables are auto-created inside this database by
# Connect's iceberg output / the KC Tabular sink at runtime — not by Terraform.
# Glue DeleteDatabase cascades to its tables, so destroy works with tables present.
resource "aws_glue_catalog_database" "bench" {
  name = var.database_name
}
```

- [ ] **Step 3: Create `terraform/modules/glue-iceberg/outputs.tf`:**

```hcl
# Glue's Iceberg REST catalog endpoint. Both engines use this same URL + SigV4
# (service=glue) so the comparison is apples-to-apples.
output "glue_rest_uri" {
  value = "https://glue.${var.region}.amazonaws.com/iceberg"
}

# Glue catalog identifier = the AWS account id (the `warehouse` for the REST catalog).
output "warehouse_account_id" {
  value = data.aws_caller_identity.current.account_id
}

# Base S3 URI for Iceberg table data/metadata (no trailing slash — the runner
# appends "/" for Connect's schema_evolution.table_location).
output "warehouse_s3_uri" {
  value = "s3://${aws_s3_bucket.warehouse.bucket}/wh"
}

output "s3_bucket" {
  value = aws_s3_bucket.warehouse.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.bench.name
}
```

- [ ] **Step 4: Format + validate (free, no AWS):**

```bash
cd benchmarking/aws/terraform/modules/glue-iceberg
terraform fmt
terraform init -backend=false
terraform validate
cd -
```
Expected: `terraform validate` prints `Success! The configuration is valid.`

- [ ] **Step 5: Commit:**

```bash
git add benchmarking/aws/terraform/modules/glue-iceberg/
git commit -m "feat(bench): glue-iceberg TF module (S3 warehouse + Glue database)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `iceberg` Terraform stack

**Files:** create `terraform/stacks/iceberg/{main.tf,variables.tf,outputs.tf}`

- [ ] **Step 1: Create `terraform/stacks/iceberg/variables.tf`:**

```hcl
variable "region" {
  type    = string
  default = "us-east-2"
}
```

- [ ] **Step 2: Create `terraform/stacks/iceberg/main.tf`** (mirrors `stacks/postgres/main.tf`; no `terraform_remote_state` needed — Glue/S3 are regional, not VPC-bound):

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "redpanda-connect-bench"
      Stack     = "iceberg"
      ManagedBy = "terraform"
    }
  }
}

module "iceberg" {
  source      = "../../modules/glue-iceberg"
  name_prefix = "rpcn-bench-ice"
  region      = var.region
}
```

- [ ] **Step 3: Create `terraform/stacks/iceberg/outputs.tf`** (the keys Plan 2A's runner consumes):

```hcl
output "glue_rest_uri" {
  value = module.iceberg.glue_rest_uri
}
output "warehouse_account_id" {
  value = module.iceberg.warehouse_account_id
}
output "warehouse_s3_uri" {
  value = module.iceberg.warehouse_s3_uri
}
output "s3_bucket" {
  value = module.iceberg.s3_bucket
}
output "glue_database" {
  value = module.iceberg.glue_database
}
```

- [ ] **Step 4: Format + validate (free):**

```bash
cd benchmarking/aws/terraform/stacks/iceberg
terraform fmt
terraform init -backend=false
terraform validate
cd -
```
Expected: `Success! The configuration is valid.` (init -backend=false fetches the module + aws provider without touching the S3 backend.)

- [ ] **Step 5: Commit:**

```bash
git add benchmarking/aws/terraform/stacks/iceberg/
git commit -m "feat(bench): iceberg TF stack (emits glue/warehouse outputs for the runner)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Shared IAM (Glue) + runner cloud-init (plugin)

**Files:** modify `terraform/shared/iam.tf`, `terraform/shared/runner-user-data.tftpl`

- [ ] **Step 1: Add Glue permissions to the runner role.** In `terraform/shared/iam.tf`, the `aws_iam_role_policy.bench_host_extra` policy has an `Action` list `["s3:PutObject", "s3:GetObject", "s3:ListBucket", "secretsmanager:GetSecretValue"]`. Add `"glue:*"` to it so the sink path (Glue REST catalog via SigV4, the `aws glue get-table` metric poller, and `aws glue delete-table` reset) works. The list becomes:

```hcl
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "secretsmanager:GetSecretValue",
          "glue:*",
        ]
```

Leave the rest of `iam.tf` unchanged. (This broadens the runner role for ALL benches; harmless for postgres/mysql which never call Glue.)

- [ ] **Step 2: Install the iceberg-kafka-connect plugin in cloud-init.** In `terraform/shared/runner-user-data.tftpl`, find the KC plugin install block (inside `%{ if install_kc ~}`), after the Aiven JDBC lines (the `rm -f /tmp/aiven-jdbc.zip` line) and BEFORE the `systemctl daemon-reload` line. Add:

```bash
  # Iceberg sink connector (Databricks build of the Tabular connector; ships
  # class io.tabular.iceberg.connect.IcebergSinkConnector — see kcConnectorSpecs["iceberg"]).
  - curl -sSL -o /tmp/iceberg-kc.zip https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/iceberg-kafka-connect-runtime-0.6.19.zip
  - unzip -q /tmp/iceberg-kc.zip -d /opt/kafka-connect/plugins/
  - rm -f /tmp/iceberg-kc.zip
```

Match the surrounding YAML cloud-init indentation (two-space list items under `runcmd:`).

- [ ] **Step 3: Validate the shared stack syntax (free):**

```bash
cd benchmarking/aws/terraform/shared
terraform fmt
terraform init -backend=false
terraform validate
cd -
```
Expected: `Success! The configuration is valid.` (The `.tftpl` is a template file rendered at apply; `terraform validate` checks the HCL that references it. A YAML typo in the template won't be caught here — re-read your added lines for correct cloud-init indentation.)

- [ ] **Step 4: Commit:**

```bash
git add benchmarking/aws/terraform/shared/iam.tf benchmarking/aws/terraform/shared/runner-user-data.tftpl
git commit -m "feat(bench): grant runner Glue IAM + install iceberg-kafka-connect plugin

Additive shared-stack changes for the iceberg sink bench: glue:* on the
runner role, and the Databricks iceberg-kafka-connect-runtime v0.6.19 plugin
(ships io.tabular.iceberg.connect.IcebergSinkConnector) in cloud-init.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Sink scenarios (full + smoke)

**Files:** create `scenarios/iceberg/orders-sink.yaml`, `scenarios/iceberg/orders-sink-smoke.yaml`

Sink scenarios have `direction: sink`, no `infra.source:`, no `workload:` (bounded), and no `reset:` (sinkTopology generates its own reset). The runner injects the redpanda input + iceberg catalog/storage config; the scenario only supplies output tuning.

- [ ] **Step 1: Create `benchmarking/aws/scenarios/iceberg/orders-sink.yaml`:**

```yaml
name: iceberg-orders-sink
description: |
  Drain a pre-seeded Redpanda topic of flat JSON records into an Apache Iceberg
  table (AWS Glue REST catalog + S3) and compare Connect's iceberg output against
  the Kafka Connect Iceberg sink, head-to-head across a vCPU sweep. Throughput is
  the Iceberg table's committed-bytes growth (total-files-size), polled from Glue.
  Both engines reach Glue via the same REST endpoint + SigV4 (service=glue), so
  the comparison is apples-to-apples. Bounded dataset (no sustained workload):
  the topic is the fixed input; each sweep point re-reads it from the beginning.

direction: sink
connector: iceberg
stack: iceberg

infra:
  runner:
    instance_type: c8g.4xlarge

dataset:
  initial_rows: 160000000   # ~180 GiB at 1200 B/row → ≥15 min at 200 MB/s (8 vCPU)
  row_size_bytes: 1200
  seeder: json-orders
  expected_peak_mb_s: 200

pipeline:
  # sinkTopology injects the redpanda INPUT (the pre-seeded topic) and the
  # iceberg catalog/storage/table config from TF outputs + BenchNames. The
  # scenario supplies only output-side tuning.
  output:
    iceberg:
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]
```

- [ ] **Step 2: Create `benchmarking/aws/scenarios/iceberg/orders-sink-smoke.yaml`** (1 vCPU, small dataset that still clears the 15-min floor at a conservative 1-vCPU peak):

```yaml
name: iceberg-orders-sink-smoke
description: |
  1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
  pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
  a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
  path on both engines before the full sweep.

direction: sink
connector: iceberg
stack: iceberg

infra:
  runner:
    instance_type: c8g.4xlarge

dataset:
  initial_rows: 12000000    # ~13.7 GiB at 1200 B/row → ~915 s at 15 MB/s ≥ 15 min
  row_size_bytes: 1200
  seeder: json-orders
  expected_peak_mb_s: 15

pipeline:
  output:
    iceberg:
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1]
```

- [ ] **Step 3: Validate both scenarios (free — runs the runner's validate, no AWS):**

```bash
go run ./benchmarking/aws/runner validate --scenario=benchmarking/aws/scenarios/iceberg/orders-sink.yaml
go run ./benchmarking/aws/runner validate --scenario=benchmarking/aws/scenarios/iceberg/orders-sink-smoke.yaml
```
Expected: each prints `scenario iceberg-orders-sink... OK (...)`. This exercises `sinkTopology.Validate` (sinkSpec exists) + the bounded-dataset wall-clock check. If the smoke errors with "would complete in <900s", bump `initial_rows` until `(initial_rows*1200/1048576)/expected_peak_mb_s ≥ 900`.

- [ ] **Step 4: Commit:**

```bash
git add benchmarking/aws/scenarios/iceberg/
git commit -m "feat(bench): iceberg sink scenarios (full sweep + 1-vCPU smoke)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: 1-vCPU smoke — OPERATOR-RUN (real AWS spend ~$1)

**This is NOT a subagent task.** It requires `aws-vault exec bench`, the Enterprise license at the repo root, and live AWS provisioning/teardown. Prakhar runs it when ready to babysit. Pre-flight is the bench-framework skill's "Operate a bench" checklist.

- [ ] **Step 1: Pre-flight** (see bench-framework skill / `references/workflow-essentials.md`): on `benchmarking` with latest commits; `rpcn.license` at repo root (NOT `~/Downloads/`); aws-vault `bench` profile set up; `make zip` run in `cleanup-lambda/` recently. **Not running another bench concurrently** (Task 3 re-applies shared, recreating the runner with the iceberg plugin).

- [ ] **Step 2: Run the smoke** (both engines, 1 vCPU):

```bash
aws-vault exec bench -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/rpcn.license \
  task aws:bench scenario=iceberg/orders-sink-smoke
```

`REDPANDA_LICENSE_FILEPATH` is mandatory (enterprise iceberg output won't start without it). SIGINT (Ctrl+C) only if you must interrupt — never SIGKILL (strands infra).

- [ ] **Step 3: Acceptance** — when it finishes, check `benchmarking/aws/results/iceberg/iceberg-orders-sink-smoke/<timestamp>.json`:
  - TWO `PointResult` entries at vCPU 1 (one `connect`, one `kafka_connect`).
  - BOTH `Summary.MedianMBPerSec > 0` (derived from Iceberg `total-files-size` growth).
  - `BrokerSeries` populated for both (it carries the iceberg-derived series for sinks).
  - `docs/benchmark-results/iceberg.md` appended; `SUMMARY.md` refreshed.

- [ ] **Step 4: If it fails, triage with the real artifacts in S3** (`s3://<results_bucket>/runs/<sess>/`):
  - `iceberg-1-connect.txt` / `iceberg-1-kc.txt` — the Glue-poll dumps. If all `total_files_size_bytes 0`: the engine isn't committing to Iceberg (check `sweep-1.log` for Connect / `kc-1.log` for KC). Likely causes: **(highest risk)** KC Tabular sink's REST-catalog-to-Glue SigV4 config rejected (`iceberg.catalog.rest.sigv4-enabled`/`signing-name=glue`) — check the KC worker response in `kc-1.log`; the plugin missing (`iceberg-kafka-connect*` not on the runner — check cloud-init ran); or runner IAM lacking `glue:*` (Task 3).
  - `could not load region` / empty region in Glue calls → confirm `runBench` injected `aws_region` (Plan 2A `main.go`).
  - `kc-1.log` HTTP 4xx on connector submit → the iceberg connector config was rejected; the response body is printed (Plan 3's `--fail`-free submit).
  - Per the bench-framework skill's Debug table for shared symptoms (DNS flake, orphan-TTL, state lock).

- [ ] **Step 5: Teardown** — happy path auto-destroys (`defer destroy`). If hung: `aws-vault exec bench -- task aws:down scenario=iceberg/orders-sink-smoke`. Verify in the console that the Glue database `bench`, the `rpcn-bench-ice-<account>` S3 bucket, and the runner/load-gen instances are gone.

- [ ] **Step 6: Record the numbers** — once green, tune `expected_peak_mb_s` in both scenarios to the observed 1-vCPU/8-vCPU medians and commit the scenario adjustment. Then the full sweep (`scenario=iceberg/orders-sink`) is ready to run when desired (~3-4h, real spend).

---

## Self-Review

**Spec coverage (Plan 2B scope = TF + scenarios + smoke):**
- `modules/glue-iceberg` (S3 + Glue DB + force_destroy) — Task 1. ✓
- `stacks/iceberg` emitting the Plan 2A TF-output contract (`glue_rest_uri`/`warehouse_account_id`/`warehouse_s3_uri`/`s3_bucket`) — Task 2. ✓
- Runner Glue IAM + `iceberg-kafka-connect` plugin install — Task 3. ✓ (`aws_region` correctly NOT added as a TF output — Plan 2A injects it.)
- `scenarios/iceberg/orders-sink.yaml` + `-smoke.yaml`, both clearing the bounded 15-min floor (full: 160M×1200/1MiB/200 ≈ 915s; smoke: 12M×1200/1MiB/15 ≈ 915s) — Task 4. ✓
- 1-vCPU smoke acceptance (two PointResults, both MedianMBPerSec>0, series populated) — Task 5. ✓
- Symmetric Glue access (both engines via the REST endpoint emitted as `glue_rest_uri`) — Tasks 1-2 + Plan 2A's `kcConnectorSpecs["iceberg"]`/`sinkTopology.Pipeline`. ✓

**Placeholder scan:** none — all HCL/YAML is complete. The KC plugin URL is the concrete v0.6.19 artifact the repo's own Dockerfile uses. Sizing numbers are computed against the real validation formula. Task 5's "tune expected_peak_mb_s to observed" is a deliberate post-smoke calibration, not a plan gap.

**Consistency with Plan 2A:** TF output keys match exactly what `sinkTopology`/`substitutePlaceholders` read (`glue_rest_uri`→`${GLUE_REST_URI}`, etc.); Glue database name `bench` matches `sinkSpecs["iceberg"].Namespace`; the KC connector class in the plugin matches `kcConnectorSpecs["iceberg"].Class` and `RequiredPlugins: ["iceberg-kafka-connect*"]`; the seeder name `json-orders` matches the Plan 2A seeder dir; `warehouse_s3_uri` has no trailing slash (runner appends one).

---

## After Plan 2B

The iceberg sink bench is complete end-to-end. Follow-ups (separate efforts, not in this plan): the **Avro + Schema Registry variant** (`orders-sink-avro.yaml` — re-enable the shared Redpanda SR listener, an `avro-orders` seeder, `AvroConverter`/`schema_registry` on both engines) for the "realistic decode" number; and updating the bench-framework skill's "Sink (any) → Plan 4 TBD" row + adding an iceberg row to the connector/exemplar tables now that the sink path exists.
