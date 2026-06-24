# Iceberg Sink — Pre-created Tables Design

**Date:** 2026-06-02
**Status:** Approved (design)
**Context:** Fix for the iceberg-sink smoke blocker (see memory `iceberg-sink-smoke-findings`).

## Problem

KC's Tabular Iceberg sink fails its task against the AWS Glue Iceberg REST catalog:
`Malformed request: Location information cannot be null while creating an iceberg table`. Its `autoCreateTable` calls `catalog.createTable` without a location, and the Glue REST catalog requires one (it does not derive a location from the database's `LocationUri` — verified live, with a worker restart to clear caches). Connect's `iceberg` output avoids this by passing a location on create (`schema_evolution.table_location`), which is why the Connect side works end-to-end (~0.46 MB/s @ 1 vCPU in the smoke).

## Solution

Pre-create **both** engines' per-engine Iceberg tables with an explicit S3 location and a fixed schema, and disable connector-side auto-create. Both engines then write to identically pre-created tables — which also removes "Connect infers schema vs KC maps to declared" as a fairness variable.

## Components

### `iceberg-tablegen` (new Go tool)
`benchmarking/aws/seeders/iceberg-tablegen/` — a `main` package mirroring the seeder convention (cross-compiled `GOOS=linux GOARCH=arm64`, staged on a bench host, run via SSM). It imports the repo's own `internal/impl/iceberg/catalogx` (same Go module, importable) and `github.com/apache/iceberg-go`, and creates a table via the same path Connect uses:
`catalogx.Client.CreateTable(ctx, table, schema, catalog.WithLocation(location))` — writing proper Iceberg metadata (not a raw Glue stub, which `aws glue create-table` would risk).

- **Flags:** `--catalog-uri`, `--warehouse`, `--region`, `--namespace`, `--table`, `--location`.
- **Auth:** SigV4 to the Glue REST endpoint (`service=glue`), using the default AWS credential chain (the runner's instance-profile Glue IAM). Construct the `catalogx` client with the same catalog/auth params Connect uses.
- **Plan-time verification (risk):** confirm `catalogx` exposes a constructor usable *standalone* (not requiring a `service.ParsedConfig` from the Connect runtime). If it's too coupled to use cleanly, the **fallback** is to construct the REST catalog directly via `github.com/apache/iceberg-go/catalog/rest` with the SigV4 signing options (`signing-name=glue`, region) — the same library catalogx wraps. The plan must pin which of the two is used after reading `catalogx`'s public API.
- **Schema (fixed, matches `json-orders`):** `id` long (required), `ts` string (required), `region` string (required), `amount` double (required), `status` string (required), `payload` string (required).
- **Idempotent:** if the table already exists, treat as success (the reset drops first, so normally it won't).

### Staging on the runner
The bench runner stages `redpanda-connect` + config + license on the runner host. Extend that to also build + stage the `iceberg-tablegen` binary (it needs the runner's Glue IAM, and the reset script — which creates the tables — runs on the runner). Built and uploaded the same way the seeder is, downloaded to `/opt/bench/iceberg-tablegen` on the runner.

### Reset flow (`sinkTopology.ResetScript`)
Runs on the runner before every sweep point. New order:
1. Drop both per-engine tables (`aws glue delete-table`) — existing.
2. **Create both empty tables** (`connect` + `kafka_connect`) via `/opt/bench/iceberg-tablegen` with the fixed schema and location `${WAREHOUSE_S3_URI}/<namespace>/<table>`.
3. Reset both consumer-group offsets to earliest — existing.
4. Idempotent KC connector DELETE — existing.

Each sweep point therefore starts with fresh empty tables (`total-files-size` 0 → grows). The metric sidecar is unchanged (it polls `total-files-size`, which now exists from the start).

### Connector config
- **KC** (`kcConnectorSpecs["iceberg"]`): set `iceberg.tables.auto-create-enabled` to `"false"` (the table pre-exists).
- **Connect** (`sinkTopology.Pipeline`): unchanged — it writes to the now-existing table (its create-if-not-exists becomes a no-op). Keep `schema_evolution.table_location` so behavior is unchanged if the table is somehow absent.

### Bundled fix: KC plugin-readiness race
Independently observed in smoke run 3: the KC bench script submits after "REST API up", but the large iceberg plugin can still be scanning, yielding a spurious "class not found". Fix in `kcscript.go renderKCBenchScript`: after REST-up, poll `localhost:8083/connector-plugins` for the connector's class (extracted from the config JSON) until present, with a bounded timeout, before submitting.

## Fairness

Both engines write to identically pre-created tables (same schema, same location pattern under the shared warehouse, auto-create off on both). The canonical metric (Iceberg `total-files-size` growth, polled from Glue) is unchanged and symmetric.

## Testing

- Unit: `iceberg-tablegen` builds the fixed schema correctly; `sinkTopology.ResetScript` emits the create commands for both engines with the right location; `kcConnectorSpecs["iceberg"]` has `auto-create-enabled=false`; the KC bench script polls for the class before submit.
- Cross-compile: `iceberg-tablegen` builds for `linux/arm64`.
- Live: re-run the 1-vCPU smoke with `--keep-on-fail` (so any residual issue is inspectable). Acceptance: two `PointResult`s, both `MedianMBPerSec > 0` (Iceberg metric), no KC task failure.

## Coupling notes (documented, accepted)

1. `iceberg-tablegen`'s schema must track the `json-orders` record shape (separate files; same 6 fields). A drift would surface as a write/mapping error in the smoke.
2. Connect must accept writing to a pre-existing table — verified in the smoke.

## Out of scope

Tuning `expected_peak_mb_s` to observed throughput (post-smoke calibration); the full multi-vCPU sweep.
