# Kafka Connect → Redpanda Connect Converter — Per-Connector Gap Report

**Date:** 2026-06-23 · **Branch:** `kafka_converter` @ `7d1aa5812`
**Scope:** every connector the converter supports, **10 configs per connector** (110 total).

> **STATUS (update): all gaps below were subsequently fixed in this branch.** A
> re-run of the same 110-config sweep now reports **0 convert errors and 0 lint
> failures** (was 6), and the noise/correctness gaps (G1–G15) are addressed. See
> the regression tests in `internal/connect_converter/gap_fixes_test.go`. This
> document is retained as the analysis that motivated the fixes.

## Method

For each of the 11 supported connectors, 10 Kafka Connect connector configs (REST-wrapped
JSON) were generated **blind to what the converter supports** — authored purely from
real-world Confluent / Aiven / Debezium / MirrorMaker2 production usage and documentation,
not from the converter's SMT/connector registry. This is deliberate: tests written against
the support matrix only prove the matrix; tests written against *reality* expose gaps.

Every config was run through the real `connectconverter.Convert` engine and the emitted YAML
was validated against live Redpanda Connect component schemas via
`service.NewStreamBuilder().SetYAML` (the same `assertValidRPCN` harness the unit tests use).

- Inputs: `/tmp/kc-sweep/<connector>/01.json … 10.json` (+ `index.md` per connector describing each scenario).
- Harness: `internal/connect_converter/zz_sweep_test.go` (throwaway, gated on `KC_SWEEP=1`).
- Raw output (lint status, warnings, TODO markers, full YAML for all 110): `/tmp/kc-sweep/REPORT.md`.
- Reproduce: `KC_SWEEP=1 go test ./internal/connect_converter/ -run TestKCSweep -v`

## Headline results

| Metric | Result |
|---|---|
| Configs converted without a hard error | **110 / 110** |
| Panics | **0** |
| Emitted YAML that is lint-valid RPCN | **104 / 110** |
| Emitted YAML that **fails** RPCN lint (invalid config) | **6 / 110** |

The 6 lint failures collapse into **two** root causes (G1, G2 below). The remaining gaps are
*semantic*: valid YAML that is lossy or wrong. SMT coverage is now broad — across 110 configs
the only genuinely unrecognized SMT was a (hallucinated) `io.debezium.transforms.TombstoneHandler`,
which fell through to the graceful "map manually" stub as designed.

### Regressions fixed since the morning 10-config report (`connect-converter-conversion-gaps-2026-06-23.md`)

Verified fixed by this sweep:
- **Debezium spurious decode (old Gap 2):** no Debezium config emits `schema_registry_decode` — the converter now consumes `value.converter` for CDC. ✅
- **Snowflake multi-table (old Gap 13):** `topic2table.map` fan-out now emits a `table: ${! match @kafka_topic { … } }` expression instead of collapsing to the first table. ✅
- **MirrorMaker regex topics (old Gap 14):** emits `regexp_topics: true`. ✅
- Predicate gating, header SMTs, value-shape SMTs, Debezium SMT family — all exercised and working.

---

## A. Lint-breaking gaps (emitted YAML is INVALID RPCN) — fix first

### G1 — Oracle JDBC URLs are unrecognized → empty `driver` → invalid config *(CRITICAL)*
**Affects:** `jdbc_source` (2/10), `jdbc_sink` (2/10) — every Oracle config.
`driverAndDSN` (`conn_jdbc.go`) does not recognize Oracle JDBC URL forms
(`jdbc:oracle:thin:@//host:1521/SVC`, `…:1521:SID`, RAC SCAN URLs), so it emits
`driver: ""`. The `sql_select`/`sql_insert` `driver` field is a closed enum, and `""` is
rejected:

```
lint errors: (18,1) value  is not a valid option for this field
```

Postgres / MySQL / SQL Server URLs are recognized and lint clean; only Oracle breaks. Fix:
recognize the Oracle URL prefixes and map to the `oracle` driver (confirm `sql_*` supports an
`oracle` driver in this distribution; if not, that is itself the gap to close).

### G2 — Debezium `mysql_cdc` emits an empty `tables` list when capture is defined by exclude/DB-level lists *(CRITICAL)*
**Affects:** `debezium_mysql` (2/10); latent for all Debezium variants.
`mysql_cdc` requires `tables` to contain ≥1 entry. When the source config selects tables via
`database.include.list` (DB-level), regex `table.include.list`, or **only** a
`table.exclude.list` (no include list), the converter can't enumerate a concrete table list
and emits a stub:

```yaml
tables:
  - ""   # TODO: list tables to capture
```
```
lint errors: (10,1) field 'tables' must contain at least one table
```

This is common (capture-everything-except patterns, DB-level capture). Options: emit a
commented placeholder that is still lint-valid, or translate regex include-lists into the
nearest `tables` form and warn. Same risk exists for `postgres_cdc` / `oracledb_cdc` /
`microsoft_sql_server_cdc` whenever no concrete include list is present.

---

## B. Correctness gaps (valid YAML, wrong/misleading behavior)

### G3 — JDBC **source** emits a spurious `schema_registry_decode` *(HIGH — same bug class as the fixed Debezium Gap 2)*
**Affects:** `jdbc_source` 7/10 (every config with an Avro / Protobuf / JSON-Schema value converter).
`sql_select` reads structured rows **directly from the database** — there are no
Avro/Protobuf-encoded Kafka bytes to decode. The JDBC source connector's `value.converter`
describes how rows are *serialized onto Kafka by KC*, which is irrelevant to reading the DB.
Yet the converter inserts `schema_registry_decode` into the pipeline:

```yaml
input:
  sql_select: { … }
pipeline:
  processors:
    - schema_registry_decode: { url: http://schema-registry:8081 }   # ← will fail: rows aren't Avro bytes
```

This was fixed for Debezium (the mapper consumes `value.converter`); the **JDBC source mapper
must do the same** — consume `*.converter*` and emit no deserialization processors.

### G4 — JDBC **sink** does not translate `insert.mode=upsert/update` → emits plain INSERT *(HIGH)*
**Affects:** `jdbc_sink` 6/10 (upsert is the dominant real-world JDBC-sink mode).
For `insert.mode=upsert`/`update` the converter emits `sql_insert` (plain INSERT) with an
empty `suffix` and a TODO to "write an ON CONFLICT or UPDATE suffix manually". The upsert
semantics — the entire point of these configs — are dropped. Related: `pk.mode`/`pk.fields`
are surfaced only as a TODO; `columns` defaults to a placeholder `id` and `args_mapping` to
`root = [ this.id ]`, so the column list/value mapping is never derived from the record.
Consider generating an `ON CONFLICT (<pk.fields>) DO UPDATE` suffix (dialect-aware) for
upsert, and deriving `columns` from `pk.fields` + `fields.whitelist`.

### G5 — Debezium unwrap advanced options (`add.fields`, `add.headers`, `delete.handling.mode`) silently dropped *(MEDIUM)*
**Affects:** `debezium_*` configs using `ExtractNewRecordState` with options (common).
The unwrap SMT is correctly recognized as a no-op for `*_cdc` inputs, but its options that
**add columns/headers to the output** (`add.fields=op,source.ts_ms`, `add.headers=…`) and its
delete handling (`delete.handling.mode=rewrite` → a `__deleted` flag column) are not
translated. KC output would carry those extra fields; the RPCN output will not. At minimum,
warn that `add.fields`/`add.headers`/`delete.handling.mode` change the row shape and are not
reproduced.

### G6 — JDBC source `mode` (incrementing/timestamp/bulk) is not implemented *(MEDIUM)*
**Affects:** `jdbc_source` all 10.
`mode=incrementing`/`timestamp`/`timestamp+incrementing` becomes an empty `suffix` with a TODO
("add ORDER BY / WHERE to replicate incremental polling"); `mode=bulk` likewise. The emitted
`sql_select` therefore does a full-table read with no incremental cursor — functionally
different from the source connector. A custom `query` is dropped into `suffix` (with `table: ""`
still required → see also G1/G2-style required-field friction). Incremental polling is the
core feature of the JDBC source; consider emitting an `ORDER BY <col>` + offset-cursor pattern.

### G7 — MirrorMaker `replication.policy.class` dropped → wrong target topic names *(MEDIUM)*
**Affects:** `mirror_source` all 10.
`DefaultReplicationPolicy` renames mirrored topics to `<source.alias>.<topic>`;
`IdentityReplicationPolicy` keeps the original name. The converter drops
`replication.policy.class`, `replication.policy.separator`, and `source/target.cluster.alias`,
and always writes to the original topic name. For the (default) DefaultReplicationPolicy that
is the **wrong** target topic. Translate the policy into the output topic expression
(`${source.alias}.${! @kafka_topic }` vs identity).

### G8 — BigQuery topic-derived table → empty `table` + dropped upsert/delete/partitioning *(MEDIUM)*
**Affects:** `bigquery_sink` (CDC/regex-topic configs).
With `topics.regex` (table derived from topic) the converter emits `table: ""` (TODO), which
is semantically broken even though it lints. `upsertEnabled`, `deleteEnabled`, `mergeIntervalMs`,
`mergeRecordsThreshold`, `keySource`, time-partitioning/clustering, and `topic2TableMap` are all
dropped. Derive `table` from `@kafka_topic` (mirror the Snowflake `match` approach), and warn
that upsert/delete merge semantics aren't reproduced.

---

## C. Coverage / silent-loss gaps (valid YAML, features dropped as unmapped)

### G9 — Object-store partitioning & encoding not translated *(MEDIUM — S3 & GCS)*
**Affects:** `s3_sink`, `gcs_sink` (every TimeBased/Daily/Hourly/Field partitioner config).
Dropped as unmapped: `partitioner.class`, `path.format`, `partition.duration.ms`,
`timestamp.extractor`, `timezone`, `locale`, `parquet.codec`/`file.compression.type`,
`format.output.fields*`, `file.name.template`. The emitted `path` uses
`${! @kafka_topic }/${! timestamp_unix() }-…` instead of a `year=/month=/day=` layout derived
from `path.format`. Also (carry-over of old Gap 15) object-store sinks **decode but never
re-encode**: an Avro/Parquet config writes a `.parquet`/`.avro`-named object with only a
`# TODO: add an encode step` — content is JSON under the wrong extension. Translate
`TimeBasedPartitioner` + `path.format` into an interpolated path, map `file.compression.type`
to the output codec, and emit the matching encode processor.

### G10 — `ByteArrayConverter` unregistered → "unsupported value converter" noise *(LOW)*
**Affects:** 10 configs (MirrorMaker default; raw-bytes S3/GCS).
`org.apache.kafka.connect.converters.ByteArrayConverter` isn't registered, so every
byte-passthrough config logs `unsupported value converter`. For raw-bytes flows this should be
a **no-op converter** (bytes pass straight through), exactly like `StringConverter`. Register it
as `noopConverter{}` to remove the false warning.

### G11 — `topic.prefix` universally dropped *(LOW-MEDIUM — Debezium + JDBC source)*
**Affects:** ~45 configs (the #1 unmapped field overall).
For Debezium and JDBC source, `topic.prefix` (and legacy `database.server.name`) is the root
of the produced topic names and the basis for RegexRouter rewrites. It's dropped as unmapped
everywhere. It has no direct `*_cdc` equivalent, but silently dropping it on every CDC config
is noisy; consider consuming it with an explanatory note (and use it when a RegexRouter rewrite
references it).

### G12 — Debezium connection/TLS & topology settings dropped *(MEDIUM)*
**Affects:** all Debezium variants.
Dropped as unmapped: `database.sslmode` (PG), `database.encrypt` / `database.trustServerCertificate`
(SQL Server), `database.pdb.name` / `database.connection.adapter` / `database.url` / `log.mining.strategy`
(Oracle), `plugin.name` (PG). Several of these are connection-critical — e.g. Oracle
`database.pdb.name` and the LogMiner/XStream adapter affect *which* DB you connect to, and TLS
mode affects whether the connection succeeds. These should map onto the corresponding `*_cdc`
TLS/connection fields or at least warn loudly rather than being listed among generic unmapped
fields.

### G13 — Debezium `schema.history.internal.kafka.*` flagged as unmapped *(LOW — noise)*
**Affects:** MySQL / SQL Server / Oracle (~28 configs, 56 warning lines).
RPCN `*_cdc` inputs don't use an external Kafka schema-history topic, so these keys are
correctly irrelevant — but they surface as generic "unmapped field" TODOs, adding noise.
Consume them silently with a one-line note ("schema history is internal to `*_cdc`; no Kafka
history topic needed"), the way the Debezium converter handling already silences `*.converter`.

### G14 — Snowflake `enable.schematization` / ingestion-method / buffer nuances dropped *(LOW)*
**Affects:** `snowflake_sink`.
`snowflake.enable.schematization`, `snowflake.streaming.enable.single.buffer`,
`snowflake.ingestion.method` (SNOWPIPE vs SNOWPIPE_STREAMING), and granular `snowflake.metadata.*`
toggles are dropped. The account-URL→identifier mismatch (`…snowflakecomputing.com:443` vs the
bare account id) is correctly flagged with a TODO. Schematization in particular changes the
target table shape and is worth a warning.

### G15 — MirrorMaker `topics.exclude`, `groups`, security, offset-syncs dropped *(LOW-MEDIUM)*
**Affects:** `mirror_source` all 10.
`topics.exclude`/`topics.blacklist` (no negative-filter applied to the input topic set),
`groups` (consumer-group replication — no RPCN equivalent), per-cluster `*.security.protocol` /
`sasl.*` / SSL settings, and `offset-syncs.*` are all dropped. Topic exclusion is the notable
one: a `topics: .*` + `topics.exclude: .*\.internal` config will mirror the excluded topics.
Also minor: `regexp_topics: true` is emitted even when the `topics` list is literal (harmless —
exact names still match — but slightly misleading).

---

## D. Per-connector summary

| Connector | Configs | Lint | Dominant gaps |
|---|---|---|---|
| **S3 sink** | 10 | 10/10 valid | G9 (partitioner/path/encode), G10 (ByteArray) |
| **GCS sink** | 10 | 10/10 valid | G9 (partitioner/format/compression/template), G10 |
| **BigQuery sink** | 10 | 10/10 valid | G8 (topic-derived table empty; upsert/delete/partitioning dropped) |
| **Snowflake sink** | 10 | 10/10 valid | G14 (schematization/ingestion); multi-table ✅, account-URL TODO ✅ |
| **JDBC source** | 10 | 8/10 valid | **G1 (Oracle driver=invalid)**, **G3 (spurious decode)**, G6 (mode not implemented), G11 |
| **JDBC sink** | 10 | 8/10 valid | **G1 (Oracle driver=invalid)**, **G4 (upsert→plain INSERT)**, Flatten passthrough, dialect/whitelist dropped |
| **Debezium Postgres** | 10 | 10/10 valid | G5 (unwrap opts), G11 (topic.prefix), G12 (sslmode/plugin.name) |
| **Debezium MySQL** | 10 | 8/10 valid | **G2 (empty tables)**, checkpoint_cache required, G5, G11, G13 |
| **Debezium SQL Server** | 10 | 10/10 valid | G12 (encrypt/trustServerCertificate; `database.names` multi-DB), G13 |
| **Debezium Oracle** | 10 | 10/10 valid | G12 (pdb.name/adapter/url, LogMiner strategy), G5, G13 |
| **MirrorMaker2 source** | 10 | 10/10 valid | G7 (replication policy → topic names), G15 (exclude/security), G10 |

---

## E. Suggested fix priority

1. **G1** Oracle JDBC URL → `driver` (unblocks 4 invalid configs; trivial URL-prefix mapping).
2. **G2** Debezium empty `tables` from exclude/DB-level capture (invalid config on a common pattern).
3. **G3** JDBC source spurious `schema_registry_decode` (silently breaks 7/10 source pipelines; same fix shape as the Debezium fix already shipped).
4. **G4** JDBC sink upsert/update → emit ON CONFLICT/UPDATE suffix + derive columns (dominant real-world mode).
5. **G7 / G8** MirrorMaker replication-policy topic naming; BigQuery topic-derived table — both produce wrong/empty targets.
6. **G9** Object-store partitioner + re-encode (broad S3/GCS correctness/usability).
7. **G5, G6, G12** semantic completeness (unwrap opts, JDBC polling mode, CDC TLS/connection).
8. **G10, G11, G13, G14, G15** noise reduction & low-impact coverage.

---

*Artifacts retained under `/tmp/kc-sweep/` (110 inputs, per-connector `index.md`, full `REPORT.md`).
Throwaway harness `internal/connect_converter/zz_sweep_test.go` is `KC_SWEEP`-gated and safe to delete.*
