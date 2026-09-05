# Connector Field-Coverage Expansion — Design Addendum

**Date:** 2026-06-22
**Branch:** `kafka_converter`
**Extends:** `2026-06-22-kafka-connect-converter-design.md`

## Motivation

Testing against a real Confluent S3 sink config showed the v1 mappers cover only
each connector's identity fields (bucket/region/etc.) and surface everything else
as `# TODO: unmapped field`. Many of those "unmapped" fields are documented,
deterministically-mappable settings (e.g. `flush.size` → batching) — and a few are
Confluent/KC plumbing with no RPCN equivalent that should be dropped quietly rather
than cluttering the review.

This is not a limit of the deterministic approach: each supported connector has a
finite, documented config schema, so we can complete the rule set. Genuinely
open-ended cases (unknown connectors, custom plugin classes, semantically ambiguous
settings) keep the existing TODO + warning fallback.

## Shared helpers (add once, reuse across connectors)

- **`mapBatching(ctx, body, countKey, byteSizeKey, periodMsKey)`** — emits a
  `batching:` block (`count` / `byte_size` / `period`) from whichever KC keys are
  present; converts a `*.ms` period to a duration string. No keys present → emits
  nothing. (Verify the `batching` sub-field names/types against the real output spec
  via the linter; `aws_s3`, `gcp_cloud_storage`, `gcp_bigquery`, `snowflake_streaming`
  all expose the common `batching` policy.)
- **`objectFormatExtension(ctx)`** — reads `format.class`, consumes it, and returns
  the object extension: `*.AvroFormat`→`.avro`, `*.json.JsonFormat`→`.json`,
  `*.parquet.ParquetFormat`→`.parquet`, `*.ByteArrayFormat`→`.bin`; default `.json`.
  Used to fix the object `path` extension (today hardcoded `.json`).
- **`consumeIgnored(ctx, keys...)`** — marks recognized-but-irrelevant KC plumbing
  keys consumed WITHOUT a warning, so they don't appear as TODO noise. (Distinct
  from `Warn`: these are intentionally dropped, documented no-ops.)
- Extend `topicObjectPath(ext string)` to take the extension from
  `objectFormatExtension`.

## Per-connector additions

### S3 sink → `aws_s3`
- `flush.size` → `batching.count`; `rotate.interval.ms` / `rotate.schedule.interval.ms`
  → `batching.period`.
- `format.class` → object path extension (fix the `.json` hardcode). If the format
  implies encoding the data (Avro/Parquet), add a `# TODO` noting an encode step may
  be needed.
- `aws.access.key.id` / `aws.secret.access.key` (if present) → `credentials.id` /
  `credentials.secret`.
- `topics.dir` (if present) → path prefix.
- Silently consume (no RPCN equivalent / KC plumbing): `storage.class`,
  `schema.generator.class`, `schema.compatibility`, `s3.part.size`,
  `s3.compression.type`, `partitioner.class` when `DefaultPartitioner`. A
  non-default `partitioner.class` → keep a `# TODO` (path layout differs).

### GCS sink → `gcp_cloud_storage`
- `flush.size` → `batching.count`; `rotate.interval.ms` → `batching.period`.
- `format.class` → path extension (same helper).
- `topics.dir` → path prefix.
- Silently consume: `gcs.part.size`, `storage.class`, `schema.generator.class`,
  `schema.compatibility`, `partitioner.class`=Default.

### BigQuery sink → `gcp_bigquery`
- Verify available fields against the real `gcp_bigquery` output spec. Map the
  documented ones: credentials/keyfile → `credentials_json` (if present); batching
  knobs (e.g. `queueSize`) → `batching`; `autoCreateTables` → the create-disposition
  field if one exists.
- Silently consume Confluent-internal: `sanitizeTopics`, `allBQFieldsNullable`,
  `schemaRetriever`, `bigQueryRetry*`, etc. (recognized plumbing).

### Snowflake sink → `snowflake_streaming`
- `buffer.count.records` → `batching.count`; `buffer.flush.time` (sec) →
  `batching.period`; `buffer.size.bytes` → `batching.byte_size`.
- `snowflake.private.key.passphrase` → `private_key_pass`.
- Silently consume: `snowflake.topic2table.map` (or → table TODO),
  `behavior.on.null.values`, `jvm.proxy.*`, `value.converter*` (already meta).

### JDBC source → `sql_select`
- `mode` (incrementing/timestamp/bulk) + `incrementing.column.name` /
  `timestamp.column.name` → shape the query / add ordering as a `# TODO` note
  (RPCN `sql_select` polls differently; surface intent rather than guess).
- `query` (if set) → prefer over table, with a TODO.
- Silently consume: `poll.interval.ms`, `batch.max.rows` (→ no direct field),
  `validate.non.null`, `numeric.mapping`.

### JDBC sink → `sql_insert`
- `insert.mode` (insert/upsert/update) → `# TODO` note on the chosen suffix
  (sql_insert is plain INSERT; upsert needs a different query — flag it).
- `batch.size` → `batching.count`.
- `pk.fields` / `pk.mode` → inform the `args_mapping` / `columns` TODO.
- Silently consume: `auto.create`, `auto.evolve`, `quote.sql.identifiers`.

### MirrorMaker → `kafka_franz`
- `source.cluster.security.protocol` / `sasl.*` / `ssl.*` (if present) → `tls` /
  `sasl` blocks on the input; target equivalents on the output (best-effort + TODO,
  since auth secrets won't be in the connector JSON).
- Silently consume: `replication.factor`, `sync.topic.configs.enabled`,
  `refresh.topics.enabled`, `emit.heartbeats.enabled`, `emit.checkpoints.enabled`.

## Guardrails

- Every change must keep `assertValidRPCN` passing (real benthos schema is the
  source of truth for field names/types — correct the map if the linter rejects a
  field).
- Golden files WILL change (output is now richer). Regenerate with `-update` and
  **manually review every diff** to confirm the new fields/extensions/batching are
  correct and the dropped keys are genuinely plumbing.
- Anything still genuinely unmappable keeps its `# TODO` + warning. Only drop keys
  that are recognized no-ops via `consumeIgnored`.

## Rollout

S3 first (establishes the shared helpers + the ignored-keys convention), reviewed
clean, then GCS / BigQuery / Snowflake / JDBC / MirrorMaker reuse the helpers.
