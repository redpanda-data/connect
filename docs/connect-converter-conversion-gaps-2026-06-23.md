# Kafka Connect → Redpanda Connect Converter — Conversion Gap Report

**Date:** 2026-06-23 · **Branch:** `kafka_converter` @ `b312b8640`
**Method:** 10 dynamically generated Kafka Connect configs (REST-wrapped JSON) covering
every supported connector family plus a broad SMT/converter mix and deliberate edge cases,
each run through the real `connectconverter.Convert` engine **and** validated against the
live benthos stream builder (`service.NewStreamBuilder().SetYAML`).

**Headline result:** all 10 configs converted without a hard error and **all 10 produced
lint-valid RPCN YAML**. The gaps below are *semantic* — places where the emitted YAML is
valid but wrong, lossy, or a graceful TODO stub for an unsupported pattern.

---

## Test matrix

| # | Connector class | Converter | SMTs exercised | Result |
|---|---|---|---|---|
| 01 | `io.confluent.connect.s3.S3SinkConnector` | Avro | RegexRouter, InsertField$Value(timestamp.field) | lint-valid; gaps 1,5,11,15,17 |
| 02 | `io.aiven.kafka.connect.gcs.GcsSinkConnector` | Json | ReplaceField$Value(blacklist) | lint-valid; gaps 6,12,18 |
| 03 | `com.wepay…BigQuerySinkConnector` | Avro | ExtractField$Value, TimestampConverter$Value(format) | lint-valid; gap 1 |
| 04 | `com.snowflake…SnowflakeSinkConnector` | SnowflakeJson | — | lint-valid; gap 13 |
| 05 | `io.confluent.connect.jdbc.JdbcSourceConnector` | (JSON-Schema) | — | lint-valid; gaps 4,10 |
| 06 | `io.confluent.connect.jdbc.JdbcSinkConnector` | Avro | Cast$Value | lint-valid; gaps 3,4 |
| 07 | `io.debezium…PostgresConnector` | Avro | ExtractNewRecordState | lint-valid; gaps 2,9 |
| 08 | `io.debezium…MySqlConnector` | Json | ValueToKey, MaskField$Value | lint-valid; clean (expected CDC TODOs) |
| 09 | `org.apache.kafka.connect.mirror.MirrorSourceConnector` | — | — | lint-valid; gap 14 |
| 10 | `io.confluent.connect.elasticsearch.*` (unsupported) | Json | HoistField$Value, Cast$Key, Filter+predicate | lint-valid; gaps 7,8,16 |

Inputs live in `/tmp/kc-probe/*.json`; the throwaway harness was `zz_probe_test.go`
(`go test ./internal/connect_converter/ -run TestProbeGaps -v`).

---

## A. Correctness bugs — valid YAML, wrong behavior

### Gap 1 — TimestampConverter ignores the `format` property *(common, high impact)*
`smt_timestampconverter.go:37,41` hardcode the Go layout `2006-01-02T15:04:05Z07:00`
(RFC3339) for every `target.type` of `string`/`Timestamp`/`Date`/`Time`. The SMT's
`format` prop (a Joda/SimpleDateFormat pattern, e.g. `yyyy-MM-dd HH:mm:ss`) is **never
read**, so config 03's `ts_parse("2006-01-02T15:04:05Z07:00")` will fail on the actual
`yyyy-MM-dd HH:mm:ss` values. Fix: translate the Joda pattern → Go layout (at least the
common tokens) and emit it; fall back to TODO only on unrecognised patterns.

### Gap 2 — Debezium connectors emit a spurious `schema_registry_decode` *(high impact)*
`consumeDebeziumCommon` (conn_debezium.go) does **not** consume `key.converter` /
`value.converter` / `value.converter.schema.registry.url`, so those fall through to the
shared converter layer. With an Avro converter (config 07) the pipeline gets a
`schema_registry_decode` processor inserted **into a `postgres_cdc` stream**. CDC inputs
read already-structured rows straight from the WAL/binlog — there are no Avro-encoded
Kafka bytes to decode, so this processor is at best a no-op and at worst breaks the
stream. (Config 08 escaped only because JsonConverter maps to a no-op.) Fix: Debezium
mappers should consume `*.converter*` and emit **no** converter processors — the
converter describes Debezium's *Kafka* serialization, which is irrelevant to direct CDC.

### Gap 3 — Cast to integer types produces a float *(minor, lossy)*
`castMethod` maps `int8/16/32/64` → Bloblang `.number()`, which yields a float
(config 06: `root.qty = this.qty.number()`). For integer targets prefer `.int64()` (or
`.number().floor()`) so downstream typing/SQL column types stay correct.

## B. Silent data loss

### Gap 4 — JDBC drops `connection.user` / `connection.password` *(high impact, inconsistent)*
`driverAndDSN` (conn_jdbc.go) builds the DSN from `connection.url` only and never reads
the credential fields, so configs 05 & 06 emit an unauthenticated DSN
(`postgresql://db:5432/sales`) and surface user/password merely as "unmapped field"
warnings. This is inconsistent with the Debezium mappers, which **do** inline credentials
into the DSN. Fix: fold `connection.user`/`connection.password` into the DSN userinfo (with
the same "move to a secret" TODO the Debezium path emits).

## C. Coverage gaps — graceful TODO, but common patterns unsupported

### Gap 5 — InsertField only supports `static.field`/`static.value`
`smt_insertfield.go` ignores `timestamp.field`, `topic.field`, `partition.field`,
`offset.field`, `key.field` → emits a `root = this` passthrough (config 01, very common
"add ingest timestamp" pattern). All of these are mappable to RPCN metadata
(`timestamp_unix()`, `@kafka_topic`, `metadata("kafka_partition")`, …).

### Gap 6 — ReplaceField misses legacy `blacklist`/`whitelist` and `include` projection
`smt_replacefield.go` reads only `renames` and `exclude`. The legacy aliases
`blacklist`/`whitelist` (config 02 used `blacklist`) and the modern `include` (whitelist
projection) are unsupported → passthrough. `blacklist` is trivially mappable to the same
`deleted()` lines as `exclude`. **Also (Gap 18):** the fallback comment/warning says
"include/whitelist semantics" even when the input was a blacklist — misleading.

### Gap 7 — Cast whole-value/whole-key form (`spec=string`) unsupported
`smt_cast.go` only parses `field:type` pairs; a bare `spec=string` (cast the entire
value/key, config 10 `Cast$Key`) has no colon, is skipped, and yields a passthrough stub.

### Gap 8 — Filter SMT and the predicates framework are entirely unsupported
Config 10's `Filter` SMT → "unsupported SMT" passthrough, and the whole `predicates.*`
block (`RecordIsTombstone`, `transforms.<x>.predicate`) is dropped as unmapped. Filtering
(esp. tombstone dropping) is a routine pattern; RPCN can express it via a `mapping` +
`filter`/`drop`-style branch.

### Gap 9 — Debezium `ExtractNewRecordState` → misleading "map manually" TODO
The canonical Debezium unwrap SMT (config 07) is treated as a generic unsupported SMT.
Since RPCN `*_cdc` inputs already emit unwrapped row state, the right behavior is to
recognise it and emit an informational note ("RPCN *_cdc already emits unwrapped
records; this SMT is a no-op") rather than telling the user to map it by hand.

### Gap 10 — Confluent JSON-Schema converter family unregistered
Only `org.apache.kafka.connect.json.JsonConverter` is registered. Confluent's
`io.confluent.connect.json.JsonSchemaConverter` (and the JSON-Schema serializer family)
is not, so it falls to "unsupported value converter" (config 05). Register it
(JSON-Schema → `schema_registry_decode` with the JSON-Schema codec, or no-op + TODO).

### Gap 11 — Object-store partitioner not translated
S3/GCS `partitioner.class` (e.g. `TimeBasedPartitioner`), `path.format`,
`partition.duration.ms`, `locale`, `timezone` (config 01) are all dropped as unmapped.
The emitted `path:` uses `timestamp_unix()` instead of a `year=/month=/day=` interpolation
derived from `path.format`. At minimum, translate `TimeBasedPartitioner` + `path.format`
into a Bloblang-interpolated path.

### Gap 12 — Aiven object-store format/compression/rollover fields dropped
`format.output.type` (jsonl/csv/parquet), `file.compression.type` (gzip), and
`file.max.records` (config 02) are unmapped. `file.max.records` → `batching.count` and
the compression/format → output codec are direct, available mappings.

### Gap 13 — Snowflake multi-table `topic2table.map` collapses to the first table
`snowflake_streaming` has a single `table` per output, so config 04's
`events:tbl_events,metrics:tbl_metrics` keeps only `tbl_events` (with a TODO). Multi-table
fan-out would need a `switch` output or one output per table — currently lossy.

### Gap 14 — MirrorMaker `topics` regex emitted as a literal topic
`topics: orders.*` (a MM2 regex) is rendered as a literal entry in the redpanda input
`topics:` list (config 09), which won't match `orders.*` as a pattern. Should use
`regexp_topics: true` (or the redpanda input's regex form).

### Gap 15 — Object-store sinks decode but never re-encode
With an Avro value converter, the pipeline gets `schema_registry_decode` but the S3/GCS
output writes `.avro`-named objects with no re-encode step (config 01 carries a TODO).
Net result is JSON content under an `.avro` extension unless the user adds an encode step.

### Gap 16 — `topics.regex` sinks / unknown connectors get no input synthesis
Sink input synthesis only fires for **known** connector classes and only from a literal
`topics` CSV. Config 10 (unknown class + `topics.regex`) falls back to a `stdin` stub.
Known follow-up; `topics.regex` → `regexp_topics` is still unhandled.

## D. Cosmetic / known follow-ups
- **Gap 17:** RegexRouter emits `metadata("kafka_topic").re_replace_all(...)`; `@kafka_topic`
  is the more idiomatic form (already noted in the SDD ledger).
- **Gap 18:** see Gap 6 — ReplaceField warning text mislabels blacklist input.

## E. Connector families NOT exercised by these 10 (for completeness)
Documented as untested here, not as gaps — all are registered:
Debezium **SQLServer** & **Oracle** (unit-tested only, no goldens), Snowflake **Streaming**
alias, **Protobuf** converter, Aiven **S3** alias. MongoDB Debezium remains intentionally
unmapped (no `mongodb_cdc` input).

---

## Suggested priority order for fixes
1. **Gap 2** (Debezium spurious decode) — silently breaks Avro CDC pipelines.
2. **Gap 4** (JDBC dropped credentials) — pipeline can't authenticate; inconsistent with Debezium.
3. **Gap 1** (TimestampConverter ignores `format`) — silently wrong timestamp parsing.
4. **Gap 6 / Gap 5** (ReplaceField blacklist, InsertField timestamp.field) — cheap, very common SMT patterns currently no-op'd.
5. **Gap 7 / Gap 3** (whole-value Cast, integer cast type) — cheap SMT correctness.
6. Remaining coverage gaps (8–16) as connector-by-connector enhancements.
