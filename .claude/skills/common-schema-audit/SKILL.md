---
name: common-schema-audit
description: Audit every consumer of the schema.Common metadata format (the format produced by schema_registry_decode's store_schema_metadata, the parquet_decode processor, and CDC sources) for type-coverage drift and value-coercion gaps. Run this whenever a new component starts consuming schema.Common, when a new schema.CommonType variant is added upstream in benthos, or as a periodic maintenance check.
argument-hint: "[--format=md|json] [--component=<name>]"
disable-model-invocation: true
allowed-tools: Bash(go *), Bash(grep *), Bash(find *), Read, Glob, Grep, Task
---

# Common-schema consumer drift audit

`schema.Common` (from `github.com/redpanda-data/benthos/v4/public/schema`) is the canonical type metadata that flows through `meta(schema)` between Avro / Parquet / CDC sources and downstream sinks. Every consumer of this metadata must:

1. **Handle every variant of `schema.CommonType`** — or fail loudly with a useful error that names the missing case, not a generic "unsupported".
2. **Coerce values when the Go type of the message body doesn't match the schema-declared type** — specifically the temporal-to-numeric and numeric-to-temporal bridges that the iceberg shredder implements via `coerceTemporalToNumeric` and the metadata-aware path in `internal/impl/iceberg/shredder/temporal.go:208`.

This skill produces a per-consumer report so reviewers can catch drift before it ships.

## Why this matters

The "GF iceberg issue" was a value-vs-metadata mismatch class. Fixes closed the gap in each consumer:

- `iceberg` output → temporal coerce + numeric metadata-aware scaling
- `parquet_encode` → type coverage for Date/TimeOfDay/UUID/Map, temporal coerce bridges
- `confluent` decoder / metadata parser → field-level logicalType, Debezium connect.name, duration
- `confluent` JSON-Schema encoder → Date/TimeOfDay/UUID

A new consumer of `schema.Common`, or a new `schema.CommonType` variant added upstream in benthos, can re-introduce the same bug class without anyone noticing until a customer pipeline breaks. The audit catches the drift mechanically.

## Workflow

1. **Enumerate the type universe.** Read every `schema.CommonType` constant from the benthos source — the authoritative list of variants every consumer must consider.

   ```bash
   gopath=$(go env GOMODCACHE)
   benthos_dir=$(ls -d $gopath/github.com/redpanda-data/benthos/v4@*/ | tail -1)
   grep -E '^\s*(Boolean|Int32|Int64|Float32|Float64|String|ByteArray|Object|Map|Array|Null|Union|Timestamp|Date|TimeOfDay|UUID|Decimal|BigDecimal|Any)\s+CommonType' "$benthos_dir/public/schema/common.go"
   ```

   Cross-check against the current set (as of the GF issue):
   `Boolean, Int32, Int64, Float32, Float64, String, ByteArray, Object, Map, Array, Null, Union, Timestamp, Date, TimeOfDay, UUID, Decimal, BigDecimal, Any`.

   If new variants appear in benthos that aren't in this list, every consumer below will silently need an additional case — flag it loudly and update the skill's audit list.

2. **Find every consumer.** A "consumer" of `schema.Common` is a code path that reads parsed schema metadata and uses it to drive downstream type decisions. The reliable signal is a `schema.ParseFromAny(...)` call, plus any direct `schema.Common` type switches in encoding/coercion paths.

   ```bash
   grep -rln 'schema\.ParseFromAny\|case schema\.\(Boolean\|Int32\|Int64\|Float32\|Float64\|String\|ByteArray\|Object\|Map\|Array\|Null\|Union\|Timestamp\|Date\|TimeOfDay\|UUID\|Decimal\|BigDecimal\|Any\)\b' internal/impl/ | grep -v _test
   ```

   Producers (CDC schema builders in `mysql/`, `oracledb/`, `postgresql/`, `mongodb/cdc/`, `mssqlserver/`) are *not* consumers in this sense — they construct `schema.Common` from a source database's metadata; the type-coverage question doesn't apply. Filter those out.

3. **Per-consumer audit.** For each consumer, delegate to the Explore agent with the brief below. Run consumers in parallel.

   ```text
   Working dir: <connect repo>

   Audit the consumer at <file>:<function> against the full schema.CommonType variant set:
   Boolean, Int32, Int64, Float32, Float64, String, ByteArray, Object, Map, Array,
   Null, Union, Timestamp, Date, TimeOfDay, UUID, Decimal, BigDecimal, Any.

   Report:
     (a) Type-coverage table: for each variant, which target type the consumer maps to (or whether it errors). Cite file:line.
     (b) Value-coercion handling: when a message value's Go type doesn't match the
         schema-declared type, does the consumer coerce or fail loudly? Specifically
         check these cross-type cases:
           - time.Time value + schema-declared Timestamp + integer-typed target column
           - time.Duration value + schema-declared TimeOfDay + integer-typed target column
           - Numeric int64 value + schema-declared Timestamp + integer-typed target column (unit-aware scaling)
           - Numeric int32 value + schema-declared Date + integer-typed target column
         Cite the coercion function and its location.
     (c) Verdict: COVERED | PARTIAL | GAP, with one-line justification.

   Reference implementations to compare against:
     - iceberg shredder's coerceTemporalToNumeric in internal/impl/iceberg/shredder/temporal.go
     - iceberg shredder's metadata-aware numeric scaling at temporal.go:208 onwards
     - iceberg type_resolver's commonTypeToIcebergTypeRec in internal/impl/iceberg/type_resolver.go

   Under 300 words per consumer.
   ```

4. **Aggregate.** Combine the per-consumer reports into a single matrix:

   ```
   | Consumer | Missing types | Missing coercions | Verdict |
   |---|---|---|---|
   | iceberg | (none) | (none) | COVERED |
   | parquet_encode | … | … | … |
   …
   ```

5. **Recommend.** For each GAP / PARTIAL row, propose the fix shape (port from iceberg, add cases to switch, etc.). Reference implementations to mirror, by file path so the pointers stay valid as the codebase evolves:

   - Type coverage extension pattern: `internal/impl/parquet/processor_encode.go::parquetNodeFromCommonField` and `internal/impl/iceberg/type_resolver.go::commonTypeToIcebergTypeRec` — both have a case for every `schema.CommonType` variant with explicit loud-error arms for shapes the sink cannot express.
   - Temporal-to-numeric coerce: `internal/impl/iceberg/shredder/temporal.go::coerceTemporalToNumeric` — the `time.Time → unit-scaled int64` helper used when the iceberg column is integer-typed but the schema metadata says Timestamp.
   - Numeric-to-temporal scaling: `internal/impl/iceberg/shredder/temporal.go::convertTimestamp` (the `if n, ok := numericInt64(value); ok && common != nil && common.Type == schema.Timestamp` branch) — the metadata-aware unit interpretation for numeric values flowing into time-typed columns.
   - JSON Schema format mapping: `internal/impl/confluent/common_to_json_schema.go::commonToJSONSchemaNode` — the `schema.Date → {format:"date"}` / `TimeOfDay → time` / `UUID → uuid` cases.

## Output format

By default, produce a Markdown report on stdout with these sections, in order:

1. **Variant universe** — the full list of `schema.CommonType` values found, plus a delta vs the canonical list (above) so reviewers spot when benthos adds new variants.
2. **Consumer matrix** — one row per consumer, columns as above.
3. **Detailed findings** — per-consumer block with the Explore agent's report verbatim.
4. **Recommendations** — ranked by impact (a sink that customers actually use comes ahead of an internal-only path).

If `--format=json` is passed, emit a structured JSON document with the same sections; useful for CI.

If `--component=<name>` is passed, audit only that one consumer (matched by directory name under `internal/impl/`).

## Adding new consumers

When adding a new consumer of `schema.Common`:

1. Either add a case for every variant in your type switch, OR explicitly error on unsupported with a message that names which variant and points at the upstream coercion that would close the gap.
2. If your consumer accepts user-provided values, implement the temporal-to-numeric coercion bridge analogous to `coerceTemporalToNumeric`. The customer is going to flip `preserve_logical_types: true` and start sending `time.Time` values; without the bridge you'll crash on shred/encode time.
3. Add an integration test analogous to `internal/impl/iceberg/integration/schema_metadata_timestamp_test.go::TestIntegrationCoerceTemporalIntoExistingBigintColumn` that pre-creates the target with a numeric column type, sends a typed value through, and asserts the coerce path fires correctly.

## Notes

- Producers of `schema.Common` (CDC schema builders) are intentionally out of scope. Their type-mapping coverage is a separate question and varies per source database.
- This skill is read-only. It must not write code or commit changes — its job is to produce the report so a human can prioritise fixes.
- If a consumer's type switch is implemented across multiple files (e.g. iceberg has the switch in `type_resolver.go` plus value handling in `shredder/`), evaluate the consumer as a whole.
- When in doubt, run the existing test suites for the suspected consumer (`go test ./internal/impl/<consumer>/...`) to see what's actually exercised. Coverage gaps in production code rarely have corresponding test coverage.
