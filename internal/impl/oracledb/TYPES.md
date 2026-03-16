# Oracle CDC Type System

## Overview

The `oracledb_cdc` input delivers row data as native Go types via JSON-serialised
message bodies. Downstream consumers calling `AsBytes()` receive JSON with
consistent types regardless of whether the row came from a snapshot or a streaming
(LogMiner) event.

Two independent code paths produce row data:

- **Snapshot** — Standard `database/sql` scanning via `prepSnapshotScannerAndMappers`
  in `replication/snapshot.go`. Each Oracle type maps to a specific `sql.Null*`
  scanner that produces the correct Go type directly (e.g. `NUMBER(10)` → `int64`,
  `BINARY_FLOAT` → `float64`).

- **Streaming** — Oracle LogMiner returns `SQL_REDO` statements (raw SQL text).
  The `sqlredo.Parser` extracts column→value pairs from the AST. Oracle function
  calls (`TO_DATE`, `TO_TIMESTAMP`, `HEXTORAW`) are converted to native Go types
  by the `OracleValueConverter`. All other values in INSERT statements are quoted
  strings in the SQL_REDO text, so they arrive as Go `string` values.

Both paths must produce identical Go types for the same Oracle column. To achieve
this, a **coercion step** in the publish path converts streaming string values to
their proper Go types using column metadata from the schema cache.

## Type Mapping

| Oracle Type | Schema Type | Snapshot Go Type | Streaming Go Type | JSON Wire Format |
|---|---|---|---|---|
| `NUMBER(p≤18, 0)` | Int64 | `int64` | `int64` ¹ | `42` |
| `NUMBER(p>18, 0)` | String | `json.Number` | `json.Number` ¹ | `99999999999999999999` |
| `NUMBER(p, s>0)` | String | `json.Number` | `json.Number` ¹ | `123.456` |
| `NUMBER` (bare) | String | `json.Number` | `json.Number` ¹ | `42` |
| `INTEGER` / `INT` / `SMALLINT` | Int64 ² | `int64` | `int64` ¹ | `42` |
| `FLOAT` | String ² | `json.Number` | `json.Number` ¹ | `1.5` |
| `BINARY_FLOAT` | Float32 | `float64` | `float64` ¹ | `1.5` |
| `BINARY_DOUBLE` | Float64 | `float64` | `float64` ¹ | `3.14` |
| `DATE` | Timestamp | `time.Time` | `time.Time` ³ | `"2024-01-15T10:30:00Z"` |
| `TIMESTAMP` | Timestamp | `time.Time` | `time.Time` ³ | `"2024-01-15T10:30:00.123456Z"` |
| `TIMESTAMP WITH TIME ZONE` | Timestamp | `time.Time` | `time.Time` ³ | `"2024-01-15T10:30:00+05:30"` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | Timestamp | `time.Time` | `time.Time` ³ | `"2024-01-15T10:30:00Z"` |
| `RAW` / `LONG RAW` / `BLOB` | ByteArray | `[]byte` | `[]byte` ³ | `"DEADBEEF"` (base64) |
| `CHAR` / `VARCHAR2` | String | `string` | `string` | `"hello"` |
| `NCHAR` / `NVARCHAR2` | String | `string` | `string` | `"hello"` |
| `CLOB` / `NCLOB` / `LONG` | String | `string` | `string` | `"long text..."` |
| `JSON` | Any | `any` (native) | `string` ⁴ | varies |

### Notes

¹ **Streaming value coercion.** LogMiner's `SQL_REDO` quotes all values in INSERT
statements. The parser correctly treats quoted values as strings (to avoid
misinterpreting a VARCHAR value like `'12345'` as a number). The `coerceStreamingValues`
function in the publish path then converts these strings to the proper Go type using
column metadata from the schema cache. See [Value Coercion](#value-coercion) below.

² **Precision-dependent mapping.** Oracle's `INTEGER`, `INT`, `SMALLINT`, and `FLOAT`
are aliases for `NUMBER` with specific precision/scale. `isNumberType()` routes these
through `oracleNumberToCommonType()` which considers precision and scale. For example,
`INTEGER` (which is `NUMBER(38,0)`) maps to `String` (precision > 18), while
`SMALLINT` (which is `NUMBER(38,0)` as well) also maps to `String`. In practice,
the actual `DATA_PRECISION` and `DATA_SCALE` reported by `ALL_TAB_COLUMNS` determine
the mapping.

³ **Converted by Oracle function calls.** In streaming, date/timestamp values appear
as `TO_DATE(...)`, `TO_TIMESTAMP(...)`, etc., which the `OracleValueConverter` converts
to `time.Time`. Binary values appear as `HEXTORAW(...)`, converted to `[]byte`. These
conversions happen at parse time, before the coercion step.

⁴ **JSON limitation.** In streaming, JSON column values appear as quoted strings in
`SQL_REDO`. There is no way to distinguish a JSON string from a regular string at
parse time, so JSON columns produce `string` in streaming vs `any` (unmarshalled) in
snapshot. This is an accepted limitation — JSON columns are uncommon in CDC workloads.

## Value Coercion

The coercion step runs in `batchPublisher.Publish()` after schema resolution
and before JSON marshalling. It only applies to **streaming events** (INSERT,
UPDATE, DELETE from LogMiner). Snapshot events already have correct Go types
from `sql.Scan`.

### How It Works

1. The schema cache stores a `columnTypeInfo` for each table, containing:
   - `colTypes`: maps column name → `schema.CommonType`
   - `numericCols`: set of column names that are `NUMBER`-type columns mapped to
     `schema.String` (i.e. `NUMBER` with fractional scale or precision > 18)

2. For each column in the streaming event's data map:
   - If the value is not a `string`, skip (already typed by the value converter)
   - Look up the column's `CommonType` from the cache
   - Coerce based on type:

   | Schema Type | Coercion | Result |
   |---|---|---|
   | `Int64` | `strconv.ParseInt(s, 10, 64)` | `int64` |
   | `Float32` / `Float64` | `strconv.ParseFloat(s, 64)` | `float64` |
   | `String` + in `numericCols` | wrap as `json.Number(s)` | `json.Number` |
   | `String` + not in `numericCols` | no-op | `string` |
   | Any other type | no-op | original value |

3. On parse failure (e.g. a corrupt value), a warning is logged and the
   original string value is preserved. This ensures data is never silently
   dropped.

### Why numericCols?

Both `NUMBER(20,5)` and `VARCHAR2` columns map to `schema.String` in the
schema type system. Without additional context, coercion cannot distinguish
between a `NUMBER` column (whose string value `"123.45"` should become
`json.Number("123.45")`) and a `VARCHAR2` column (whose string value `"123.45"`
should remain a plain string).

The `numericCols` set tracks which `String`-typed columns are actually `NUMBER`
columns. It is populated when the schema is built from `ALL_TAB_COLUMNS` or
from snapshot column metadata.

## Schema Metadata

Each message carries a `schema` metadata field containing a serialised
`schema.Common` object with:

- **Name**: Oracle table name (uppercase)
- **Type**: `schema.Object`
- **Children**: One entry per column with `Name`, `Type` (from type mapping),
  and `Optional: true`
- **Fingerprint**: SHA-256 hash of the schema structure (auto-generated by
  `schema.Common.ToAny()`)

The schema is attached in `batchPublisher.Publish()` and can be consumed by
downstream processors like `schema_registry_encode`.

### Schema Sources

| Phase | Source | Trigger |
|---|---|---|
| Snapshot | `buildColumnMeta()` from `sql.ColumnType` | Every snapshot batch |
| Streaming (cached) | Reused from snapshot seed | Every streaming event |
| Streaming (refresh) | `fetchTableSchema()` from `ALL_TAB_COLUMNS` | When a column in the event is not in the cached schema |
| Startup | `fetchTableSchema()` from `ALL_TAB_COLUMNS` | Pre-fetch during `Connect()` |

### Schema Drift Detection

The schema cache uses **addition-only drift detection**:

- When a streaming event contains a column name not present in the cached schema,
  the cache is refreshed from `ALL_TAB_COLUMNS`.
- This handles `ALTER TABLE ... ADD COLUMN` during streaming.
- Column drops are **not** detected during streaming (events for dropped columns
  simply stop appearing). The cache reflects column drops after a connector restart.
- UPDATE events with partial column sets and DELETE events with empty data maps
  do **not** trigger false drift detections because the check only fires when
  an event key is *not found* in the cache, not when a cache key is missing
  from the event.

### Fingerprint Stability

The schema fingerprint changes only when the column set or types change.
Messages from the same table with the same schema always have the same
fingerprint, regardless of whether they came from snapshot or streaming.
This enables efficient schema caching in downstream processors.

## go-ora Driver Type Names

The go-ora Oracle driver reports non-standard type names via
`sql.ColumnType.DatabaseTypeName()`. Both the snapshot scanner and schema
mapper handle these aliases:

| Oracle Type | go-ora `DatabaseTypeName()` | Standard `ALL_TAB_COLUMNS.DATA_TYPE` |
|---|---|---|
| `BINARY_FLOAT` | `IBFloat` or `BFloat` | `BINARY_FLOAT` |
| `BINARY_DOUBLE` | `IBDouble` or `BDouble` | `BINARY_DOUBLE` |
| `TIMESTAMP WITH TIME ZONE` | `TimeStampTZ` or `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TimeStampeLTZ` or `TimeStampLTZ_DTY` | `TIMESTAMP WITH LOCAL TIME ZONE` |
| `TIMESTAMP` (internal) | `TimeStampDTY` or `TimeStampTZ_DTY` | varies |

The `oracleTypeToCommonType()` function normalises all variants via
`strings.ToUpper()`. The snapshot scanner in `prepSnapshotScannerAndMappers`
lists the exact driver names since its switch is case-sensitive.

## Key Files

| File | Responsibility |
|---|---|
| `schema.go` | Type mapping (`oracleTypeToCommonType`, `oracleNumberToCommonType`), schema cache, drift detection, streaming value coercion (`coerceStreamingValues`) |
| `batcher.go` | Publish path: schema resolution, coercion call, metadata attachment |
| `replication/snapshot.go` | Snapshot scanning (`prepSnapshotScannerAndMappers`), column metadata extraction (`buildColumnMeta`) |
| `logminer/sqlredo/parser.go` | SQL_REDO parsing, value extraction from AST |
| `logminer/sqlredo/valueconverter.go` | Oracle function conversion (`TO_DATE`, `HEXTORAW`, etc.), bare numeric conversion |
| `input_oracledb_cdc.go` | Component registration, config spec, schema pre-fetch on connect |
