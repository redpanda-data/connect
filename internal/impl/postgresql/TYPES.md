# PostgreSQL CDC Type System

## Overview

The `postgres_cdc` input delivers row data as native Go types via `SetStructuredMut`.
Downstream consumers calling `AsStructured()` (e.g. `parquet_encode`) receive typed
values directly. Consumers calling `AsBytes()` get lazily-marshaled JSON.

Two independent code paths produce row data:

- **CDC** — pgx v5 decodes WAL logical replication messages via `decodeTextColumnData`.
  A normalization switch on the pgtype name adjusts values so the Go type matches
  the declared schema type (e.g. int16 → int32, pgtype.Numeric → string).

- **Snapshot** — Standard `database/sql` scanning via `prepareScannersAndGetters`.
  Each column type maps to a specific `sql.Null*` scanner that produces the
  matching Go type directly.

Both paths must produce identical Go types for the same PostgreSQL column. The schema
(exposed as message metadata) reflects these types so downstream processors can
rely on them.

## Type Mapping

| PG Type | Schema Type | CDC Go Type | Snapshot Go Type |
|---|---|---|---|
| BOOL | Boolean | bool | bool |
| SMALLINT (int2) | Int32 | int32 | int32 |
| INTEGER (int4) | Int32 | int32 | int32 |
| BIGINT (int8) | Int64 | int64 | int64 |
| REAL (float4) | Float32 | float32 | float32 |
| DOUBLE PRECISION (float8) | Float64 | float64 | float64 |
| NUMERIC / DECIMAL | String | string | string |
| TEXT / VARCHAR / CHAR | String | string | string |
| BYTEA | ByteArray | []byte | []byte |
| DATE | Timestamp | time.Time | time.Time |
| TIME | String | string | string |
| TIMETZ | String | string | string |
| TIMESTAMP | Timestamp | time.Time | time.Time |
| TIMESTAMPTZ | Timestamp | time.Time | time.Time |
| UUID | String | string | string |
| JSON / JSONB | Any | (native) | (native) |

### Notes

- **SMALLINT (int2)**: pgx decodes int2 as int16. The CDC normalizer promotes
  this to int32 to match the Int32 schema type.
- **NUMERIC / DECIMAL**: Represented as strings to preserve arbitrary precision.
  The CDC path returns the raw PostgreSQL text representation, bypassing the
  pgtype.Numeric struct.
- **DATE**: Mapped to Timestamp schema type. Both paths return `time.Time`.
  ±infinity dates return `nil`.
- **TIME / TIMETZ**: Returned as raw PostgreSQL text strings. The CDC path
  bypasses pgtype.Time to avoid struct values. Note: timetz (OID 1266) is not
  in pgx's default type map; the CDC path handles it via a `string(data)`
  fallback, and the snapshot path resolves the numeric OID via `resolveTypeName`.
- **TIMESTAMP / TIMESTAMPTZ**: Both paths return `time.Time`. ±infinity
  timestamps return `nil`.
- **JSON / JSONB**: Both paths run `json.Unmarshal`, producing a tree of stdlib
  types (`map[string]any`, `[]any`, `float64`, `string`, `bool`, `nil`). No raw
  `sql.*` wrappers leak through.
- **FLOAT4**: The snapshot path scans via `sql.NullFloat64` and narrows to
  `float32`. The CDC path receives `float32` natively from pgx.

## Key Files

- `pglogicalstream/schema.go` — PG type name → schema type mapping
  (`pgTypeNameToCommonType`), OID fallback (`resolveTypeName`), and schema
  construction for both CDC (`relationMessageToSchema`) and snapshot
  (`columnTypesToSchema`) paths.
- `pglogicalstream/replication_message_decoders.go` — CDC type normalization
  (`decodeTextColumnData`)
- `pglogicalstream/snapshotter.go` — Snapshot scanning
  (`prepareScannersAndGetters`)
