# MySQL CDC Type System

## Overview

The `mysql_cdc` input delivers row data as native Go types via `SetStructuredMut`.
Downstream consumers calling `AsStructured()` (e.g. `parquet_encode`) receive typed
values directly. Consumers calling `AsBytes()` get lazily-marshaled JSON.

Two independent code paths produce row data:

- **CDC** — The go-mysql canal library decodes binlog events into Go values.
  `mapMessageColumn` normalizes these (e.g. int8 → int32) so the Go type matches
  the declared schema type.

- **Snapshot** — Standard `database/sql` scanning via `prepSnapshotScannerAndMappers`.
  Each column type maps to a specific `sql.Null*` scanner that produces the
  matching Go type directly.

Both paths must produce identical Go types for the same MySQL column. The schema
(exposed as message metadata) reflects these types so downstream processors can
rely on them.

## Type Mapping

| MySQL Type | Schema Type | CDC Go Type | Snapshot Go Type |
|---|---|---|---|
| TINYINT | Int32 | int32 | int32 |
| SMALLINT | Int32 | int32 | int32 |
| MEDIUMINT | Int32 | int32 | int32 |
| INT | Int32 | int32 | int32 |
| UNSIGNED TINYINT | Int32 | int32 | int32 |
| UNSIGNED SMALLINT | Int32 | int32 | int32 |
| UNSIGNED MEDIUMINT | Int32 | int32 | int32 |
| UNSIGNED INT | Int64 | int64 | int64 |
| BIGINT | Int64 | int64 | int64 |
| UNSIGNED BIGINT | Int64 | int64 | int64 |
| YEAR | Int32 | int32 | int32 |
| FLOAT | Float32 | float32 | float32 |
| DOUBLE | Float64 | float64 | float64 |
| DECIMAL / NUMERIC | String | string | string |
| DATE | Timestamp | time.Time | time.Time |
| DATETIME | Timestamp | time.Time | time.Time |
| TIMESTAMP | Timestamp | time.Time | time.Time |
| TIME | String | string | string |
| BIT | Int64 | int64 | int64 |
| CHAR / VARCHAR / TEXT | String | string | string |
| BINARY / VARBINARY / BLOB | ByteArray | []byte | []byte |
| ENUM | String | string | string |
| SET | Array[String] | []any | []any |
| JSON | Any | (native) | (native) |

### Notes

- **Integer width**: BIGINT and UNSIGNED INT use Int64 because their max values
  exceed int32 range. All other integer types fit in int32.
- **DECIMAL**: Represented as strings to preserve arbitrary precision. Using
  float64 would silently lose digits.
- **JSON**: Both paths run `json.Unmarshal`, producing a tree of stdlib types
  (`map[string]any`, `[]any`, `float64`, `string`, `bool`, `nil`). No raw
  `sql.*` wrappers leak through.
- **Zero datetimes**: CDC delivers invalid datetimes (e.g. `"0000-00-00 00:00:00"`)
  as strings. `mapMessageColumn` converts these to `nil`.
- **UNSIGNED BIGINT > MaxInt64**: Values exceeding `math.MaxInt64` are passed
  through as `uint64`. This is an edge case that most downstream consumers
  won't encounter.

## Key Files

- `schema.go` — MySQL column type → schema type mapping (`mysqlColumnToCommon`)
- `input_mysql_stream.go` — CDC normalization (`mapMessageColumn`) and snapshot
  scanning (`prepSnapshotScannerAndMappers`)
