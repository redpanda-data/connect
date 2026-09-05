# Debezium → Redpanda Connect CDC — Design Addendum

**Date:** 2026-06-23
**Branch:** `kafka_converter`
**Extends:** the connector-mapper framework in `internal/connect_converter`.

## Motivation

Kafka Connect's **Debezium** source connectors are log-based CDC (read the WAL /
binlog; capture insert/update/delete). They correspond to Redpanda Connect's
`*_cdc` inputs — NOT to the JDBC source connector (which is query polling and
correctly maps to `sql_select`). This addendum adds a Debezium connector family
mapping each Debezium connector to its RPCN CDC input.

## Mapping

| Debezium `connector.class` | RPCN input |
|---|---|
| `io.debezium.connector.postgresql.PostgresConnector` | `postgres_cdc` |
| `io.debezium.connector.mysql.MySqlConnector` | `mysql_cdc` |
| `io.debezium.connector.sqlserver.SqlServerConnector` | `microsoft_sql_server_cdc` |
| `io.debezium.connector.oracle.OracleConnector` | `oracledb_cdc` |

MongoDB (`io.debezium.connector.mongodb.MongoDbConnector`) has no RPCN
`mongodb_cdc` equivalent — leave it unregistered so it degrades to the annotated
`drop` stub (do not invent a mapping).

All four produce a `Component{Input: <cdc>}` (they are sources). `assemble` emits
an output TODO stub, as for other source connectors.

## Field mapping (best-effort; verify every field/required against the real spec)

Debezium connectors share a common config shape. Read the real `*_cdc` input
specs (`internal/impl/postgresql`, `internal/impl/mysql`,
`internal/impl/sqlserver` / `microsoft`, `internal/impl/oracle`) for exact field
names and which are required — the linter (`assertValidRPCN`) is the source of
truth; correct the map where it rejects a field, and TODO-stub any required field
with no Debezium source.

Common Debezium → CDC mappings:
- **DSN:** build the connection string from `database.hostname`, `database.port`,
  `database.user`, `database.password`, `database.dbname` (Postgres/Oracle/SQL
  Server). Postgres → `postgres://user:password@host:port/dbname`; MySQL →
  `user:password@tcp(host:port)/dbname`; adjust to whatever DSN format each
  `*_cdc` component documents. Annotate the password with a `# TODO` (it's a
  secret that probably shouldn't be inlined).
- **Tables:** `table.include.list` (comma list of `schema.table` / `db.table`) →
  the component's `tables`/`table` field (a list). `schema.include.list` →
  `schemas` if the component has it.
- **Postgres specifics:** `slot.name` → `slot_name`; `publication.name` →
  `publication_name` (if the field exists); `snapshot.mode` → the component's
  `snapshot`/snapshot-mode field if there's a clean equivalent, else TODO.
- **MySQL specifics:** `database.server.id` → the component's server-id field if
  present, else TODO.
- **consumeIgnored** recognized Debezium plumbing with no CDC equivalent, e.g.:
  `topic.prefix` / `database.server.name` (Debezium topic naming — surface as a
  TODO rather than silently drop, since it carries intent), `tombstones.on.delete`,
  `decimal.handling.mode`, `time.precision.mode`, `heartbeat.interval.ms`,
  `max.batch.size` (→ batching if the component supports it, else ignore). Only
  silence fields with genuinely no equivalent; otherwise TODO.
- `key.converter*` / `value.converter*` are already meta (handled by the converter
  layer); `transforms.*` by the SMT layer.

Anything not cleanly mappable keeps a `# TODO` + warning. Required fields with no
Debezium source get a TODO stub so output still lints.

## Tests & goldens

- `conn_debezium_test.go`: one test per registered connector — a realistic
  Debezium config → assert the correct `*_cdc` input component, the constructed
  `dsn`, and `tables` from `table.include.list`; `assertValidRPCN` passes. A test
  that MongoDB Debezium falls through to the `drop` stub (unregistered).
- Golden cases `debezium_postgres` and `debezium_mysql` (at least) added to the
  golden suite; READ the generated YAML to confirm correctness. No existing golden
  changes (new fixtures only).

## Out of scope

- Debezium MongoDB (no RPCN equivalent).
- Per-table column/key mapping beyond the include lists.
- Debezium SMT unwrap (`ExtractNewRecordState`) — handled generically by the SMT
  layer if/when added; not part of this addendum.
