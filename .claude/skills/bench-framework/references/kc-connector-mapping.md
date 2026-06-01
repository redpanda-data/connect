# Kafka Connect connector mapping

For every Redpanda Connect connector that has a comparison bench, pick the right Kafka Connect plugin. The mapping lives in `benchmarking/aws/runner/kcconnectors.go::kcConnectorSpecs`.

## Installed plugins

The runner's cloud-init installs these:

| Plugin | Class | Direction | Source |
|--------|-------|-----------|--------|
| Debezium PostgreSQL 2.7.x | `io.debezium.connector.postgresql.PostgresConnector` | Source (CDC) | Debezium Apache 2.0 |
| Debezium MySQL 2.7.x | `io.debezium.connector.mysql.MySqlConnector` | Source (CDC) | Debezium Apache 2.0 |
| Aiven JDBC Sink 6.10.0 | `io.aiven.connect.jdbc.JdbcSinkConnector` | Sink | Aiven Apache 2.0 |
| Aiven JDBC Source 6.10.0 | `io.aiven.connect.jdbc.JdbcSourceConnector` | Source (polling) | Aiven Apache 2.0 |
| Confluent S3 Sink (deferred to Plan 4) | TBD | Sink | — |

## Picking the plugin for a new connector

| Redpanda Connect connector | KC plugin | Why |
|----------------------------|-----------|-----|
| `postgres_cdc` | Debezium PostgreSQL | Both speak logical replication / pgoutput |
| `mysql_cdc` | Debezium MySQL | Both speak binlog |
| `sqlserver_cdc` | Debezium SQL Server (NOT INSTALLED — add to cloud-init) | Both speak CDC table tail |
| `mongo_cdc` | Debezium MongoDB (NOT INSTALLED) | Both speak change-stream / oplog |
| Postgres polling-source | Aiven JDBC Source | Both poll a query |
| S3 sink | Confluent S3 Sink (Plan 4 placeholder) | Both write objects to S3 |
| Iceberg sink | Tabular Iceberg sink (Plan 4 placeholder) | Both write Iceberg tables |
| Kafka → Kafka MM | MirrorMaker 2 (bundled but not exercised) | Both replicate topics |

**For CDC sources without a Debezium equivalent**, polling via Aiven JDBC Source is a fallback but is NOT a fair comparison — Connect's CDC reads the replication stream directly while JDBC Source uses timestamp/incrementing-key polling.

## Adding a new KC plugin (cloud-init step)

If the connector you're benching needs a plugin not in the list above:

1. Find the Confluent Hub / GitHub release URL for the connector's `*.zip` distribution.
2. In `benchmarking/aws/terraform/shared/runner-user-data.tftpl`, add a step in the cloud-init runcmd to download + unzip into `/opt/kafka/plugins/<name>/`.
3. Restart the kafka-connect systemd unit (`systemctl restart kafka-connect`) so KC's plugin scan picks it up.
4. Verify on the runner: `curl -s localhost:8083/connector-plugins | jq -r '.[].class'` must include the new connector's class.
5. Add the class + props template to `kcConnectorSpecs` in `runner/kcconnectors.go`.

## kcConnectorSpec template gotchas

When you add an entry to `kcConnectorSpecs`:

1. **`Direction`** must be `kcSource` or `kcSink`. Affects how the reset hooks treat topic teardown.
2. **`PropsTemplate`** is `text/template`-rendered. Inputs are documented next to `renderKCConfig` (the renderer in `runner/kcconnectors.go`). For Debezium SQL Server, you'll likely need `Host/Port/User/Password/Database/SchemaTables/TopicPrefix` (same shape as Postgres).
3. **`snapshot.mode`** — copy MySQL's reasoning: if your engine requires a snapshot to bootstrap an offset, use `no_data` so the schema is captured without the rows. If your engine can stream from current position without an offset (like pgoutput), use `never`.
4. **`RequiredPlugins`** is a `glob` list checked against `curl :8083/connector-plugins`. Must match the installed plugin's class glob (e.g. `debezium-connector-sqlserver*`).
