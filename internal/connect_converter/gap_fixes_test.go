// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// G1 — Oracle JDBC URLs resolve to the oracle driver + an oracle:// DSN
// (previously emitted driver: "" which fails the sql_* driver enum lint).
func TestGapOracleJDBCServiceURL(t *testing.T) {
	y := gapConvert(t, `{"name":"o","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector","connection.url":"jdbc:oracle:thin:@//ohost:1521/ORCLPDB","connection.user":"u","connection.password":"p","table.whitelist":"T","mode":"bulk"}}`)
	assert.Contains(t, y, "driver: oracle")
	assert.Contains(t, y, "dsn: oracle://u:p@ohost:1521/ORCLPDB")
	assert.NotContains(t, y, "unrecognized JDBC URL")
}

func TestGapOracleJDBCSidURL(t *testing.T) {
	y := gapConvert(t, `{"name":"o","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:oracle:thin:@ohost:1521:ORCL","connection.user":"u","connection.password":"p","table.name.format":"T","topics":"t"}}`)
	assert.Contains(t, y, "driver: oracle")
	assert.Contains(t, y, "dsn: oracle://u:p@ohost:1521/ORCL")
}

// G3 — JDBC source must not emit a schema_registry_decode: sql_select reads
// rows directly from the DB, not Avro-encoded Kafka bytes.
func TestGapJDBCSourceNoSchemaDecode(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector","connection.url":"jdbc:postgresql://h:5432/db","table.whitelist":"orders","value.converter":"io.confluent.connect.avro.AvroConverter","value.converter.schema.registry.url":"http://sr:8081"}}`)
	assert.NotContains(t, y, "schema_registry_decode")
}

// G4 — JDBC sink upsert builds a dialect-aware ON CONFLICT suffix and derives
// columns/args from fields.whitelist.
func TestGapJDBCSinkUpsertPostgres(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:postgresql://h:5432/db","table.name.format":"orders","insert.mode":"upsert","pk.mode":"record_key","pk.fields":"id","fields.whitelist":"id,name,email","topics":"orders"}}`)
	assert.Contains(t, y, "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email")
	assert.Contains(t, y, "root = [ this.id, this.name, this.email ]")
}

func TestGapJDBCSinkUpsertMySQL(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:mysql://h:3306/db","table.name.format":"orders","insert.mode":"upsert","pk.fields":"id","fields.whitelist":"id,qty","topics":"orders"}}`)
	assert.Contains(t, y, "ON DUPLICATE KEY UPDATE qty = VALUES(qty)")
}

// G2 — Debezium tables is never an empty list entry; an exclude-list-only
// capture emits a non-empty placeholder so the *_cdc input lints.
func TestGapDebeziumMySQLExcludeOnlyTables(t *testing.T) {
	y := gapConvert(t, `{"name":"d","config":{"connector.class":"io.debezium.connector.mysql.MySqlConnector","database.hostname":"h","database.user":"u","database.password":"p","database.dbname":"shop","table.exclude.list":"shop.audit_log","topic.prefix":"shop"}}`)
	assert.Contains(t, y, "schema.table")
	assert.NotContains(t, y, `- ""`)
}

// G7 — MirrorMaker DefaultReplicationPolicy prefixes the target topic with the
// source alias; IdentityReplicationPolicy keeps the name.
func TestGapMirrorDefaultPolicyPrefix(t *testing.T) {
	y := gapConvert(t, `{"name":"m","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector","source.cluster.alias":"primary","target.cluster.alias":"dr","source.cluster.bootstrap.servers":"s:9092","target.cluster.bootstrap.servers":"t:9092","topics":"orders","replication.policy.class":"org.apache.kafka.connect.mirror.DefaultReplicationPolicy"}}`)
	assert.Contains(t, y, "topic: primary.${! @kafka_topic }")
}

func TestGapMirrorIdentityPolicy(t *testing.T) {
	y := gapConvert(t, `{"name":"m","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector","source.cluster.alias":"primary","target.cluster.alias":"dr","source.cluster.bootstrap.servers":"s:9092","target.cluster.bootstrap.servers":"t:9092","topics":"orders","replication.policy.class":"org.apache.kafka.connect.mirror.IdentityReplicationPolicy"}}`)
	assert.Contains(t, y, "topic: ${! @kafka_topic }")
	assert.NotContains(t, y, "primary.${! @kafka_topic }")
}

// G10 — ByteArrayConverter is a recognized no-op: no decode, no warning.
func TestGapByteArrayConverter(t *testing.T) {
	res, err := Convert([]byte(`{"name":"s","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","s3.region":"us-east-1","topics":"t","value.converter":"org.apache.kafka.connect.converters.ByteArrayConverter"}}`))
	require.NoError(t, err)
	y := string(res.YAML)
	assert.NotContains(t, y, "schema_registry_decode")
	for _, w := range res.Warnings {
		assert.NotContains(t, w.Message, "unsupported value converter")
	}
}

// G9 — DailyPartitioner becomes a year/month/day time-bucketed path prefix.
func TestGapS3DailyPartitioner(t *testing.T) {
	y := gapConvert(t, `{"name":"s","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","s3.region":"us-east-1","topics":"t","partitioner.class":"io.confluent.connect.storage.partitioner.DailyPartitioner"}}`)
	assert.Contains(t, y, `year=`)
	assert.Contains(t, y, `day=`)
	assert.Contains(t, y, `ts_format("2006")`)
	assert.NotContains(t, y, "partitioner.class")
}

// G8 — BigQuery derives a topic-based table instead of an empty stub.
func TestGapBigQueryTopicDerivedTable(t *testing.T) {
	y := gapConvert(t, `{"name":"b","config":{"connector.class":"com.wepay.kafka.connect.bigquery.BigQuerySinkConnector","project":"p","defaultDataset":"d","topics":"orders"}}`)
	assert.Contains(t, y, "table: ${! @kafka_topic }")
}

// G12 — Debezium Postgres folds database.sslmode into the DSN.
func TestGapDebeziumPostgresSSLMode(t *testing.T) {
	y := gapConvert(t, `{"name":"d","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"h","database.user":"u","database.password":"p","database.dbname":"db","table.include.list":"public.t","slot.name":"s","database.sslmode":"require"}}`)
	assert.Contains(t, y, "?sslmode=require")
	assert.NotContains(t, y, "unmapped field database.sslmode")
}
