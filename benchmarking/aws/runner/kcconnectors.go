// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

// kcDirection captures whether the connector reads from or writes to the
// shared Redpanda cluster. The matrix runner uses this to attribute the
// broker-side throughput metric to produce-side (sources) or consume-side
// (sinks) traffic. Plan 2 only ships sources.
type kcDirection int

const (
	kcSource kcDirection = iota
	kcSink
)

// kcConnectorSpec describes the Kafka Connect counterpart of a Redpanda
// Connect connector. Each entry pins the connector class, the JSON config
// template (which can reference scenario fields + TF outputs via Go
// text/template syntax), and any plugin globs that should exist on the
// runner host before the connector is submitted.
//
// To add a new connector to the comparison framework, add one entry to
// kcConnectorSpecs below. Touch no other files.
type kcConnectorSpec struct {
	Class           string
	PropsTemplate   string
	Direction       kcDirection
	RequiredPlugins []string
}

// kcConnectorSpecs is the registry of KC counterparts keyed by the Redpanda
// Connect connector name (the same key used in engineSpecs).
var kcConnectorSpecs = map[string]kcConnectorSpec{
	"postgres_cdc": {
		Class:     "io.debezium.connector.postgresql.PostgresConnector",
		Direction: kcSource,
		// PropsTemplate is rendered via Go text/template. The render
		// inputs are documented next to renderKCConfig (Task 8). The
		// JSON shape here is what Debezium 2.7.x expects from the KC
		// REST PUT /connectors/<name>/config endpoint.
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.dbname": "{{.Database}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "table.include.list": "{{.SchemaTables}}",
  "plugin.name": "pgoutput",
  "slot.name": "kc_bench_slot",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "never",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-postgres*"},
	},
	"mysql_cdc": {
		Class:     "io.debezium.connector.mysql.MySqlConnector",
		Direction: kcSource,
		// snapshot.mode=no_data (not "never" like postgres): Debezium MySQL
		// requires either an existing offset OR a snapshot to know where
		// to start streaming. Plan 3's per-vCPU connector names give each
		// sweep point a fresh connector with no previous offset, so
		// "never" fails the task before warmup. "no_data" snapshots the
		// schema only (table is TRUNCATEd between sweep points → no rows
		// to capture) then streams from current binlog. Postgres Debezium
		// is forgiving here because pgoutput can stream from current WAL
		// position without an offset; MySQL is stricter.
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.server.id": "184054",
  "database.include.list": "{{.Database}}",
  "table.include.list": "{{.SchemaTables}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "schema.history.internal.kafka.bootstrap.servers": "{{.BootstrapServers}}",
  "schema.history.internal.kafka.topic": "_kc_schema_history_{{.TopicPrefix}}",
  "snapshot.mode": "no_data",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-mysql*"},
	},
	"oracledb_cdc": {
		Class:     "io.debezium.connector.oracle.OracleConnector",
		Direction: kcSource,
		// RDS Oracle 19c SE2 is non-CDB, so database.dbname is the SID and there
		// is no database.pdb.name. snapshot.mode=no_data captures the schema (the
		// table is TRUNCATEd between sweep points, so there are no rows to copy)
		// then streams from the current SCN — matching the Connect side's
		// stream_snapshot:false. log.mining.strategy=online_catalog matches the
		// oracledb_cdc connector default and avoids writing the dictionary to
		// redo. Like the mysql connector, Debezium Oracle keeps a schema-history
		// topic. Both engines mine the same RDS redo via LogMiner — fair head-to-head.
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.oracle.OracleConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.dbname": "{{.Database}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "table.include.list": "{{.SchemaTables}}",
  "schema.history.internal.kafka.bootstrap.servers": "{{.BootstrapServers}}",
  "schema.history.internal.kafka.topic": "_kc_schema_history_{{.TopicPrefix}}",
  "snapshot.mode": "no_data",
  "log.mining.strategy": "online_catalog",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-oracle*"},
	},
	"microsoft_sql_server_cdc": {
		Class:     "io.debezium.connector.sqlserver.SqlServerConnector",
		Direction: kcSource,
		// The fairest pairing in the suite: Debezium SQL Server reads the same
		// cdc.<schema>_<table>_CT change tables that Connect's
		// microsoft_sql_server_cdc reads, populated by the same SQL Server
		// capture job. Neither engine touches the transaction log.
		//
		// database.names (plural, a list) is the 2.x property — NOT
		// database.dbname as in the postgres/oracle connectors. Topics land at
		// <prefix>.<database>.<schema>.<table>, so SQL Server topic names carry
		// one more segment than the other engines; combineReset enumerates
		// topics by prefix, so that needs no special handling.
		//
		// driver.encrypt/driver.trustServerCertificate are documented
		// pass-throughs to the mssql-jdbc driver. mssql-jdbc 10+ defaults to
		// encrypt=true and the RDS-internal CA isn't in the runner's trust
		// store, so trustServerCertificate=true is required or the handshake
		// fails. This matches the Connect side's DSN (encrypt=true +
		// TrustServerCertificate=true) so both engines pay the same TLS cost at
		// every pinned-vCPU point.
		//
		// snapshot.mode=no_data, as for mysql/oracle: the per-vCPU connector
		// names give each sweep point a connector with no prior offset, so
		// "never" would fail the task before warmup. no_data captures the schema
		// (the table is truncated between points, so there are no rows) then
		// streams from the current position.
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.names": "{{.Database}}",
  "database.encrypt": "true",
  "driver.encrypt": "true",
  "driver.trustServerCertificate": "true",
  "topic.prefix": "{{.TopicPrefix}}",
  "table.include.list": "{{.SchemaTables}}",
  "schema.history.internal.kafka.bootstrap.servers": "{{.BootstrapServers}}",
  "schema.history.internal.kafka.topic": "_kc_schema_history_{{.TopicPrefix}}",
  "snapshot.mode": "no_data",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-sqlserver*"},
	},
	"mongodb_cdc": {
		Class:     "io.debezium.connector.mongodb.MongoDbConnector",
		Direction: kcSource,
		// Debezium MongoDB consumes change streams — the same source the Connect
		// mongodb_cdc input reads, so the head-to-head is fair. mongodb.connection.string
		// points at the single-node replica set (no auth, private-subnet-only, so no
		// credentials in the URI). snapshot.mode=never streams from the current oplog
		// position without a backfill, matching the Connect side's stream_snapshot:false.
		// capture.mode=change_streams_update_full delivers the full post-image on updates
		// (parity with mongodb_cdc's default document output). collection.include.list is
		// <db>.<collection> ({{.SchemaTables}}, formatted in buildKCRenderInputs).
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "tasks.max": "1",
  "mongodb.connection.string": "mongodb://{{.Host}}:{{.Port}}/?replicaSet=rs0",
  "topic.prefix": "{{.TopicPrefix}}",
  "collection.include.list": "{{.SchemaTables}}",
  "capture.mode": "change_streams_update_full",
  "snapshot.mode": "never",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-mongodb*"},
	},
	"iceberg": {
		Class:     "io.tabular.iceberg.connect.IcebergSinkConnector",
		Direction: kcSink,
		PropsTemplate: `{
  "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "1",
  "topics": "{{.Topic}}",
  "iceberg.catalog.type": "rest",
  "iceberg.catalog.uri": "{{.GlueRESTURI}}",
  "iceberg.catalog.warehouse": "{{.Warehouse}}",
  "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "iceberg.catalog.rest.sigv4-enabled": "true",
  "iceberg.catalog.rest.signing-name": "glue",
  "iceberg.catalog.rest.signing-region": "{{.Region}}",
  "iceberg.catalog.client.region": "{{.Region}}",
  "iceberg.tables": "{{.Namespace}}.{{.Table}}",
  "iceberg.tables.auto-create-enabled": "false",
  "iceberg.control.commit.interval-ms": "10000",
  "iceberg.control.commit.timeout-ms": "30000",
  "iceberg.kafka.session.timeout.ms": "300000",
  "iceberg.kafka.max.poll.interval.ms": "300000",
  "consumer.override.auto.offset.reset": "earliest",
  "consumer.override.session.timeout.ms": "300000",
  "consumer.override.max.poll.interval.ms": "300000",
  "consumer.override.max.partition.fetch.bytes": "1048576",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"iceberg-kafka-connect*"},
	},
	"s3": {
		Class:     "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
		Direction: kcSink,
		// Aiven's open-source S3 Sink Connector for Apache Kafka. Leaving
		// aws.access.key.id/aws.secret.access.key unset falls back to the AWS
		// default credential provider chain, which on the runner EC2 instance
		// resolves to its IAM instance profile — do not set explicit
		// credentials. file.name.prefix is NOT a real, currently-valid
		// connector property (verified against the connector's own README:
		// it only appears inside the deprecation note for the old
		// aws.s3.prefix property, never in the "List of new configuration
		// parameters" section) — Kafka Connect silently ignores unknown
		// connector-specific properties, so setting it was a silent no-op
		// that let every object land at the bucket root under the
		// connector's default filename (observed live). file.name.template
		// is the only real way to add a path prefix. Its own
		// {{topic}}/{{partition}}/{{start_offset}} placeholders use the
		// exact same {{ }} delimiters this PropsTemplate is rendered
		// through (renderKCConfig via text/template), so they're escaped
		// below via Go template string-literal actions ({{"{{"}} / {{"}}"}})
		// that print the two characters verbatim instead of being
		// reinterpreted — the net rendered value after this template
		// executes once is still the literal string
		// "<prefix>{{topic}}-{{partition}}-{{start_offset}}" for the Aiven
		// connector's own template engine to interpret when it runs.
		// format.output.type=jsonl and format.output.fields=value are the
		// closest fair match to the Connect side's gzip'd NDJSON output
		// (payload-only, no key/offset/timestamp envelope).
		// consumer.override.session/poll timeouts and
		// max.partition.fetch.bytes mirror the iceberg sink's same settings
		// below: with a large pre-seeded backlog and
		// auto.offset.reset=earliest, an unbounded fetch plus Aiven's
		// record-grouping buffer (which by default only flushes every
		// offset.flush.interval.ms, 60s) let in-memory buffering grow
		// until the worker's heap OOMs (observed live: OutOfMemoryError in
		// NetworkReceive.readFrom). Bounding the fetch size and shortening
		// offset.flush.interval.ms — the generic Connect-framework property
		// controlling how often the framework triggers a flush/offset
		// commit — keeps that buffered window small.
		// consumer.override.max.poll.records bounds the other axis: a sink
		// task's poll() call synchronously hands every record it returns to
		// the connector's put() (Aiven's own buffering/compression/S3-upload
		// work) before the next poll() happens. Against this bench's
		// pre-seeded backlog (unlike a trickling CDC source, the topic is
		// already full), an unbounded record count per cycle let a single
		// poll's processing run past max.poll.interval.ms, so the consumer
		// self-evicted ("consumer poll timeout has expired"), triggering a
		// group rebalance and collapsing throughput (observed live in the
		// worker log). This is exactly the fix the Kafka Connect framework's
		// own warning names: reduce the max batch size returned by poll().
		// tasks.max is left as the __TASKS_MAX__ sentinel below: renderKCConfig
		// renders this template once per scenario, before the per-vCPU-point
		// sweep loop runs, so no vCPU value is known yet. matrix.go patches the
		// sentinel to the current sweep point vCPU count before use.
		// offset.flush.interval.ms is likewise left as the __FLUSH_INTERVAL_MS__
		// sentinel, patched by matrix.go alongside __TASKS_MAX__. Per the Aiven
		// S3 connector README's "Record grouping" section, this connector has no
		// record-count-based file rotation — it only flushes grouped, buffered
		// records per offset.flush.interval.ms. Buffered-but-unflushed volume
		// therefore scales with (aggregate throughput) x (flush interval), and
		// aggregate throughput now scales with tasks.max, so the interval must
		// scale inversely (10000/n ms) to keep buffered memory roughly bounded
		// across the vCPU sweep instead of growing with task count and OOMing
		// the JVM heap (observed live at vCPU=2 before this fix).
		PropsTemplate: `{
  "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
  "tasks.max": "__TASKS_MAX__",
  "topics": "{{.Topic}}",
  "aws.s3.bucket.name": "{{.Bucket}}",
  "aws.s3.region": "{{.Region}}",
  "file.name.template": "{{.Prefix}}{{"{{"}}topic{{"}}"}}-{{"{{"}}partition{{"}}"}}-{{"{{"}}start_offset{{"}}"}}",
  "file.compression.type": "gzip",
  "format.output.type": "jsonl",
  "format.output.fields": "value",
  "consumer.override.auto.offset.reset": "earliest",
  "consumer.override.session.timeout.ms": "300000",
  "consumer.override.max.poll.interval.ms": "300000",
  "consumer.override.max.partition.fetch.bytes": "1048576",
  "consumer.override.max.poll.records": "2000",
  "offset.flush.interval.ms": "__FLUSH_INTERVAL_MS__",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"s3-sink-connector-for-apache-kafka*"},
	},
}

func kcConnectorSpecFor(connector string) (kcConnectorSpec, bool) {
	es, ok := kcConnectorSpecs[connector]
	return es, ok
}

// kcRenderInputs carries the values interpolated into a kcConnectorSpec's
// PropsTemplate. Populated by the orchestrator from TF outputs + scenario.
type kcRenderInputs struct {
	// Database connection
	Host     string
	Port     string
	User     string
	Password string
	Database string

	// Tables to capture (formatted differently per engine in SchemaTables)
	Tables       []string
	SchemaTables string // engine-specific, e.g. "public.orders" for PG, "benchdb.orders" for MySQL

	// Output topic prefix for Debezium (Debezium prepends to each table topic)
	TopicPrefix string

	// Kafka bootstrap.servers for the internal schema-history topic (MySQL)
	BootstrapServers string

	// Sink (iceberg) render inputs. Empty for source connectors.
	GlueRESTURI   string
	Warehouse     string
	Region        string
	Namespace     string
	Table         string
	Topic         string
	ConsumerGroup string

	// Sink (s3) render inputs. Empty for source connectors and for the
	// iceberg sink (Region/Topic/ConsumerGroup above are shared).
	Bucket string
	Prefix string
}

// renderKCConfig produces the JSON config map ready to POST to the KC REST
// API. It looks up the connector's PropsTemplate, renders it with the given
// inputs, then merges any per-scenario `kafka_connect.config` overrides on
// top.
func renderKCConfig(s *Scenario, in kcRenderInputs) (map[string]any, error) {
	spec, ok := kcConnectorSpecFor(s.Connector)
	if !ok {
		return nil, fmt.Errorf("no kcConnectorSpec registered for connector %q", s.Connector)
	}

	tmpl, err := template.New("kc").Parse(spec.PropsTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template for %q: %w", s.Connector, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, in); err != nil {
		return nil, fmt.Errorf("render template for %q: %w", s.Connector, err)
	}

	var cfg map[string]any
	dec := json.NewDecoder(strings.NewReader(buf.String()))
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode rendered JSON for %q: %w; body:\n%s", s.Connector, err, buf.String())
	}

	// Shallow-merge scenario's `kafka_connect.config` over the base.
	if s.KafkaConnect != nil {
		if over, ok := s.KafkaConnect["config"].(map[string]any); ok {
			for k, v := range over {
				cfg[k] = v
			}
		}
	}

	return cfg, nil
}
