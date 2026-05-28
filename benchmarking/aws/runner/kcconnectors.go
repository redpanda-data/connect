// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

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
  "snapshot.mode": "never",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-mysql*"},
	},
}

func kcConnectorSpecFor(connector string) (kcConnectorSpec, bool) {
	es, ok := kcConnectorSpecs[connector]
	return es, ok
}
