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
