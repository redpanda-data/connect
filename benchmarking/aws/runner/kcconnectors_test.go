// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func TestKCConnectorSpecFor_Unknown(t *testing.T) {
	_, ok := kcConnectorSpecFor("does_not_exist")
	if ok {
		t.Error("expected lookup of unknown connector to return ok=false")
	}
}

func TestKCConnectorSpecFor_PostgresPlaceholder(t *testing.T) {
	// Registry will be filled in later tasks. This test just confirms
	// the lookup function compiles and returns ok=false on an empty registry.
	_, ok := kcConnectorSpecFor("postgres_cdc")
	_ = ok // not asserting yet; future tasks add entries.
}

func TestKCConnectorSpecFor_PostgresCDC(t *testing.T) {
	es, ok := kcConnectorSpecFor("postgres_cdc")
	if !ok {
		t.Fatal("postgres_cdc should be registered")
	}
	if es.Class != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Errorf("Class = %q, want Debezium Postgres class", es.Class)
	}
	if es.Direction != kcSource {
		t.Errorf("Direction should be kcSource")
	}
	if es.PropsTemplate == "" {
		t.Error("PropsTemplate should not be empty")
	}
	// The template should reference at least the DSN substitution and the
	// table list. Accept either {{.Host}} or {{.HostPort}} for the host
	// field, and either {{.SchemaTables}} or {{.Tables}} for the table list.
	if !strings.Contains(es.PropsTemplate, "{{.HostPort}}") &&
		!strings.Contains(es.PropsTemplate, "{{.Host}}") {
		t.Errorf("template should interpolate host info; got:\n%s", es.PropsTemplate)
	}
	if !strings.Contains(es.PropsTemplate, "{{.SchemaTables}}") &&
		!strings.Contains(es.PropsTemplate, "{{.Tables}}") {
		t.Errorf("template should interpolate table list; got:\n%s", es.PropsTemplate)
	}
	if len(es.RequiredPlugins) == 0 {
		t.Error("RequiredPlugins should list at least debezium-connector-postgres glob")
	}
}

func TestKCConnectorSpecFor_MySQLCDC(t *testing.T) {
	es, ok := kcConnectorSpecFor("mysql_cdc")
	if !ok {
		t.Fatal("mysql_cdc should be registered")
	}
	if es.Class != "io.debezium.connector.mysql.MySqlConnector" {
		t.Errorf("Class = %q, want Debezium MySQL class", es.Class)
	}
	if es.Direction != kcSource {
		t.Errorf("Direction should be kcSource")
	}
	if !strings.Contains(es.PropsTemplate, "database.server.id") {
		t.Errorf("MySQL Debezium config must include server.id; got:\n%s", es.PropsTemplate)
	}
	if !strings.Contains(es.PropsTemplate, "{{.Host}}") {
		t.Errorf("template should interpolate host; got:\n%s", es.PropsTemplate)
	}
}

func TestRenderKCConfig_PostgresBasic(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"tables": []any{"orders"},
					"schema": "public",
				},
			},
		},
	}
	inputs := kcRenderInputs{
		Host:             "rds.example.com",
		Port:             "5432",
		User:             "bench",
		Password:         "s3cret",
		Database:         "benchdb",
		Tables:           []string{"orders"},
		SchemaTables:     "public.orders",
		TopicPrefix:      "bench_sess123_postgres_cdc_kc",
		BootstrapServers: "10.42.10.10:9092",
	}
	cfg, err := renderKCConfig(s, inputs)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["database.hostname"] != "rds.example.com" {
		t.Errorf("hostname = %v, want rds.example.com", cfg["database.hostname"])
	}
	if cfg["database.port"] != "5432" {
		t.Errorf("port = %v, want 5432", cfg["database.port"])
	}
	if cfg["table.include.list"] != "public.orders" {
		t.Errorf("table.include.list = %v, want public.orders", cfg["table.include.list"])
	}
	if cfg["topic.prefix"] != "bench_sess123_postgres_cdc_kc" {
		t.Errorf("topic.prefix = %v", cfg["topic.prefix"])
	}
	if cfg["snapshot.mode"] != "never" {
		t.Errorf("snapshot.mode should default to never; got %v", cfg["snapshot.mode"])
	}
}

func TestRenderKCConfig_ScenarioOverride(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"tables": []any{"orders"},
					"schema": "public",
				},
			},
		},
		KafkaConnect: map[string]any{
			"config": map[string]any{
				"snapshot.mode":         "initial",
				"decimal.handling.mode": "string",
			},
		},
	}
	inputs := kcRenderInputs{
		Host: "rds.example.com", Port: "5432",
		User: "bench", Password: "s3cret", Database: "benchdb",
		Tables: []string{"orders"}, SchemaTables: "public.orders",
		TopicPrefix:      "bench_sess123_postgres_cdc_kc",
		BootstrapServers: "10.42.10.10:9092",
	}
	cfg, err := renderKCConfig(s, inputs)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["snapshot.mode"] != "initial" {
		t.Errorf("override should win; got snapshot.mode = %v", cfg["snapshot.mode"])
	}
	if cfg["decimal.handling.mode"] != "string" {
		t.Errorf("override should win; got decimal.handling.mode = %v", cfg["decimal.handling.mode"])
	}
	// Non-overridden fields preserved from the registry template:
	if cfg["database.hostname"] != "rds.example.com" {
		t.Errorf("base template field should survive override; got %v", cfg["database.hostname"])
	}
}

func TestRenderKCConfig_Iceberg(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	in := kcRenderInputs{
		GlueRESTURI:   "https://glue.us-east-2.amazonaws.com/iceberg",
		Warehouse:     "123456789012",
		Region:        "us-east-2",
		Namespace:     "bench",
		Table:         "bench_sess_iceberg_kafka_connect",
		Topic:         "bench_sess_iceberg_src",
		ConsumerGroup: "bench_sess_iceberg_kafka_connect",
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["connector.class"] != "io.tabular.iceberg.connect.IcebergSinkConnector" {
		t.Errorf("class = %v", cfg["connector.class"])
	}
	if cfg["iceberg.catalog.rest.sigv4-enabled"] != "true" {
		t.Errorf("sigv4 must be enabled for Glue REST")
	}
	if cfg["iceberg.tables"] != "bench.bench_sess_iceberg_kafka_connect" {
		t.Errorf("iceberg.tables = %v", cfg["iceberg.tables"])
	}
	if cfg["iceberg.tables.auto-create-enabled"] != "false" {
		t.Errorf("auto-create must be false (tables are pre-created); got %v", cfg["iceberg.tables.auto-create-enabled"])
	}
	// Aligned to the repo's working reference connector.json: the sink manages
	// its own consumer group for transactional offset commits, so we must NOT
	// override it; and the control consumer/producer needs the longer timeouts.
	if _, ok := cfg["consumer.override.group.id"]; ok {
		t.Errorf("must NOT set consumer.override.group.id (interferes with the sink's own transactional group); got %v", cfg["consumer.override.group.id"])
	}
	if cfg["iceberg.control.commit.timeout-ms"] != "30000" {
		t.Errorf("iceberg.control.commit.timeout-ms = %v, want 30000", cfg["iceberg.control.commit.timeout-ms"])
	}
	if cfg["iceberg.kafka.max.poll.interval.ms"] != "300000" {
		t.Errorf("iceberg.kafka.max.poll.interval.ms = %v, want 300000", cfg["iceberg.kafka.max.poll.interval.ms"])
	}
}

func TestRenderKCConfig_UnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "does_not_exist"}
	_, err := renderKCConfig(s, kcRenderInputs{})
	if err == nil {
		t.Error("expected error for unknown connector")
	}
}
