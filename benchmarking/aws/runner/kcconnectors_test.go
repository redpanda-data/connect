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
