// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package saphanatest provides helpers for SAP HANA CDC integration tests.
// Tests using this package require a running HANA Express (HXE) instance and
// must be gated with the HANA_INTEGRATION_TESTS environment variable.
package saphanatest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver"
	"github.com/stretchr/testify/require"
)

const (
	defaultHost     = "localhost"
	defaultPort     = "39015"
	defaultUser     = "SYSTEM"
	defaultPassword = "HXEHana1"
	defaultDatabase = "HXE"
)

// VerifyChange is one row from _CDC_VERIFY.CHANGES.
type VerifyChange struct {
	ID         int64
	Op         string // 'I', 'U', 'D'
	SchemaName string
	TableName  string
	OpTime     time.Time
	PKJson     string // empty if NULL
	OldValues  string // empty if NULL
	NewValues  string // empty if NULL
}

// TestDB wraps sql.DB with helpers for CDC test assertions.
type TestDB struct {
	*sql.DB
}

// Connect opens a connection to HXE using environment variables for overrides.
// It skips the test unless HANA_INTEGRATION_TESTS=1.
//
// Environment variables (all optional — defaults match HXE defaults):
//
//	HANA_HOST        — hostname (default: localhost)
//	HANA_PORT        — port    (default: 39015)
//	HANA_USER        — user    (default: SYSTEM)
//	HANA_PASSWORD    — password (default: HXEHana1)
//	HANA_DATABASE    — tenant database name (default: HXE)
func Connect(t *testing.T) *TestDB {
	t.Helper()

	if os.Getenv("HANA_INTEGRATION_TESTS") != "1" {
		t.Skip("skipping SAP HANA integration test: set HANA_INTEGRATION_TESTS=1 to run")
	}

	host := envOrDefault("HANA_HOST", defaultHost)
	port := envOrDefault("HANA_PORT", defaultPort)
	user := envOrDefault("HANA_USER", defaultUser)
	password := envOrDefault("HANA_PASSWORD", defaultPassword)
	database := envOrDefault("HANA_DATABASE", defaultDatabase)

	dsn := fmt.Sprintf("hdb://%s:%s@%s:%s?databaseName=%s", user, password, host, port, database)

	connector, err := driver.NewDSNConnector(dsn)
	require.NoError(t, err, "building HANA DSN connector")

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// HANA takes time to become ready; retry ping until it responds.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil
	}, 2*time.Minute, 2*time.Second, "timed out waiting for HANA to become ready")

	t.Cleanup(func() { _ = db.Close() })

	return &TestDB{db}
}

// MustExec executes SQL, failing the test on error.
func (db *TestDB) MustExec(t *testing.T, query string, args ...any) sql.Result {
	t.Helper()
	result, err := db.Exec(query, args...)
	require.NoError(t, err, "executing SQL: %s", query)
	return result
}

// ExecIgnoreError executes SQL and discards any error.
// Useful for cleanup statements and "IF NOT EXISTS" workarounds on databases
// that do not support that syntax (e.g. HANA's DROP TABLE does not accept IF NOT EXISTS).
func (db *TestDB) ExecIgnoreError(query string) {
	_, _ = db.Exec(query)
}

// ClearVerifyChanges removes all rows from _CDC_VERIFY.CHANGES.
// Uses DELETE (not TRUNCATE) so the operation participates in HANA's
// transactional semantics — ensuring each test starts with a clean slate.
func (db *TestDB) ClearVerifyChanges(t *testing.T) {
	t.Helper()
	_, err := db.Exec("DELETE FROM _CDC_VERIFY.CHANGES")
	require.NoError(t, err, "clearing _CDC_VERIFY.CHANGES")
}

// quoteIdent wraps a HANA SQL identifier in ANSI double-quotes, escaping
// embedded double-quotes by doubling them (SQL standard).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// ClearTable deletes all rows from the given fully-qualified table and then
// clears the verify-changes table so each test starts with zero baseline events.
func (db *TestDB) ClearTable(t *testing.T, schema, table string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("DELETE FROM %s.%s", quoteIdent(schema), quoteIdent(table)))
	require.NoError(t, err, "clearing table %s.%s", schema, table)
	db.ClearVerifyChanges(t)
}

// QueryVerifyChanges returns all verify-change rows for the given schema+table,
// ordered by ID (insertion order).  Pass empty strings to return all rows.
func (db *TestDB) QueryVerifyChanges(t *testing.T, schemaName, tableName string) []VerifyChange {
	t.Helper()

	query := `
SELECT
    ID,
    OP,
    SCHEMA_NAME,
    TABLE_NAME,
    OP_TIME,
    COALESCE(PK_JSON,   '') AS PK_JSON,
    COALESCE(OLD_VALUES,'') AS OLD_VALUES,
    COALESCE(NEW_VALUES,'') AS NEW_VALUES
FROM _CDC_VERIFY.CHANGES`

	var args []any
	if schemaName != "" && tableName != "" {
		query += " WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?"
		args = append(args, schemaName, tableName)
	} else if schemaName != "" {
		query += " WHERE SCHEMA_NAME = ?"
		args = append(args, schemaName)
	} else if tableName != "" {
		query += " WHERE TABLE_NAME = ?"
		args = append(args, tableName)
	}
	query += " ORDER BY ID"

	rows, err := db.Query(query, args...)
	require.NoError(t, err, "querying _CDC_VERIFY.CHANGES")
	defer rows.Close()

	var changes []VerifyChange
	for rows.Next() {
		var c VerifyChange
		err := rows.Scan(&c.ID, &c.Op, &c.SchemaName, &c.TableName, &c.OpTime, &c.PKJson, &c.OldValues, &c.NewValues)
		require.NoError(t, err, "scanning verify change row")
		changes = append(changes, c)
	}
	require.NoError(t, rows.Err(), "iterating verify change rows")
	return changes
}

// RequireChangeCount asserts that exactly n changes were recorded for the
// given schema.table and returns them for further assertion.
func (db *TestDB) RequireChangeCount(t *testing.T, schema, table string, n int) []VerifyChange {
	t.Helper()
	changes := db.QueryVerifyChanges(t, schema, table)
	require.Len(t, changes, n,
		"expected %d change(s) in _CDC_VERIFY.CHANGES for %s.%s, got %d",
		n, schema, table, len(changes))
	return changes
}

// envOrDefault returns the value of the named environment variable, or def if unset.
func envOrDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
