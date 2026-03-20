// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledbtest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDB wraps sql.DB with testing utilities for Oracle database integration tests.
// It provides helper methods for table creation, supplemental logging enablement, and assertions.
type TestDB struct {
	*sql.DB

	T *testing.T
}

// MustExec executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.T, err)
}

// MustExecContext takes a context and executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExecContext(ctx context.Context, query string, args ...any) {
	_, err := db.ExecContext(ctx, query, args...)
	require.NoError(db.T, err)
}

// MustEnableSupplementalLogging enables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
// This enables supplemental logging for all columns, which is required for CDC.
func (db *TestDB) MustEnableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Enabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Enable supplemental logging for all columns on the table
	// This ensures all column values (before and after) are captured in redo logs
	query := fmt.Sprintf(`ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging enabled for table %q", fullTableName)
}

// MustDisableSupplementalLogging disables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
func (db *TestDB) MustDisableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Disabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Drop supplemental logging for all columns on the table
	query := fmt.Sprintf(`ALTER TABLE %s.%s DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging disabled for table %q", fullTableName)
}

// CreateTableWithSupplementalLoggingIfNotExists creates the given test tables ensuring supplemental logging is enabled.
func (db *TestDB) CreateTableWithSupplementalLoggingIfNotExists(ctx context.Context, fullTableName, createTableQuery string, _ ...any) error {
	// default to SYSTEM if not found
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Enable creation of local users in CDB root (required to avoid ORA-65096)
	if _, err := db.Exec("ALTER SESSION SET \"_ORACLE_SCRIPT\"=TRUE"); err != nil {
		return err
	}

	q := `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'RPCN';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER rpcn IDENTIFIED BY rpcn123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO rpcn';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO rpcn';
		END IF;
	END;`
	if _, err := db.Exec(q); err != nil {
		return err
	}

	// Check if table exists using Oracle's user_tables view
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
		schema, tableName).Scan(&count)
	if err != nil {
		return err
	}

	// Only create table if it doesn't exist
	if count == 0 {
		// Create the table
		if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
			return err
		}

		// Enable supplemental logging for all columns on the table
		enableSupplementalLogging := fmt.Sprintf(
			"ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
			schema, tableName)
		if _, err := db.ExecContext(ctx, enableSupplementalLogging); err != nil {
			return err
		}
	}

	return nil
}

// SetupTestWithOracleDBVersion starts an Oracle XE Docker container with the specified version,
// enables supplemental logging for CDC, and returns the connection string and TestDB wrapper.
// The container is automatically cleaned up when the test completes.
func SetupTestWithOracleDBVersion(t *testing.T, version string) (string, *TestDB) {
	ctx := t.Context()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "container-registry.oracle.com/database/express:" + version,
			ExposedPorts: []string{"1521/tcp"},
			Env: map[string]string{
				"ORACLE_PWD": "YourPassword123",
			},
			WaitingFor: wait.ForLog("DATABASE IS READY TO USE!").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()))
	})

	port, err := container.MappedPort(ctx, "1521/tcp")
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)

	pdbConnectionString := fmt.Sprintf("oracle://system:YourPassword123@%s:%s/XE", host, port.Port())

	db, err := sql.Open("oracle", pdbConnectionString)
	require.NoError(t, err)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)
	require.NoError(t, db.PingContext(ctx))

	_, err = db.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	assert.NoError(t, err)

	// Enable minimal supplemental logging for primary keys at CDB level
	_, err = db.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS")
	assert.NoError(t, err)

	// Enable creation of local users in CDB root (required to avoid ORA-65096)
	_, err = db.ExecContext(ctx, "ALTER SESSION SET \"_ORACLE_SCRIPT\"=TRUE")
	require.NoError(t, err, "Failed to enable _ORACLE_SCRIPT session parameter")

	sql := `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER testdb IDENTIFIED BY testdb123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb';
		END IF;
	END;`

	_, err = db.ExecContext(t.Context(), sql)
	assert.NoError(t, err, "Creating 'testdb' schema for testing across multiple schemas")

	sql = `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB2';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER testdb2 IDENTIFIED BY testdb2123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb2';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb2';
		END IF;
	END;`

	_, err = db.ExecContext(t.Context(), sql)
	assert.NoError(t, err, "Creating 'testdb2' schema for testing across multiple schemas")

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return pdbConnectionString, &TestDB{db, t}
}

// ---------------------------------------------------------------------------
// Schema metadata integration tests
// ---------------------------------------------------------------------------

// ExtractSchema extracts and parses the schema metadata from a service.Message.
// Returns a zero-value schema.Common if the metadata is absent.
func ExtractSchema(t *testing.T, msg *service.Message) schema.Common {
	t.Helper()
	var raw any
	_ = msg.MetaWalkMut(func(k string, v any) error {
		if k == "schema" {
			raw = v
		}
		return nil
	})
	if raw == nil {
		return schema.Common{}
	}
	c, err := schema.ParseFromAny(raw)
	require.NoError(t, err)
	return c
}

// ExtractFingerprint extracts the fingerprint string from schema metadata.
func ExtractFingerprint(t *testing.T, msg *service.Message) string {
	t.Helper()
	var raw any
	_ = msg.MetaWalkMut(func(k string, v any) error {
		if k == "schema" {
			raw = v
		}
		return nil
	})
	if raw == nil {
		return ""
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	fp, _ := m["fingerprint"].(string)
	return fp
}

// ChildByName finds a child by name in a Common schema for test assertions.
func ChildByName(t *testing.T, c schema.Common, name string) schema.Common {
	t.Helper()
	for i := range c.Children {
		if c.Children[i].Name == name {
			return c.Children[i]
		}
	}
	t.Fatalf("child %q not found in schema %q", name, c.Name)
	return schema.Common{}
}
