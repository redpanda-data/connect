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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	_ "github.com/sijms/go-ora/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch represents the expected test output.
type Batch struct {
	sync.Mutex
	Msgs []string
}

// Reset sets the messages in the batch to nil.
func (c *Batch) Reset() {
	c.Lock()
	defer c.Unlock()
	c.Msgs = nil
}

// Count returns the total number of messages in the batch.
func (c *Batch) Count() int {
	c.Lock()
	defer c.Unlock()
	return len(c.Msgs)
}

// Clone returns a clone of the underlying Msgs.
func (c *Batch) Clone() []string {
	c.Lock()
	defer c.Unlock()
	return slices.Clone(c.Msgs)
}

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

// SetupTestWithOracleDBVersion starts an Oracle Free Docker container, enables supplemental
// logging for CDC, and returns the connection string and TestDB wrapper.
// The container is automatically cleaned up when the test completes.
func SetupTestWithOracleDBVersion(t *testing.T) (string, *TestDB) {
	ctx := t.Context()
	cfg := startContainer(t, ctx)

	_, err := cfg.dbConn.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	assert.NoError(t, err)

	// Enable minimal supplemental logging for primary keys at CDB level
	_, err = cfg.dbConn.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS")
	assert.NoError(t, err)

	// Enable creation of local users in CDB root (required to avoid ORA-65096)
	_, err = cfg.dbConn.ExecContext(ctx, "ALTER SESSION SET \"_ORACLE_SCRIPT\"=TRUE")
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

	_, err = cfg.dbConn.ExecContext(t.Context(), sql)
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

	_, err = cfg.dbConn.ExecContext(t.Context(), sql)
	assert.NoError(t, err, "Creating 'testdb2' schema for testing across multiple schemas")

	return cfg.connStr, &TestDB{cfg.dbConn, t}
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

// SetupCDBTestWithPDB starts an Oracle Free container and configures it for CDB
// mode testing. It connects to CDB$ROOT (FREE service) to enable supplemental
// logging and create the rpcn checkpoint user, then connects to FREEPDB1 to
// create the testdb and testdb2 schema users.
//
// Returns:
//   - cdbConnStr: connection string targeting CDB$ROOT (use as connection_string in the connector config with pdb_name set)
//   - pdbDB: TestDB connected to FREEPDB1 for creating tables and inserting test data
//   - pdbName: "FREEPDB1"
func SetupCDBTestWithPDB(t *testing.T) (string, *TestDB, string) {
	ctx := t.Context()
	cfg := startContainer(t, ctx)

	// Enable CDB-level supplemental logging.
	_, err := cfg.dbConn.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	require.NoError(t, err)
	_, err = cfg.dbConn.ExecContext(ctx, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS")
	require.NoError(t, err)

	// In CDB mode the connector auto-derives the checkpoint cache table as
	// C##RPCN.CDC_CHECKPOINT_<PDBNAME>, so the common user must exist as C##RPCN.
	// Common users require the C## prefix but do not need _ORACLE_SCRIPT workaround.
	_, err = cfg.dbConn.ExecContext(ctx, `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'C##RPCN';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER "C##RPCN" IDENTIFIED BY rpcn123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO "C##RPCN"';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO "C##RPCN"';
		END IF;
	END;`)
	require.NoError(t, err)

	// FREEPDB1 connection for creating PDB-local schema users and test tables.
	// PDB-local users do not require the C## prefix or _ORACLE_SCRIPT workaround.
	pdbName := "FREEPDB1"
	pdbConnStr := fmt.Sprintf("oracle://system:YourPassword123@%s:%s/%s", cfg.host, cfg.port.Port(), pdbName)
	rawPDBDB, err := sql.Open("oracle", pdbConnStr)
	require.NoError(t, err)
	rawPDBDB.SetMaxOpenConns(10)
	rawPDBDB.SetMaxIdleConns(5)
	rawPDBDB.SetConnMaxLifetime(time.Minute * 5)
	require.NoError(t, rawPDBDB.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, rawPDBDB.Close()) })

	for _, username := range []string{"TESTDB", "TESTDB2"} {
		_, err = rawPDBDB.ExecContext(ctx, fmt.Sprintf(`
		DECLARE
			user_exists NUMBER;
		BEGIN
			SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = '%s';
			IF user_exists = 0 THEN
				EXECUTE IMMEDIATE 'CREATE USER %s IDENTIFIED BY %s123';
				EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO %s';
				EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO %s';
			END IF;
		END;`, username, strings.ToLower(username), strings.ToLower(username), username, username))
		require.NoError(t, err, "creating %s in FREEPDB1", username)
	}

	return cfg.connStr, &TestDB{rawPDBDB, t}, pdbName
}

// CreatePDBTableWithSupplementalLoggingIfNotExists creates a table in a PDB and
// enables supplemental logging on it. Unlike CreateTableWithSupplementalLoggingIfNotExists,
// it skips the _ORACLE_SCRIPT workaround and rpcn user creation — those are only
// needed in CDB$ROOT context and are handled by SetupCDBTestWithPDB.
func (db *TestDB) CreatePDBTableWithSupplementalLoggingIfNotExists(ctx context.Context, fullTableName, createTableQuery string) error {
	parts := strings.SplitN(fullTableName, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("fullTableName must be schema.table, got %q", fullTableName)
	}
	schemaName := strings.ToUpper(parts[0])
	tableName := strings.ToUpper(parts[1])

	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
		schemaName, tableName).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return err
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
		schemaName, tableName))
	return err
}

type containerCfg struct {
	dbConn  *sql.DB
	host    string
	connStr string
	port    network.Port
}

func startContainer(t *testing.T, ctx context.Context) containerCfg {
	t.Helper()

	container, err := testcontainers.Run(ctx, "container-registry.oracle.com/database/free:latest-lite",
		testcontainers.WithExposedPorts("1521/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ORACLE_PWD": "YourPassword123",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("DATABASE IS READY TO USE!").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "1521/tcp")
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)

	// CDB$ROOT connection string — the connector uses this with pdb_name set.
	connStr := fmt.Sprintf("oracle://system:YourPassword123@%s:%s/FREE", host, port.Port())
	dbConn, err := sql.Open("oracle", connStr)
	require.NoError(t, err)

	dbConn.SetMaxOpenConns(10)
	dbConn.SetMaxIdleConns(5)
	dbConn.SetConnMaxLifetime(time.Minute * 5)
	require.NoError(t, dbConn.PingContext(ctx))
	t.Cleanup(func() {
		assert.NoError(t, dbConn.Close())
	})

	return containerCfg{
		dbConn:  dbConn,
		host:    host,
		connStr: connStr,
		port:    port,
	}
}
