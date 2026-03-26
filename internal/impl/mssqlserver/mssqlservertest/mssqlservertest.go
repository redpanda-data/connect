// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlservertest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
)

// TestDB wraps sql.DB with testing utilities for Microsoft SQL Server integration tests.
// It provides helper methods for table creation, CDC enablement, and assertions.
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

// MustEnableCDC enables Change Data Capture on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "dbo.all_data_types").
// If only a table name is provided, defaults to "dbo" schema.
func (db *TestDB) MustEnableCDC(ctx context.Context, fullTableName string) {
	db.T.Logf("Enabling Change Data Capture for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"dbo", table[0]}
	}
	schema := table[0]
	tableName := table[1]

	query := fmt.Sprintf(`
		EXEC sys.sp_cdc_enable_table
		@source_schema = '%s',
		@source_name   = '%s',
		@role_name     = NULL;`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	// Wait for CDC table to be ready
	for {
		var minLSN, maxLSN []byte
		if err = db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(?)", fullTableName).Scan(&minLSN); err != nil {
			break
		}
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err != nil {
			break
		}
		if minLSN != nil && maxLSN != nil {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			goto end
		case <-time.After(time.Second):
		}
	}

end:
	require.NoError(db.T, err)
	db.T.Logf("Change Data Capture enabled for table %q", fullTableName)
}

// MustDisableCDC disables Change Data Capture on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "dbo.all_data_types").
// If only a table name is provided, defaults to "dbo" schema.
func (db *TestDB) MustDisableCDC(ctx context.Context, fullTableName string) {
	db.T.Logf("Disabling Change Data Capture for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"dbo", table[0]}
	}
	schema := table[0]
	tableName := table[1]

	query := fmt.Sprintf(`
		EXEC sys.sp_cdc_disable_table
		@source_schema = '%s',
		@source_name   = '%s',
		@capture_instance = 'all';`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Change Data Capture enabled for table %q", fullTableName)
}

// CreateTableWithCDCEnabledIfNotExists creates the given test tables ensuring CDC is enabled.
func (db *TestDB) CreateTableWithCDCEnabledIfNotExists(ctx context.Context, fullTableName, createTableQuery string, _ ...any) error {
	// default to dbo if not found
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"dbo", table[0]}
	}
	schema := table[0]
	tableName := table[1]

	q := `
	IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')
	BEGIN
		EXEC('CREATE SCHEMA %s');
	END
	IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpcn')
	BEGIN
		EXEC('CREATE SCHEMA rpcn');
	END`
	if _, err := db.Exec(fmt.Sprintf(q, schema, schema)); err != nil {
		return err
	}

	enableSnapshot := `ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;`
	enableCDC := fmt.Sprintf(`
		EXEC sys.sp_cdc_enable_table
		@source_schema = '%s',
		@source_name   = '%s',
		@role_name     = NULL;`, schema, tableName)
	q = fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('%s'))
		BEGIN
			%s
			%s
			%s
		END;`, tableName, schema, createTableQuery, enableCDC, enableSnapshot)
	if _, err := db.Exec(q); err != nil {
		return err
	}

	// wait for CDC table to be ready, this avoids time.sleeps
	for {
		var minLSN, maxLSN []byte
		// table isn't ready yet
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(?)", fullTableName).Scan(&minLSN); err != nil {
			return err
		}
		// cdc agent still preparing
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err != nil {
			return err
		}
		if minLSN != nil && maxLSN != nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}

// SetupTestWithMicrosoftSQLServerVersion starts a Microsoft SQL Server Docker container with the specified version,
// creates a testdb database, enables CDC, and returns the connection string and TestDB wrapper.
// The container is automatically cleaned up when the test completes.
func SetupTestWithMicrosoftSQLServerVersion(t *testing.T) (string, *TestDB) {
	ctr, err := tcmssql.Run(t.Context(),
		"mcr.microsoft.com/mssql/server:2025-latest",
		testcontainers.WithImagePlatform("linux/amd64"),
		tcmssql.WithAcceptEULA(),
		tcmssql.WithPassword("YourStrong!Passw0rd"),
		testcontainers.WithEnv(map[string]string{
			"MSSQL_AGENT_ENABLED": "true",
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	connectionString, err := ctr.ConnectionString(t.Context(), "database=master", "encrypt=disable")
	require.NoError(t, err)

	// Retry creating testdb and enabling CDC — the SQL Agent may not be ready immediately.
	require.Eventually(t, func() bool {
		db, err := sql.Open("mssql", connectionString)
		if err != nil {
			return false
		}
		defer db.Close()

		if err = db.Ping(); err != nil {
			return false
		}

		if _, err = db.Exec(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
			BEGIN
				CREATE DATABASE testdb;
			END;`); err != nil {
			return false
		}

		return true
	}, 2*time.Minute, 2*time.Second)

	connectionString, err = ctr.ConnectionString(t.Context(), "database=testdb", "encrypt=disable")
	require.NoError(t, err)

	var db *sql.DB
	require.Eventually(t, func() bool {
		var openErr error
		db, openErr = sql.Open("mssql", connectionString)
		if openErr != nil {
			return false
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if openErr = db.Ping(); openErr != nil {
			return false
		}

		// enable CDC on database
		if _, openErr = db.Exec("EXEC sys.sp_cdc_enable_db;"); openErr != nil {
			return false
		}

		return true
	}, 2*time.Minute, 2*time.Second)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, &TestDB{db, t}
}

// MustSetupTestWithMicrosoftSQLServerVersion starts a Microsoft SQL Server Docker container with the specified version
// and returns the connection string and raw sql.DB connected to the master database.
// Unlike SetupTestWithMicrosoftSQLServerVersion, this does not create testdb or enable CDC.
// The container is automatically cleaned up when the test completes.
func MustSetupTestWithMicrosoftSQLServerVersion(t *testing.T) (string, *sql.DB) {
	ctr, err := tcmssql.Run(t.Context(),
		"mcr.microsoft.com/mssql/server:2025-latest",
		testcontainers.WithImagePlatform("linux/amd64"),
		tcmssql.WithAcceptEULA(),
		tcmssql.WithPassword("YourStrong!Passw0rd"),
		testcontainers.WithEnv(map[string]string{
			"MSSQL_AGENT_ENABLED": "true",
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	connectionString, err := ctr.ConnectionString(t.Context(), "database=master", "encrypt=disable")
	require.NoError(t, err)

	var db *sql.DB
	require.Eventually(t, func() bool {
		var openErr error
		if db, openErr = sql.Open("mssql", connectionString); openErr != nil {
			return false
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if openErr = db.Ping(); openErr != nil {
			return false
		}

		return true
	}, 2*time.Minute, 2*time.Second)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, db
}
