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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
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
	captureInstance := schema + "_" + tableName
	for {
		var minLSN, maxLSN []byte
		if err = db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(?)", captureInstance).Scan(&minLSN); err != nil {
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

// WaitForCDCChanges waits until the CDC change table for each given source table
// has at least minRows entries. Under x86 emulation on Apple Silicon the CDC
// capture agent can be very slow, so tests must poll rather than sleep.
func (db *TestDB) WaitForCDCChanges(ctx context.Context, minRows int, tables ...string) {
	db.T.Helper()
	for _, fullTableName := range tables {
		table := strings.Split(fullTableName, ".")
		if len(table) != 2 {
			table = []string{"dbo", table[0]}
		}
		query := "SELECT COUNT(*) FROM [cdc].[" + table[0] + "_" + table[1] + "_CT]"
		var lastCount int
		if !assert.Eventually(db.T, func() bool {
			if ctx.Err() != nil {
				return false
			}
			if err := db.QueryRowContext(ctx, query).Scan(&lastCount); err != nil {
				return false
			}
			return lastCount >= minRows
		}, 5*time.Minute, time.Second) {
			db.T.Fatalf("WaitForCDCChanges(%q): expected >= %d rows, got %d", fullTableName, minRows, lastCount)
		}
	}
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

	q = fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('%s'))
		BEGIN
			%s
		END;`, tableName, schema, createTableQuery)
	if _, err := db.Exec(q); err != nil {
		return err
	}

	if _, err := db.Exec(`ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;`); err != nil {
		return err
	}

	enableCDC := fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('%s.%s'))
		BEGIN
			EXEC sys.sp_cdc_enable_table
			@source_schema = '%s',
			@source_name   = '%s',
			@role_name     = NULL;
		END`, schema, tableName, schema, tableName)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err := db.Exec(enableCDC)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "SQL Server Agent is starting") || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	// wait for CDC table to be ready, this avoids time.sleeps
	captureInstance := schema + "_" + tableName
	for {
		var minLSN, maxLSN []byte
		// table isn't ready yet
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(?)", captureInstance).Scan(&minLSN); err != nil {
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

// cdcJobsMu makes the calls that change the CDC jobs in msdb run one at a time.
//
// The CDC jobs are the capture and cleanup SQL Server Agent jobs of each CDC database. openAndEnableCDC adds them, and
// DROP DATABASE in dropDatabase removes them. When parallel tests on the shared container add jobs at the same time
// for different databases, msdb.dbo.sp_add_job deadlocks (error 1205) and the call fails. SQL Server also runs these
// calls one at a time internally, so parallel calls are not faster.
var cdcJobsMu sync.Mutex

// State of the container that all tests in one test package share. Only sharedContainer writes it.
var (
	// sharedOnce makes sure that only the first sharedContainer call starts the container.
	sharedOnce sync.Once
	// sharedCtr is the running container, or nil if it did not start.
	// TerminateSharedContainer stops it at the end of TestMain.
	sharedCtr *tcmssql.MSSQLServerContainer
	// sharedErr is the error of the last start attempt. sharedContainer returns it to every test,
	// so that all tests fail with the same cause and no test tries to start the container again.
	sharedErr error
)

// sharedContainer returns the Microsoft SQL Server container that all tests in the test package share.
// The first call starts it. Tests isolate their state in their own database (see testDatabaseName).
// Call TerminateSharedContainer from TestMain to stop it.
//
// Go runs each test package as its own process, so mssqlserver and mssqlserver/replication each start one container.
// We do not share it across packages: no package could own its lifetime, and replication starts it for a single test.
func sharedContainer(t *testing.T) *tcmssql.MSSQLServerContainer {
	t.Helper()
	sharedOnce.Do(func() {
		// context.Background(), not t.Context(): the container outlives the test that starts it.
		ctx := context.Background()
		const maxAttempts = 3
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			sharedCtr, sharedErr = startMSSQLServerContainer(ctx)
			if sharedErr == nil {
				// The master database accepts connections only some time after the container is up.
				if sharedErr = createDatabase(ctx, sharedCtr, ""); sharedErr == nil {
					return
				}
			}
			t.Logf("mssqlserver container start attempt %d/%d failed: %v", attempt, maxAttempts, sharedErr)
			if sharedCtr != nil { // ensure we don't leak a running container
				_ = sharedCtr.Terminate(context.Background())
				sharedCtr = nil
			}
		}
	})
	require.NoError(t, sharedErr)
	return sharedCtr
}

// TerminateSharedContainer stops the container that sharedContainer started, if there is one.
// Call it from TestMain after m.Run in each package that uses this package.
func TerminateSharedContainer() {
	if sharedCtr != nil {
		_ = sharedCtr.Terminate(context.Background())
	}
}

// testDatabaseName returns a database name that is unique to the running test.
//
// Each test on the shared container gets its own database. The rules are the same as databaseNameForTest in the
// mongodb/cdc tests: every character outside [A-Za-z0-9_] becomes an underscore, and a name longer than 63 bytes is
// cut and gets a short hash of the full name, so two long names with the same prefix do not collide.
// SQL Server permits 128 characters, but the CDC job names put the database name between a prefix and a suffix,
// so a short name keeps them short too.
func testDatabaseName(t *testing.T) string {
	name := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, t.Name())
	const maxLen = 63
	if len(name) <= maxLen {
		return name
	}
	sum := sha256.Sum256([]byte(t.Name()))
	suffix := "_" + hex.EncodeToString(sum[:])[:8]
	return name[:maxLen-len(suffix)] + suffix
}

// dropDatabase drops the test database, so that its CDC capture job stops and does not load the shared container.
// It disconnects all open sessions first. Errors are only logged, because the container is removed at the end anyway.
func dropDatabase(t *testing.T, ctr *tcmssql.MSSQLServerContainer, dbName string) {
	ctx := context.Background()
	connStr, err := ctr.ConnectionString(ctx, "database=master", "encrypt=disable")
	if err != nil {
		t.Logf("drop database %q: %v", dbName, err)
		return
	}
	db, err := sql.Open("mssql", connStr)
	if err != nil {
		t.Logf("drop database %q: %v", dbName, err)
		return
	}
	defer db.Close()
	q := fmt.Sprintf("ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s];", dbName, dbName)
	cdcJobsMu.Lock()
	defer cdcJobsMu.Unlock()
	if _, err := db.ExecContext(ctx, q); err != nil {
		t.Logf("drop database %q: %v", dbName, err)
	}
}

// SetupTestWithMicrosoftSQLServerVersion creates a database for the test on the shared Microsoft SQL Server container,
// enables CDC on it, and returns its connection string and a TestDB wrapper.
// The database is dropped when the test completes.
func SetupTestWithMicrosoftSQLServerVersion(t *testing.T) (string, *TestDB) {
	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)

	require.NoError(t, createDatabase(t.Context(), ctr, dbName))
	t.Cleanup(func() { dropDatabase(t, ctr, dbName) })

	db, connectionString, err := openAndEnableCDC(t.Context(), ctr, dbName)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, &TestDB{db, t}
}

func startMSSQLServerContainer(ctx context.Context) (*tcmssql.MSSQLServerContainer, error) {
	return tcmssql.Run(ctx,
		"mcr.microsoft.com/mssql/server:2025-latest",
		testcontainers.WithImagePlatform("linux/amd64"),
		tcmssql.WithAcceptEULA(),
		tcmssql.WithPassword("YourStrong!Passw0rd"),
		testcontainers.WithEnv(map[string]string{
			"MSSQL_AGENT_ENABLED": "true",
		}),
	)
}

// createDatabase waits until the master database accepts connections, then creates dbName if it does not exist.
// An empty dbName only waits.
func createDatabase(ctx context.Context, ctr *tcmssql.MSSQLServerContainer, dbName string) error {
	masterConn, err := ctr.ConnectionString(ctx, "database=master", "encrypt=disable")
	if err != nil {
		return fmt.Errorf("master connection string: %w", err)
	}

	var lastErr error
	ok := eventually(ctx, 30*time.Second, 2*time.Second, func() bool {
		masterDB, openErr := sql.Open("mssql", masterConn)
		if openErr != nil {
			lastErr = openErr
			return false
		}
		defer masterDB.Close()

		if openErr = masterDB.PingContext(ctx); openErr != nil {
			lastErr = openErr
			return false
		}

		if dbName == "" {
			return true
		}
		query := fmt.Sprintf(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'%s')
			BEGIN
				CREATE DATABASE [%s];
			END;`, dbName, dbName)
		if _, openErr = masterDB.ExecContext(ctx, query); openErr != nil {
			lastErr = openErr
			return false
		}

		return true
	})
	if !ok {
		return fmt.Errorf("create database %q: %w", dbName, lastErr)
	}
	return nil
}

func openAndEnableCDC(ctx context.Context, ctr *tcmssql.MSSQLServerContainer, dbName string) (*sql.DB, string, error) {
	connStr, err := ctr.ConnectionString(ctx, "database="+dbName, "encrypt=disable")
	if err != nil {
		return nil, "", fmt.Errorf("connection string: %w", err)
	}

	var (
		db      *sql.DB
		lastErr error
	)
	ok := eventually(ctx, 30*time.Second, 2*time.Second, func() bool {
		if db != nil {
			db.Close()
		}
		var openErr error
		db, openErr = sql.Open("mssql", connStr)
		if openErr != nil {
			lastErr = openErr
			return false
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		if openErr = db.PingContext(ctx); openErr != nil {
			lastErr = openErr
			db.Close()
			db = nil
			return false
		}

		// Add the CDC jobs here, before sp_cdc_enable_table needs them. sp_cdc_enable_table adds them with
		// @start_job = 1, which waits about 3.4s for each job to start. Adding them with @start_job = 0 and then
		// starting the capture job takes about 1s in total. The cleanup job runs on a daily schedule, so it does not
		// have to start. The IF guards and the "already running" check make a retry safe. The guards read
		// msdb.dbo.sysjobs, because msdb.dbo.cdc_jobs does not exist before the first CDC job is added.
		cdcJobsMu.Lock()
		_, openErr = db.ExecContext(ctx, `
			IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0
				EXEC sys.sp_cdc_enable_db;
			IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'cdc.' + DB_NAME() + N'_capture')
				EXEC sys.sp_cdc_add_job @job_type = N'capture', @start_job = 0;
			IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'cdc.' + DB_NAME() + N'_cleanup')
				EXEC sys.sp_cdc_add_job @job_type = N'cleanup', @start_job = 0;`)
		if openErr == nil {
			_, openErr = db.ExecContext(ctx, "EXEC sys.sp_cdc_start_job @job_type = N'capture';")
			if openErr != nil && strings.Contains(openErr.Error(), "already running") {
				openErr = nil
			}
		}
		cdcJobsMu.Unlock()
		if openErr != nil {
			lastErr = openErr
			db.Close()
			db = nil
			return false
		}

		return true
	})
	if !ok {
		if db != nil {
			db.Close()
		}
		return nil, "", fmt.Errorf("enable CDC on %q: %w", dbName, lastErr)
	}
	return db, connStr, nil
}

func eventually(ctx context.Context, timeout, tick time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(tick):
		}
	}
}

// MustSetupTestWithMicrosoftSQLServerVersion creates a database for the test on the shared Microsoft SQL Server
// container, and returns its connection string and a raw sql.DB connected to it.
// Unlike SetupTestWithMicrosoftSQLServerVersion, this does not enable CDC.
// The database is dropped when the test completes.
func MustSetupTestWithMicrosoftSQLServerVersion(t *testing.T) (string, *sql.DB) {
	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)

	require.NoError(t, createDatabase(t.Context(), ctr, dbName))
	t.Cleanup(func() { dropDatabase(t, ctr, dbName) })

	connectionString, err := ctr.ConnectionString(t.Context(), "database="+dbName, "encrypt=disable")
	require.NoError(t, err)

	db, err := sql.Open("mssql", connectionString)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(t.Context()))
	return connectionString, db
}
