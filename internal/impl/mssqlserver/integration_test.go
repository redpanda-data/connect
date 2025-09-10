// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// Test_ManualTesting_AddTestDataWithUniqueLSN adds data to an existing table and ensures each change has its own LSN
func Test_ManualTesting_AddTestDataWithUniqueLSN(t *testing.T) {
	t.Skip("This test requires a remote database to run. Aimed to test remote databases")

	// --- create database as master
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "master")
	var db *sql.DB
	var err error
	db, err = sql.Open("mssql", connectionString)
	require.NoError(t, err)

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	err = db.Ping()
	require.NoError(t, err)

	t.Log("Creating test database...")
	_, err = db.Exec(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
			BEGIN
				CREATE DATABASE testdb;
			END;`)
	require.NoError(t, err)
	db.Close()

	// --- connect to database and enable CDC
	connectionString = fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "testdb")
	db, err = sql.Open("mssql", connectionString)
	require.NoError(t, err)

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	err = db.Ping()
	require.NoError(t, err)

	// enable CDC on database
	t.Log("Enabling CDC on server...")
	_, err = db.Exec("EXEC sys.sp_cdc_enable_db;")
	require.NoError(t, err)

	// --- create tables and enable CDC on them
	t.Log("Creating test tables...")
	testDB := &testDB{db, t}
	testDB.createTableWithCDCEnabledIfNotExists("users", `
    CREATE TABLE users (
		id INT PRIMARY KEY,
		name NVARCHAR(100)
    );`)
	testDB.createTableWithCDCEnabledIfNotExists("products", `
    CREATE TABLE products (
		id INT PRIMARY KEY,
		name NVARCHAR(100)
    );`)

	// --- insert test data
	t.Log("Inserting test data into products table...")
	_, err = testDB.Exec(`
	DECLARE @i INT = 1;
	WHILE @i <= 50000
	BEGIN
		INSERT INTO products (id, name)
		VALUES (@i, CONCAT('product-', @i));
		SET @i += 1;
	END`)
	require.NoError(t, err)

	t.Log("Inserting test data into users table...")
	_, err = testDB.Exec(`
	DECLARE @i INT = 1;
	WHILE @i <= 50000
	BEGIN
		INSERT INTO users (id, name)
		VALUES (@i, CONCAT('user-', @i));
		SET @i += 1;
	END`)
	require.NoError(t, err)

	// Note: use this rather than above for much larger data sets, though they result in the same LSN
	// _, err = db.Exec(`
	// 	WITH Numbers AS (
	// 		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
	// 		FROM sys.all_objects a
	// 		CROSS JOIN sys.all_objects b
	// 	)
	// 	INSERT INTO users (id, name)
	// 	SELECT n, CONCAT('user-', n) FROM Numbers;
	// `)
	// require.NoError(t, err)
	// _, err = db.Exec(`
	// 	WITH Numbers AS (
	// 		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
	// 		FROM sys.all_objects a
	// 		CROSS JOIN sys.all_objects b
	// 	)
	// 	INSERT INTO products (id, name)
	// 	SELECT n, CONCAT('product-', n) FROM Numbers;
	// `)
	// require.NoError(t, err)
}

func TestIntegration_MSSQLServerCDC(t *testing.T) {
	connStr, db := setupTestWithMSSQLServerVersion(t, "2022-latest")

	// Create table
	db.createTableWithCDCEnabledIfNotExists("foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)

	// Insert 1000 rows for initial snapshot streaming
	for i := range 1000 {
		db.exec("INSERT INTO foo VALUES (?)", i)
	}

	template := fmt.Sprintf(`
mssql_server_cdc:
  connection_string: %s
  stream_snapshot: false
  snapshot_max_batch_size: 100
  tables:
    - foo
  checkpoint_cache: "foocache"
`, connStr)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 5)
	for i := 1000; i < 2000; i++ {
		db.exec("INSERT INTO foo VALUES (?)", i)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegration_MSSQLServerCDC_ResumesFromCheckpoint(t *testing.T) {
	connStr, db := setupTestWithMSSQLServerVersion(t, "2022-latest")

	// Create table
	db.createTableWithCDCEnabledIfNotExists("foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)

	template := fmt.Sprintf(`
mssql_server_cdc:
  connection_string: %s
  stream_snapshot: false
  tables:
    - foo
  checkpoint_cache: "foocache"
`, connStr)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	// --- launch input and insert initial rows for consumption
	for i := range 1000 {
		db.exec("INSERT INTO foo VALUES (?)", i)
	}
	go func() {
		require.NoError(t, streamOut.Run(t.Context()))
	}()

	time.Sleep(time.Second * 5)

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1000
	}, time.Minute*5, time.Millisecond*100)

	// --- stop initial input and then insert more rows
	require.NoError(t, streamOut.StopWithin(time.Second*10))
	for i := 1000; i < 2000; i++ {
		db.exec("INSERT INTO foo VALUES (?)", i)
	}

	// --- relaunched input should load checkpoint
	streamOutResume, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOutResume.Resources())
	go func() {
		require.NoError(t, streamOutResume.Run(t.Context()))
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)

	require.Contains(t, outBatches[len(outBatches)-1], "1999")
	require.NoError(t, streamOutResume.StopWithin(time.Second*10))
}

func TestFuncIntegrationTestOrderingOfIterator(t *testing.T) {
	connStr, db := setupTestWithMSSQLServerVersion(t, "2022-latest")

	// Create table
	db.createTableWithCDCEnabledIfNotExists("foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)
	db.createTableWithCDCEnabledIfNotExists("bar", `CREATE TABLE bar (b INT PRIMARY KEY);`)

	// Data across change tables will have the same LSN but unique
	// command IDs (and in rare cases sequence values that are harder to test)
	_, err := db.Exec(`
	BEGIN TRANSACTION
	DECLARE @i INT = 1;
	WHILE @i <= 10
	BEGIN
		INSERT INTO foo (a) VALUES (@i);
		INSERT INTO bar (b) VALUES (@i);
		SET @i += 1;
	END
	COMMIT TRANSACTION`)
	require.NoError(t, err)

	template := fmt.Sprintf(`
mssql_server_cdc:
  connection_string: %s
  stream_snapshot: false
  tables:
    - foo
    - bar
  checkpoint_cache: "foocache"
`, connStr)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 20
	}, time.Minute*5, time.Millisecond*100)

	var want []string
	for i := 1; i <= 10; i++ {
		want = append(want, fmt.Sprintf(`{"a":%d}`, i))
		want = append(want, fmt.Sprintf(`{"b":%d}`, i))
	}
	require.Equal(t, want, outBatches, "Order of output does not match expected")
	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func BenchmarkStreamingCDCChanges(b *testing.B) {
	b.Skip()
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")
	db, err := sql.Open("mssql", connectionString)
	require.NoError(b, err)
	defer db.Close()

	ctStream := &changeTableStream{}

	err = ctStream.verifyChangeTables(b.Context(), db, []string{"users", "products"})
	require.NoError(b, err)

	b.ReportAllocs()
	// Reset timer to exclude setup time
	for b.Loop() {
		err := ctStream.readChangeTables(b.Context(), db, func(c *change) (LSN, error) {
			fmt.Printf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
			return c.startLSN, nil
		})
		require.NoError(b, err)
	}
}

type testDB struct {
	*sql.DB

	t *testing.T
}

func (db *testDB) exec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.t, err)
}

func (db *testDB) createTableWithCDCEnabledIfNotExists(tableName, query string, _ ...any) {
	cdc := fmt.Sprintf(`
		EXEC sys.sp_cdc_enable_table
		@source_schema = 'dbo',
		@source_name   = '%s',
		@role_name     = NULL;`, tableName)
	q := fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('dbo'))
		BEGIN
			%s
			%s
		END;`, tableName, query, cdc)
	db.exec(q)
}

func setupTestWithMSSQLServerVersion(t *testing.T, version string) (string, *testDB) {
	t.Parallel()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute
	// MS SQL Server specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server",
		Tag:        version,
		Env: []string{
			"ACCEPT_EULA=y",
			"MSSQL_SA_PASSWORD=YourStrong!Passw0rd",
			"MSSQL_AGENT_ENABLED=true",
		},
		Cmd:          []string{},
		ExposedPorts: []string{"1433:1433"},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("1433/tcp")
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "master")

	var db *sql.DB
	err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		_, err = db.Exec(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
			BEGIN
				CREATE DATABASE testdb;
			END;`)
		if err != nil {
			return err
		}
		db.Close()

		connectionString = fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "testdb")
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		// enable CDC on database
		if _, err = db.Exec("EXEC sys.sp_cdc_enable_db;"); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, &testDB{db, t}
}
