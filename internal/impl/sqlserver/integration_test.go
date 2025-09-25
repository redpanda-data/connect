// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlserver_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// Test_ManualTesting_AddTestDataWithUniqueLSN adds data to an existing table and ensures each change has its own LSN
func Test_ManualTesting_AddTestDataWithUniqueLSN(t *testing.T) {
	t.Skip("This test requires a remote database to run. Aimed to seed initial data in a remote test databases")

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
				ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;
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
	t.Log("Creating test tables 'users'...")
	testDB := &testDB{db, t}
	err = testDB.createTableWithCDCEnabledIfNotExists(t.Context(), "users", `
	CREATE TABLE users (
		id INT IDENTITY(1,1) PRIMARY KEY,
		name NVARCHAR(100)
	);`)
	require.NoError(t, err)

	t.Log("Creating test tables 'products'...")
	err = testDB.createTableWithCDCEnabledIfNotExists(t.Context(), "products", `
	CREATE TABLE products (
		id INT IDENTITY(1,1) PRIMARY KEY,
		name NVARCHAR(100)
	);`)
	require.NoError(t, err)

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
	// 	INSERT INTO users (name)
	// 	SELECT CONCAT('user-', n)
	// 	FROM Numbers;
	// `)

	// require.NoError(t, err)
	// _, err = db.Exec(`
	// 	WITH Numbers AS (
	// 		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
	// 		FROM sys.all_objects a
	// 		CROSS JOIN sys.all_objects b
	// 	)
	// 	INSERT INTO products (name)
	// 	SELECT CONCAT('product-', n)
	// 	FROM Numbers;
	// `)
	// require.NoError(t, err)
}

func TestIntegration_SQLServerCDC_SnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := setupTestWithSQLServerVersion(t, "2022-latest")

	// Create table
	err := db.createTableWithCDCEnabledIfNotExists(t.Context(), "foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)
	require.NoError(t, err)

	// Insert 1000 rows for initial snapshot streaming
	for i := range 1000 {
		db.MustExec("INSERT INTO foo VALUES (?)", i)
	}

	template := fmt.Sprintf(`
sql_server_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
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

	// insert 1000 more for streaming changes
	time.Sleep(time.Second * 5)
	for i := 1000; i < 2000; i++ {
		db.MustExec("INSERT INTO foo VALUES (?)", i)
	}

	want := 2000
	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()

		got := len(outBatches)
		t.Logf("found %d messages of %d", got, want)
		return got == want
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegration_SQLServerCDC_ResumesFromCheckpoint(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := setupTestWithSQLServerVersion(t, "2022-latest")

	// Create table
	err := db.createTableWithCDCEnabledIfNotExists(t.Context(), "foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)
	require.NoError(t, err)

	template := fmt.Sprintf(`
sql_server_cdc:
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
		db.MustExec("INSERT INTO foo VALUES (?)", i)
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
		db.MustExec("INSERT INTO foo VALUES (?)", i)
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

func TestIntegration_SQLServerCDC_OrderingOfIterator(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := setupTestWithSQLServerVersion(t, "2022-latest")

	// Create table
	err := db.createTableWithCDCEnabledIfNotExists(t.Context(), "foo", `CREATE TABLE foo (a INT PRIMARY KEY);`)
	require.NoError(t, err)
	err = db.createTableWithCDCEnabledIfNotExists(t.Context(), "bar", `CREATE TABLE bar (b INT PRIMARY KEY);`)
	require.NoError(t, err)

	// Data across change tables will have the same LSN but unique
	// command IDs (and in rare cases sequence values that are harder to test)
	_, err = db.Exec(`
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
sql_server_cdc:
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

func TestIntegration_SQLServerCDC_AllTypes(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := setupTestWithSQLServerVersion(t, "2022-latest")
	q := `
CREATE TABLE all_data_types (
    -- Numeric Data Types
    tinyint_col       TINYINT        PRIMARY KEY,   -- 0 to 255
    smallint_col      SMALLINT,                     -- -32,768 to 32,767
    int_col           INT,                          -- -2,147,483,648 to 2,147,483,647
    bigint_col        BIGINT,                       -- -9e18 to 9e18
    decimal_col       DECIMAL(38, 10),              -- arbitrary precision
    numeric_col       NUMERIC(20, 5),               -- alias of DECIMAL
    float_col         FLOAT(53),                    -- double precision
    real_col          REAL,                         -- single precision

    -- Date and Time Data Types
    date_col          DATE,
    datetime_col      DATETIME,                     -- 1753-01-01 through 9999-12-31
    datetime2_col     DATETIME2(7),                 -- 0001-01-01 through 9999-12-31
    smalldatetime_col SMALLDATETIME,                -- 1900-01-01 through 2079-06-06
    time_col          TIME(7),
    datetimeoffset_col DATETIMEOFFSET(7),           -- includes time zone offset

    -- Character Data Types
    char_col          CHAR(10),
    varchar_col       VARCHAR(255),
    nchar_col         NCHAR(10),                    -- Unicode fixed-length
    nvarchar_col      NVARCHAR(255),                -- Unicode variable-length

    -- Binary Data Types
    binary_col        BINARY(16),
    varbinary_col     VARBINARY(255),

    -- Large Object Data Types
    varcharmax_col    VARCHAR(MAX),
    nvarcharmax_col   NVARCHAR(MAX),
    varbinarymax_col  VARBINARY(MAX),

    -- Other Data Types
    bit_col           BIT,                          -- Boolean-like (0,1,NULL)
    xml_col           XML,
    json_col          NVARCHAR(MAX)                -- SQL Server has no native JSON, stored as NVARCHAR
);
`
	err := db.createTableWithCDCEnabledIfNotExists(t.Context(), "all_data_types", q)
	require.NoError(t, err)

	// insert min
	allDataTypesQuery := `
INSERT INTO all_data_types (
    tinyint_col, smallint_col, int_col, bigint_col,
    decimal_col, numeric_col, float_col, real_col,
    date_col, datetime_col, datetime2_col, smalldatetime_col,
    time_col, datetimeoffset_col, char_col, varchar_col,
    nchar_col, nvarchar_col, binary_col, varbinary_col,
    varcharmax_col, nvarcharmax_col, varbinarymax_col,
    bit_col, xml_col, json_col
) VALUES (
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?);`
	_, err = db.ExecContext(t.Context(), allDataTypesQuery,
		0,                    // tinyint min
		-32768,               // smallint min
		-2147483648,          // int min
		-9223372036854775808, // bigint min
		"-9999999999999999999999999999.9999999999", // decimal min as string
		"-999999999999999.99999",                   // numeric min as string
		-1.79e+308,                                 // float min
		-3.40e+38,                                  // real min
		"0001-01-01",                               // date min
		"1753-01-01 00:00:00.000",                  // datetime min
		"0001-01-01 00:00:00.0000000",              // datetime2 min
		"1900-01-01 00:00:00",                      // smalldatetime min
		"00:00:00.0000000",                         // time min
		"0001-01-01 00:00:00.0000000 -14:00",       // datetimeoffset min
		"AAAAAAAAAA",                               // char(10)
		"",                                         // varchar(255)
		"АААААААААА",                               // nchar(10)
		"",                                         // nvarchar(255)
		[]byte{0x00},                               // binary(1)
		[]byte{0x00},                               // varbinary(1)
		"",                                         // varchar(max)
		"",                                         // nvarchar(max)
		[]byte{0x00},                               // varbinary(max)
		false,                                      // bit
		"<root></root>",                            // xml
		"{}",
	)
	require.NoError(t, err, "Inserting snapshot test data to verify data types")

	template := fmt.Sprintf(`
sql_server_cdc:
  connection_string: %s
  stream_snapshot: true
  checkpoint_cache: "foocache"
  snapshot_max_batch_size: 100
  tables:
    - all_data_types
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

	// max
	_, err = db.ExecContext(t.Context(), allDataTypesQuery,
		255,                 // tinyint max
		32767,               // smallint max
		2147483647,          // int max
		9223372036854775807, // bigint max
		"9999999999999999999999999999.9999999999", // decimal max as string
		"999999999999999.99999",                   // numeric max as string
		1.79e+308,                                 // float max
		3.40e+38,                                  // real max
		"9999-12-31",                              // date max
		"9999-12-31 23:59:59.997",                 // datetime max
		"9999-12-31 23:59:59.9999999",             // datetime2 max
		"2079-06-06 23:59:00",                     // smalldatetime max
		"23:59:59.9999999",                        // time max
		"9999-12-31 23:59:59.9999999 +14:00",      // datetimeoffset max
		"ZZZZZZZZZZ",                              // char(10)
		"Max varchar value",                       // varchar(255)
		"ZZZZZZZZZZ",                              // nchar(10)
		"Max nvarchar value",                      // nvarchar(255)
		make([]byte, 16),                          // binary(16) filled with zeros (max size is fixed)
		make([]byte, 255),                         // varbinary(255) max
		"Max varchar(max)",                        // varchar(max)
		"Max nvarchar(max)",                       // nvarchar(max)
		make([]byte, 65535),                       // varbinary(max) (big buffer for testing)
		true,                                      // bit max
		"<root>max</root>",                        // xml
		`{"max": true}`,                           // json
	)
	require.NoError(t, err, "Inserting CDC test data to verify data types")

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2
	}, time.Second*30, time.Millisecond*100)
	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// assert min
	require.JSONEq(t, `{
	"bigint_col": -9223372036854775808,
	"binary_col": "AAAAAAAAAAAAAAAAAAAAAA==",
	"bit_col": "false",
	"char_col": "AAAAAAAAAA",
	"date_col": "0001-01-01T00:00:00Z",
	"datetime2_col": "0001-01-01T00:00:00Z",
	"datetime_col": "1753-01-01T00:00:00Z",
	"datetimeoffset_col": "0001-01-01T00:00:00-14:00",
	"decimal_col": -9999999999999999999999999999.9999999999,
	"float_col": -1.79e+308,
	"int_col": -2147483648,
	"json_col": "{}",
	"nchar_col": "АААААААААА",
	"numeric_col": -999999999999999.99999,
	"nvarchar_col": "",
	"nvarcharmax_col": "",
	"real_col": "-3.3999999521443642e+38",
	"smalldatetime_col": "1900-01-01T00:00:00Z",
	"smallint_col": -32768,
	"time_col": "0001-01-01T00:00:00Z",
	"tinyint_col": 0,
	"varbinary_col": "AA==",
	"varbinarymax_col": "AA==",
	"varchar_col": "",
	"varcharmax_col": "",
	"xml_col": "\u003croot/\u003e"
	}`, outBatches[0])

	// assert max
	require.JSONEq(t, `{
    "bigint_col": -9223372036854775808,
    "binary_col": "AAAAAAAAAAAAAAAAAAAAAA==",
    "bit_col": false,
    "char_col": "AAAAAAAAAA",
    "date_col": "0001-01-01T00:00:00Z",
    "datetime2_col": "0001-01-01T00:00:00Z",
    "datetime_col": "1753-01-01T00:00:00Z",
    "datetimeoffset_col": "0001-01-01T00:00:00-14:00",
    "decimal_col": "LTk5OTk5OTk5OTk5OTk5OTk5OTk5OTk5OTk5OTkuOTk5OTk5OTk5OQ==",
    "float_col": -1.79e+308,
    "int_col": -2147483648,
    "json_col": "{}",
    "nchar_col": "АААААААААА",
    "numeric_col": "LTk5OTk5OTk5OTk5OTk5OS45OTk5OQ==",
    "nvarchar_col": "",
    "nvarcharmax_col": "",
    "real_col": -3.3999999521443642e+38,
    "smalldatetime_col": "1900-01-01T00:00:00Z",
    "smallint_col": -32768,
    "time_col": "0001-01-01T00:00:00Z",
    "tinyint_col": 0,
    "varbinary_col": "AA==",
    "varbinarymax_col": "AA==",
    "varchar_col": "",
    "varcharmax_col": "",
    "xml_col": "\u003croot/\u003e"
	}`, outBatches[1])
}

type testDB struct {
	*sql.DB

	t *testing.T
}

func (db *testDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.t, err)
}

func (db *testDB) createTableWithCDCEnabledIfNotExists(ctx context.Context, tableName, createTableQuery string, _ ...any) error {
	enableSnapshot := `ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;`
	enableCDC := fmt.Sprintf(`
		EXEC sys.sp_cdc_enable_table
		@source_schema = 'dbo',
		@source_name   = '%s',
		@role_name     = NULL;`, tableName)
	q := fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('dbo'))
		BEGIN
			%s
			%s
			%s
		END;`, tableName, createTableQuery, enableCDC, enableSnapshot)
	if _, err := db.Exec(q); err != nil {
		return err
	}

	// wait for CDC table to be ready, this avoids time.sleeps
	for {
		var minLSN, maxLSN []byte
		// table isn't ready yet
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(?)", tableName).Scan(&minLSN); err != nil {
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

func setupTestWithSQLServerVersion(t *testing.T, version string) (string, *testDB) {
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

		// switch from using master to testdb as it avoids lots of permission issues with enabling CDC on tables
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
