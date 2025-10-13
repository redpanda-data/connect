// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

func TestIntegration_MicrosoftSQLServerCDC_SnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)

	t.Run("With Default SQL Server Cache", func(t *testing.T) {
		t.Parallel()

		// Create tables
		connStr, db := setupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "test.foo", "CREATE TABLE test.foo (id INT IDENTITY(1,1) PRIMARY KEY);"))
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.foo", "CREATE TABLE dbo.foo (id INT IDENTITY(1,1) PRIMARY KEY);"))
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.bar", "CREATE TABLE dbo.bar (id INT IDENTITY(1,1) PRIMARY KEY);"))

		// Insert 3000 rows across tables for initial snapshot streaming
		want := 3000
		for range 1000 {
			db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
			db.MustExec("INSERT INTO dbo.foo DEFAULT VALUES")
			db.MustExec("INSERT INTO dbo.bar DEFAULT VALUES")
		}

		// wait for changes to propagate to change tables
		time.Sleep(5 * time.Second)

		var (
			outBatches   []string
			outBatchesMu sync.Mutex
			stream       *service.Stream
			err          error
		)
		t.Log("Lauching component...")
		{
			cfg := `
microsoft_sql_server_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  include: ["test.foo", "dbo.foo", "dbo.bar"]
  exclude: ["dbo.doesnotexist"]`

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: DEBUG`))

			require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				msgBytes, err := mb[0].AsBytes()
				require.NoError(t, err)
				outBatchesMu.Lock()
				outBatches = append(outBatches, string(msgBytes))
				outBatchesMu.Unlock()
				return nil
			}))

			stream, err = streamBuilder.Build()
			require.NoError(t, err)
			license.InjectTestService(stream.Resources())

			go func() {
				err = stream.Run(t.Context())
				require.NoError(t, err)
			}()

			t.Log("Verifying snapshot changes...")
			assert.Eventually(t, func() bool {
				outBatchesMu.Lock()
				defer outBatchesMu.Unlock()

				got := len(outBatches)
				if got > want {
					t.Fatalf("Wanted %d snapshot messages but got %d", want, got)
				}
				return got == want
			}, time.Minute*5, time.Second*1)
		}

		t.Log("Verifying streaming changes...")
		{
			// insert 3000 more for streaming changes
			for range 1000 {
				db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
				db.MustExec("INSERT INTO dbo.foo DEFAULT VALUES")
				db.MustExec("INSERT INTO dbo.bar DEFAULT VALUES")
			}

			outBatches = nil
			assert.Eventually(t, func() bool {
				outBatchesMu.Lock()
				defer outBatchesMu.Unlock()

				got := len(outBatches)
				if got > want {
					t.Fatalf("Wanted %d streaming changes but got %d", want, got)
				}
				return got == want
			}, time.Minute*5, time.Second*1)

		}

		require.NoError(t, stream.StopWithin(time.Second*10))
	})

	t.Run("With Cache Component", func(t *testing.T) {
		t.Parallel()

		// Create tables
		connStr, db := setupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "test.foo", "CREATE TABLE test.foo (id INT IDENTITY(1,1) PRIMARY KEY);"))
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.foo", "CREATE TABLE dbo.foo (id INT IDENTITY(1,1) PRIMARY KEY);"))
		require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.bar", "CREATE TABLE dbo.bar (id INT IDENTITY(1,1) PRIMARY KEY);"))

		// Insert 3000 rows across tables for initial snapshot streaming
		want := 3000
		for range 1000 {
			db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
			db.MustExec("INSERT INTO dbo.foo DEFAULT VALUES")
			db.MustExec("INSERT INTO dbo.bar DEFAULT VALUES")
		}

		// wait for changes to propagate to change tables
		time.Sleep(5 * time.Second)

		var (
			outBatches   []string
			outBatchesMu sync.Mutex
			stream       *service.Stream
			err          error
		)
		t.Log("Lauching component...")
		{
			cfg := `
microsoft_sql_server_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  include: ["test.foo", "dbo.foo", "dbo.bar"]
  exclude: ["dbo.doesnotexist"]
  checkpoint_cache: "foocache"`

			cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
			require.NoError(t, streamBuilder.AddCacheYAML(cacheConf))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: DEBUG`))

			require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				msgBytes, err := mb[0].AsBytes()
				require.NoError(t, err)
				outBatchesMu.Lock()
				outBatches = append(outBatches, string(msgBytes))
				outBatchesMu.Unlock()
				return nil
			}))

			stream, err = streamBuilder.Build()
			require.NoError(t, err)
			license.InjectTestService(stream.Resources())

			go func() {
				err = stream.Run(t.Context())
				require.NoError(t, err)
			}()

			t.Log("Verifying snapshot changes...")
			assert.Eventually(t, func() bool {
				outBatchesMu.Lock()
				defer outBatchesMu.Unlock()

				got := len(outBatches)
				if got > want {
					t.Fatalf("Wanted %d snapshot changes but got %d", want, got)
				}
				return got == want
			}, time.Minute*5, time.Second*1)
		}

		t.Log("Verifying streaming changes...")
		{
			// insert 3000 more for streaming changes
			for range 1000 {
				db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
				db.MustExec("INSERT INTO dbo.foo DEFAULT VALUES")
				db.MustExec("INSERT INTO dbo.bar DEFAULT VALUES")
			}

			outBatches = nil
			assert.Eventually(t, func() bool {
				outBatchesMu.Lock()
				defer outBatchesMu.Unlock()

				got := len(outBatches)
				if got > want {
					t.Fatalf("Wanted %d streaming changes but got %d", want, got)
				}
				return got == want
			}, time.Minute*5, time.Second*1)

		}

		require.NoError(t, stream.StopWithin(time.Second*10))
	})
}

func TestIntegration_MicrosoftSQLServerCDC_ResumesFromCheckpoint(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create table
	connStr, db := setupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
	require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "test.foo", "CREATE TABLE test.foo (id INT IDENTITY(1,1) PRIMARY KEY);"))

	cfg := `
microsoft_sql_server_cdc:
  connection_string: %s
  stream_snapshot: false
  include: ["test.foo"]
  checkpoint_cache_table_name: dbo.checkpoint_cache`

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
	)

	t.Log("Lauching component to stream initial data...")
	{
		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatchesMu.Lock()
			outBatches = append(outBatches, string(msgBytes))
			outBatchesMu.Unlock()
			return nil
		}))

		stream, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		// --- launch input and insert initial rows for consumption
		for range 1000 {
			db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
		}
		go func() {
			require.NoError(t, stream.Run(t.Context()))
		}()

		time.Sleep(time.Second * 5)

		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			return len(outBatches) == 1000
		}, time.Minute*5, time.Millisecond*100)
		require.NoError(t, stream.StopWithin(time.Second*10))
	}

	t.Log("Relaunching component to resume from checkpoint...")
	{
		// --- now stopped, insert more rows
		for range 1000 {
			db.MustExec("INSERT INTO test.foo DEFAULT VALUES")
		}

		streamResume, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(streamResume.Resources())
		go func() {
			require.NoError(t, streamResume.Run(t.Context()))
		}()

		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			return len(outBatches) == 2000
		}, time.Minute*5, time.Millisecond*100)

		require.Contains(t, outBatches[len(outBatches)-1], "2000")
		require.NoError(t, streamResume.StopWithin(time.Second*10))
	}
}

func TestIntegration_MicrosoftSQLServerCDC_OrderingOfIterator(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create table
	connStr, db := setupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
	require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.foo", `CREATE TABLE dbo.foo (a INT PRIMARY KEY);`))
	require.NoError(t, db.createTableWithCDCEnabledIfNotExists(t.Context(), "boo.bar", `CREATE TABLE boo.bar (b INT PRIMARY KEY);`))

	// Data across change tables will have the same LSN but unique
	// command IDs (and in rare cases sequence values that are harder to test)
	_, err := db.Exec(`
	BEGIN TRANSACTION
	DECLARE @i INT = 1;
	WHILE @i <= 10
	BEGIN
		INSERT INTO dbo.foo (a) VALUES (@i);
		INSERT INTO boo.bar (b) VALUES (@i);
		SET @i += 1;
	END
	COMMIT TRANSACTION`)
	require.NoError(t, err)

	cfg := `
microsoft_sql_server_cdc:
  connection_string: %s
  stream_snapshot: false
  include: ["dbo.foo", "boo.bar"]`

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))

	var outBatches []string
	var outBatchesMu sync.Mutex
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchesMu.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchesMu.Unlock()
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		err = stream.Run(t.Context())
		require.NoError(t, err)
	}()

	assert.Eventually(t, func() bool {
		outBatchesMu.Lock()
		defer outBatchesMu.Unlock()
		return len(outBatches) == 20
	}, time.Minute*5, time.Millisecond*100)

	var want []string
	for i := 1; i <= 10; i++ {
		want = append(want, fmt.Sprintf(`{"a":%d}`, i))
		want = append(want, fmt.Sprintf(`{"b":%d}`, i))
	}
	require.Equal(t, want, outBatches, "Order of output does not match expected")
	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegration_MicrosoftSQLServerCDC_AllTypes(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := setupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
	q := `
	CREATE TABLE dbo.all_data_types (
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
	);`
	err := db.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.all_data_types", q)
	require.NoError(t, err)

	// insert min
	allDataTypesQuery := `
	INSERT INTO dbo.all_data_types (
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

	cfg := `
microsoft_sql_server_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 100
  include: ["all_data_types"]`

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))

	var outBatches []string
	var outBatchesMu sync.Mutex
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchesMu.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchesMu.Unlock()
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		err = stream.Run(t.Context())
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
		outBatchesMu.Lock()
		defer outBatchesMu.Unlock()
		return len(outBatches) == 2
	}, time.Second*30, time.Millisecond*100)
	require.NoError(t, stream.StopWithin(time.Second*10))

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
	t.Log("Creating test tables 'test.users'...")
	testDB := &testDB{db, t}
	err = testDB.createTableWithCDCEnabledIfNotExists(t.Context(), "test.users", `
		CREATE TABLE test.users (
			id INT IDENTITY(1,1) PRIMARY KEY,
			name NVARCHAR(100) NOT NULL,
			surname NVARCHAR(100) NOT NULL,
			about NVARCHAR(255) NOT NULL,
			email NVARCHAR(255) NOT NULL,
			date_of_birth DATE NULL,
			join_date DATE NULL,
			created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
			is_active BIT NOT NULL DEFAULT 1,
			login_count INT NOT NULL DEFAULT 0,
			balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
		);`)
	require.NoError(t, err)

	t.Log("Creating test tables 'dbo.products'...")
	err = testDB.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.products", `
	CREATE TABLE dbo.products (
		id INT IDENTITY(1,1) PRIMARY KEY,
		name NVARCHAR(100),
		created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
		balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
	);`)
	require.NoError(t, err)

	t.Log("Creating test tables 'dbo.cart'...")
	err = testDB.createTableWithCDCEnabledIfNotExists(t.Context(), "dbo.cart", `
		CREATE TABLE dbo.cart (
			id INT IDENTITY(1,1) PRIMARY KEY,
			name NVARCHAR(100) NOT NULL,
			email NVARCHAR(255) NOT NULL,
			date_of_birth DATE NULL,
			created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
			is_active BIT NOT NULL DEFAULT 1,
			login_count INT NOT NULL DEFAULT 0,
			balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
		);`)
	require.NoError(t, err)

	// --- insert test data
	// t.Log("Inserting test data into products table...")
	// _, err = testDB.Exec(`
	// DECLARE @i INT = 1;
	// WHILE @i <= 50000
	// BEGIN
	// 	INSERT INTO products (id, name)
	// 	VALUES (@i, CONCAT('product-', @i));
	// 	SET @i += 1;
	// END`)
	// require.NoError(t, err)

	// t.Log("Inserting test data into users table...")
	// _, err = testDB.Exec(`
	// DECLARE @i INT = 1;
	// WHILE @i <= 50000
	// BEGIN
	// 	INSERT INTO users (id, name)
	// 	VALUES (@i, CONCAT('user-', @i));
	// 	SET @i += 1;
	// END`)
	// require.NoError(t, err)

	// Note: use this rather than above for much larger data sets, though they result in the same LSN
	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO test.users (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
	SELECT
		CONCAT('user-', n),                                -- name
		CONCAT('surname-', n),                             -- surname
		CONCAT('about-', n),							   -- about
		CONCAT('user', n, '@example.com'),                 -- email
		DATEADD(DAY, -n % 10000, GETDATE()),               -- date_of_birth, spread over ~27 years
		SYSUTCDATETIME(),                                  -- join_date
		SYSUTCDATETIME(),                                  -- created_at
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,             -- is_active alternating 1/0
		n % 100,                                           -- login_count between 0-99
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- balance
	FROM Numbers;
	`)

	require.NoError(t, err)
	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO dbo.products (name, created_at, balance)
		SELECT
		CONCAT('product-', n),                             -- name
		SYSUTCDATETIME(),                                  -- created_at
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- balance
	FROM Numbers;
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO dbo.cart (name, email, date_of_birth, created_at, is_active, login_count, balance)
	SELECT
		CONCAT('cart-', n),                                -- name
		CONCAT('cart', n, '@example.com'),                 -- email
		DATEADD(DAY, -n % 10000, GETDATE()),               -- date_of_birth, spread over ~27 years
		SYSUTCDATETIME(),                                  -- created_at
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,             -- is_active alternating 1/0
		n % 100,                                           -- login_count between 0-99
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- balance
	FROM Numbers;
	`)
	require.NoError(t, err)
}

type testDB struct {
	*sql.DB

	t *testing.T
}

func (db *testDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.t, err)
}

func (db *testDB) createTableWithCDCEnabledIfNotExists(ctx context.Context, fullTableName, createTableQuery string, _ ...any) error {
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
		EXEC('CREATE SCHEMA rpcn');
		EXEC('CREATE SCHEMA %s');
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

func setupTestWithMicrosoftSQLServerVersion(t *testing.T, version string) (string, *testDB) {
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
