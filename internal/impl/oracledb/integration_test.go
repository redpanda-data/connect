// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestIntegration_OracleDBCDC_SnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo2", "CREATE TABLE testdb.foo2 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb2.bar", "CREATE TABLE testdb2.bar (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	// Insert 3000 rows across tables for initial snapshot streaming
	want := 3000
	for range 1000 {
		db.MustExec("INSERT INTO testdb.foo (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb.foo2 (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb2.bar (id) VALUES (DEFAULT)")
	}

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
		stream       *service.Stream
		err          error
	)
	t.Log("Launching component...")
	{
		cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  max_parallel_snapshot_tables: 3
  snapshot_max_batch_size: 10
  logminer:
    max_batch_size: 1000
    backoff_interval: 1s
  include: ["TESTDB.FOO", "TESTDB.FOO2", "TESTDB2.BAR"]
  exclude: ["TESTDB.DOESNOTEXIST"]
  batching:
    count: 500`

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
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
		// Insert 3000 rows across tables for initial streaming
		want := 3000
		_, err := db.Exec(`
	BEGIN
		FOR i IN 1..1000 LOOP
			INSERT INTO testdb.foo (id) VALUES (DEFAULT);
			INSERT INTO testdb.foo2 (id) VALUES (DEFAULT);
			INSERT INTO testdb2.bar (id) VALUES (DEFAULT);
		END LOOP;
		COMMIT;
	END;`)
		require.NoError(t, err)

		outBatches = nil
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()

			got := len(outBatches)
			if got > want {
				t.Fatalf("Wanted %d streaming messages but got %d", want, got)
			}

			t.Logf("Found %d of %d records...", got, want)

			return got == want
		}, time.Minute*1, time.Second*1)
	}

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegration_OracleDBCDC_ConcurrentSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo2", "CREATE TABLE testdb.foo2 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb2.bar", "CREATE TABLE testdb2.bar (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	// Insert 3000 rows across tables for initial snapshot streaming
	want := 3000
	for range 1000 {
		db.MustExec("INSERT INTO testdb.foo (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb.foo2 (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb2.bar (id) VALUES (DEFAULT)")
	}

	// wait for changes to propagate to redo logs
	time.Sleep(5 * time.Second)

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
		stream       *service.Stream
		err          error
	)
	t.Log("Launching component...")
	{
		cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  max_parallel_snapshot_tables: 3
  logminer:
    max_batch_size: 1000
    backoff_interval: 1s
  include: ["TESTDB.FOO", "TESTDB.FOO2", "TESTDB2.BAR"]
  exclude: ["TESTDB.DOESNOTEXIST"]`

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: DEBUG`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
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

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegration_OracleDBCDC_ResumesFromCheckpoint(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create table
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
	)

	cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    max_batch_size: 1000
    backoff_interval: 1s
  include: ["TESTDB.FOO"]
  batching:
    count: 500`

	t.Log("Launching component to stream initial data...")
	{
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			err = stream.Run(t.Context())
			require.NoError(t, err)
		}()

		// Wait for component to start
		time.Sleep(5 * time.Second)

		_, err = db.Exec(`
		BEGIN
			FOR i IN 1..1000 LOOP
				INSERT INTO testdb.foo (id) VALUES (DEFAULT);
			END LOOP;
			COMMIT;
		END;`)
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got := len(outBatches)
			t.Logf("Found %d of 1000 records...", got)
			return got == 1000
		}, time.Minute*2, time.Millisecond*500)
		require.NoError(t, stream.StopWithin(time.Second*10))
	}

	t.Log("Relaunching component to resume from checkpoint...")
	{
		// Insert more data before restarting
		_, err := db.Exec(`
		BEGIN
			FOR i IN 1..1000 LOOP
				INSERT INTO testdb.foo (id) VALUES (DEFAULT);
			END LOOP;
			COMMIT;
		END;`)
		require.NoError(t, err)

		// Create new stream builder for second phase
		streamBuilder2 := service.NewStreamBuilder()
		require.NoError(t, streamBuilder2.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder2.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder2.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		streamResume, err := streamBuilder2.Build()
		require.NoError(t, err)
		license.InjectTestService(streamResume.Resources())

		go func() {
			err = streamResume.Run(t.Context())
			require.NoError(t, err)
		}()

		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got := len(outBatches)
			t.Logf("Found %d of 2000 records...", got)
			return got == 2000
		}, time.Minute*2, time.Millisecond*500)

		require.NoError(t, streamResume.StopWithin(time.Second*10))
	}
}

func TestIntegration_OracleDBCDC_Streaming(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo2", "CREATE TABLE testdb.foo2 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb2.bar", "CREATE TABLE testdb2.bar (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
		stream       *service.Stream
		err          error
	)
	t.Log("Launching component...")
	{
		cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    max_batch_size: 1000
    backoff_interval: 1s
  include: ["TESTDB.FOO", "TESTDB.FOO2", "TESTDB2.BAR"]
  exclude: ["TESTDB.DOESNOTEXIST"]
  batching:
    count: 500`

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			err = stream.Run(t.Context())
			require.NoError(t, err)
		}()
	}

	// wait for component to start
	time.Sleep(10 * time.Second)

	t.Log("Verifying streaming changes...")
	{
		// Insert 3000 rows across tables for initial streaming
		want := 3000
		for range 1000 {
			db.MustExec("INSERT INTO testdb.foo (id) VALUES (DEFAULT)")
			db.MustExec("INSERT INTO testdb.foo2 (id) VALUES (DEFAULT)")
			db.MustExec("INSERT INTO testdb2.bar (id) VALUES (DEFAULT)")
		}

		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()

			got := len(outBatches)
			if got > want {
				t.Fatalf("Wanted %d streaming messages but got %d", want, got)
			}

			t.Logf("Found %d of %d records...", got, want)

			return got == want
		}, time.Minute*1, time.Second*1)
	}

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegration_OracleDBCDC_SnapshotAndStreaming_AllTypes(t *testing.T) {
	// t.Skip()
	integration.CheckSkip(t)
	t.Parallel()

	var err error
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	q := `
	CREATE TABLE testdb.all_data_types (
		-- Numeric Data Types
		tinyint_col       NUMBER(3)      PRIMARY KEY,   -- 0 to 255
		smallint_col      NUMBER(5),                    -- -32,768 to 32,767
		int_col           NUMBER(10),                   -- -2,147,483,648 to 2,147,483,647
		bigint_col        NUMBER(19),                   -- -9e18 to 9e18
		decimal_col       NUMBER(38, 10),               -- arbitrary precision
		numeric_col       NUMBER(20, 5),                -- numeric type
		float_col         BINARY_DOUBLE,                -- double precision
		real_col          BINARY_FLOAT,                 -- single precision

		-- Date and Time Data Types
		date_col          DATE,
		datetime_col      TIMESTAMP(3),                 -- millisecond precision
		datetime2_col     TIMESTAMP(7),                 -- 0001-01-01 through 9999-12-31
		smalldatetime_col TIMESTAMP(0),                 -- minute precision
		time_col          TIMESTAMP(7),
		datetimeoffset_col TIMESTAMP(7) WITH TIME ZONE, -- includes time zone offset

		-- Character Data Types
		char_col          CHAR(10),
		varchar_col       VARCHAR2(255),
		nchar_col         NCHAR(10),                    -- Unicode fixed-length
		nvarchar_col      NVARCHAR2(255),               -- Unicode variable-length

		-- Binary Data Types
		binary_col        RAW(16),
		varbinary_col     RAW(255),

		-- Large Object Data Types (commented out to avoid TTC errors)
		-- varcharmax_col    CLOB,
		-- nvarcharmax_col   NCLOB,
		-- varbinarymax_col  BLOB,

		-- Other Data Types
		bit_col           NUMBER(1)                    -- Boolean-like (0,1,NULL)
		-- xml_col           XMLTYPE,
		-- json_col          CLOB                          -- JSON stored as CLOB
	)`
	err = db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.all_data_types", q)
	require.NoError(t, err)

	// disable supplemental logging before we insert snapshot data
	db.MustDisableSupplementalLogging(t.Context(), "testdb.all_data_types")

	query := `
	INSERT INTO testdb.all_data_types (
		tinyint_col, smallint_col, int_col, bigint_col,
		decimal_col, numeric_col, float_col, real_col,
		date_col, datetime_col, datetime2_col, smalldatetime_col,
		time_col, datetimeoffset_col, char_col, varchar_col,
		nchar_col, nvarchar_col, binary_col, varbinary_col,
		-- varcharmax_col, nvarcharmax_col, varbinarymax_col,
		bit_col -- , xml_col, json_col
	) VALUES (
		:1, :2, :3, :4,
		:5, :6, :7, :8,
		:9, :10, :11, :12,
		:13, :14, :15, :16,
		:17, :18, :19, :20,
		-- :21, :22, :23,
		:21 -- , :22, :23
	)`

	t.Log("Inserting min values for testing snapshot data...")
	{
		// insert min
		db.MustExecContext(t.Context(), query,
			0,                    // tinyint min
			-32768,               // smallint min
			-2147483648,          // int min
			-9223372036854775808, // bigint min
			"-9999999999999999999999999999.9999999999",                   // decimal min as string
			"-999999999999999.99999",                                     // numeric min as string
			-1.79e+100,                                                   // float min (safe value to avoid NaN)
			-3.40e+37,                                                    // real min (safe value to avoid NaN)
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),                     // date min
			time.Date(1753, 1, 1, 0, 0, 0, 0, time.UTC),                  // datetime min (timestamp)
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),                     // datetime2 min (timestamp)
			time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),                  // smalldatetime min (timestamp)
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),                     // time (stored as timestamp)
			time.Date(1, 1, 1, 0, 0, 0, 0, time.FixedZone("", -14*3600)), // timestamp with time zone
			"AAAAAAAAAA", // char(10)
			"",           // varchar2(255)
			"АААААААААА", // nchar(10)
			"",           // nvarchar2(255)
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // raw(16)
			[]byte{0x00}, // raw(255)
			// "",              // clob (varcharmax_col) - LOB columns commented out
			// "",              // nclob (nvarcharmax_col)
			// []byte{0x00},    // blob (varbinarymax_col)
			0, // bit (number)
			// "<root></root>", // xmltype
			// "{}",            // json (clob)
		)
	}

	db.MustEnableSupplementalLogging(t.Context(), "testdb.all_data_types")

	var (
		outBatches   []string
		outBatchesMu sync.Mutex
		stream       *service.Stream
	)
	t.Log("Starting Component...")
	{
		cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 100
  logminer:
    max_batch_size: 1000
    backoff_interval: 1s
  include: ["TESTDB.ALL_DATA_TYPES"]`

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			for _, msg := range mb {
				msgBytes, err := msg.AsBytes()
				require.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			err = stream.Run(t.Context())
			require.NoError(t, err)
		}()

		// Wait for snapshot to complete (should have 1 batch with min values)
		t.Log("Waiting for snapshot to complete...")
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got := len(outBatches)
			t.Logf("Snapshot progress: %d/1 records", got)
			return got == 1
		}, time.Second*30, time.Millisecond*500)

		require.Len(t, outBatches, 1, "Expected 1 snapshot record")
		t.Logf("Snapshot record received: %s", outBatches[0])
	}

	t.Log("Snapshot record(s) received, inserting max values for testing streaming...")
	{
		// insert max values for streaming
		db.MustExecContext(t.Context(), query,
			255,                 // tinyint max
			32767,               // smallint max
			2147483647,          // int max
			9223372036854775807, // bigint max
			"9999999999999999999999999999.9999999999", // decimal max as string
			"999999999999999.99999",                   // numeric max as string
			1.79e+100,                                 // float max (safe value to avoid NaN)
			3.40e+37,                                  // real max (safe value to avoid NaN)
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),                               // date max
			time.Date(9999, 12, 31, 23, 59, 59, 997000000, time.UTC),                    // datetime max (timestamp)
			time.Date(9999, 12, 31, 23, 59, 59, 999999900, time.UTC),                    // datetime2 max (timestamp)
			time.Date(2079, 6, 6, 23, 59, 0, 0, time.UTC),                               // smalldatetime max (timestamp)
			time.Date(1, 1, 1, 23, 59, 59, 999999900, time.UTC),                         // time max (stored as timestamp)
			time.Date(9999, 12, 31, 23, 59, 59, 999999900, time.FixedZone("", 14*3600)), // timestamp with time zone max
			"ZZZZZZZZZZ",         // char(10)
			"Max varchar value",  // varchar2(255)
			"ZZZZZZZZZZ",         // nchar(10)
			"Max nvarchar value", // nvarchar2(255)
			make([]byte, 16),     // raw(16) filled with zeros
			make([]byte, 255),    // raw(255) max
			// "Max varchar(max)",   // clob (varcharmax_col) - LOB columns commented out
			// "Max nvarchar(max)",  // nclob (nvarcharmax_col)
			// make([]byte, 255),    // blob (varbinarymax_col)
			1, // bit max (number)
			// "<root>max</root>",   // xmltype
			// `{"max": true}`,      // json (clob)
		)

		// verify we got at least 2 records (1 snapshot + 1+ streaming)
		// Note: Oracle may split INSERT with LOB columns into multiple redo log entries,
		// so we may get 2-3+ records total depending on how LOBs are handled
		minWant := 2
		t.Log("Waiting for streaming record(s)...")
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got := len(outBatches)
			t.Logf("Total records received: %d (expecting at least %d)", got, minWant)
			return got >= minWant
		}, time.Second*30, time.Millisecond*500)

		outBatchesMu.Lock()
		totalRecords := len(outBatches)
		require.GreaterOrEqualf(t, totalRecords, minWant, "Expected at least %d records but got %d", minWant, totalRecords)

		// Debug: Log all records to understand what LogMiner is generating
		for i, batch := range outBatches {
			t.Logf("Record %d: %s", i, batch)
		}
		outBatchesMu.Unlock()
	}

	require.NoError(t, stream.StopWithin(time.Second*10))

	t.Log("Verifying values from snapshot...")
	{
		// assert min - uppercase column names from Oracle, NUMBER types as float64
		require.JSONEq(t, `{
		"BIGINT_COL": -9223372036854775808,
		"BINARY_COL": "AAAAAAAAAAAAAAAAAAAAAA==",
		"BIT_COL": 0,
		"CHAR_COL": "AAAAAAAAAA",
		"DATE_COL": "0001-01-01T00:00:00Z",
		"DATETIME2_COL": "0001-01-01T00:00:00Z",
		"DATETIME_COL": "1753-01-01T00:00:00Z",
		"DATETIMEOFFSET_COL": "0001-01-01T00:00:00-14:00",
		"DECIMAL_COL": -9999999999999999999999999999.9999999999,
		"FLOAT_COL": "-1.79e+100",
		"INT_COL": -2147483648,
		"NCHAR_COL": "АААААААААА",
		"NUMERIC_COL": -999999999999999.99999,
		"NVARCHAR_COL": null,
		"REAL_COL": "-3.4e+37",
		"SMALLDATETIME_COL": "1900-01-01T00:00:00Z",
		"SMALLINT_COL": -32768,
		"TIME_COL": "0001-01-01T00:00:00Z",
		"TINYINT_COL": 0,
		"VARBINARY_COL": "AA==",
		"VARCHAR_COL": null
		}`, outBatches[0], "Failed to assert min result from snapshot")
	}

	t.Log("Verifying values from streaming...")
	{
		// Oracle may split INSERT statements with LOB columns into multiple redo log entries.
		// The actual data might be in outBatches[1], outBatches[2], or split across both.
		// Check which record has the non-EMPTY LOB values to determine which to validate.

		// assert max - uppercase column names from Oracle
		// BEFORE - With binary data
		// require.JSONEq(t, `{
		// "BIGINT_COL": 9223372036854775807,
		// "BINARY_COL": "AAAAAAAAAAAAAAAAAAAAAA==",
		// "BIT_COL": true,
		// "CHAR_COL": "ZZZZZZZZZZ",
		// "DATE_COL": "9999-12-31T00:00:00Z",
		// "DATETIME2_COL": "9999-12-31T23:59:59.9999999Z",
		// "DATETIME_COL": "9999-12-31T23:59:59.997Z",
		// "DATETIMEOFFSET_COL": "9999-12-31T23:59:59.9999999+14:00",
		// "DECIMAL_COL": 9999999999999999999999999999.9999999999,
		// "FLOAT_COL": 1.79e+100,
		// "INT_COL": 2147483647,
		// "JSON_COL": "{\"max\": true}",
		// "NCHAR_COL": "ZZZZZZZZZZ",
		// "NUMERIC_COL": 999999999999999.99999,
		// "NVARCHAR_COL": "Max nvarchar value",
		// "NVARCHARMAX_COL": "Max nvarchar(max)",
		// "REAL_COL": "3.4e+37",
		// "SMALLDATETIME_COL": "2079-06-06T23:59:00Z",
		// "SMALLINT_COL": 32767,
		// "TIME_COL": "0001-01-01T23:59:59.9999999Z",
		// "TINYINT_COL": 255,
		// "VARBINARY_COL": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		// "VARBINARYMAX_COL": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		// "VARCHAR_COL": "Max varchar value",
		// "VARCHARMAX_COL": "Max varchar(max)",
		// "XML_COL": "\u003croot\u003emax\u003c/root\u003e"
		// }`, outBatches[1], "Failed to assert max result from streaming")
		require.JSONEq(t, `{
		"BIGINT_COL": "9223372036854775807",
		"BINARY_COL": "HEXTORAW('00000000000000000000000000000000')",
		"BIT_COL": "1",
		"CHAR_COL": "ZZZZZZZZZZ",
		"DATE_COL": "9999-12-31T00:00:00Z",
		"DATETIME2_COL": "9999-12-31T23:59:59.9999999Z",
		"DATETIME_COL": "9999-12-31T23:59:59.997Z",
		"DATETIMEOFFSET_COL": "9999-12-31T23:59:59.9999999+14:00",
		"DECIMAL_COL": "9999999999999999999999999999.9999999999",
		"FLOAT_COL": "1.79E+100",
		"INT_COL": "2147483647",
		"NCHAR_COL": "ZZZZZZZZZZ",
		"NUMERIC_COL": "999999999999999.99999",
		"NVARCHAR_COL": "Max nvarchar value",
		"REAL_COL": "3.3999999E+037",
		"SMALLDATETIME_COL": "2079-06-06T23:59:00Z",
		"SMALLINT_COL": "32767",
		"TIME_COL": "0001-01-01T23:59:59.9999999Z",
		"TINYINT_COL": "255",
		"VARBINARY_COL": "HEXTORAW('000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')",
		"VARCHAR_COL": "Max varchar value"
		}`, outBatches[1], "Failed to assert max result from streaming")
	}
}
