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

func TestIntegration_OracleDBCDC_ConcurrentSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t, "latest")
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo2", "CREATE TABLE testdb.foo2 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.bar", "CREATE TABLE testdb.bar (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	// Insert 3000 rows across tables for initial snapshot streaming
	want := 3000
	for range 1000 {
		db.MustExec("INSERT INTO testdb.foo (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb.foo2 (id) VALUES (DEFAULT)")
		db.MustExec("INSERT INTO testdb.bar (id) VALUES (DEFAULT)")
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
  include: ["TESTDB.FOO", "TESTDB.FOO2", "TESTDB.BAR"]
  exclude: ["TESTDB.DOESNOTEXIST"]`

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

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegration_OracleDBCDC_SnapshotAndStreaming_AllTypes(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

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

		-- Large Object Data Types
		varcharmax_col    CLOB,
		nvarcharmax_col   NCLOB,
		varbinarymax_col  BLOB,

		-- Other Data Types
		bit_col           NUMBER(1),                    -- Boolean-like (0,1,NULL)
		xml_col           XMLTYPE,
		json_col          CLOB                          -- JSON stored as CLOB
	)`
	err := db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.all_data_types", q)
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
		varcharmax_col, nvarcharmax_col, varbinarymax_col,
		bit_col, xml_col, json_col
	) VALUES (
		:1, :2, :3, :4,
		:5, :6, :7, :8,
		:9, :10, :11, :12,
		:13, :14, :15, :16,
		:17, :18, :19, :20,
		:21, :22, :23,
		:24, :25, :26)`

	t.Log("Inserting snapshot data...")
	{
		// insert min
		db.MustExecContext(t.Context(), query,
			0,                    // tinyint min
			-32768,               // smallint min
			-2147483648,          // int min
			-9223372036854775808, // bigint min
			"-9999999999999999999999999999.9999999999", // decimal min as string
			"-999999999999999.99999",                   // numeric min as string
			-1.79e+308,                                 // float min
			-3.40e+38,                                  // real min
			"0001-01-01",                               // date min
			"1753-01-01 00:00:00.000",                  // datetime min (timestamp)
			"0001-01-01 00:00:00.0000000",              // datetime2 min (timestamp)
			"1900-01-01 00:00:00",                      // smalldatetime min (timestamp)
			"0001-01-01 00:00:00.0000000",              // time (stored as timestamp)
			"0001-01-01 00:00:00.0000000 -14:00",       // timestamp with time zone
			"AAAAAAAAAA",                               // char(10)
			"",                                         // varchar2(255)
			"АААААААААА",                               // nchar(10)
			"",                                         // nvarchar2(255)
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // raw(16)
			[]byte{0x00},    // raw(255)
			"",              // clob
			"",              // nclob
			[]byte{0x00},    // blob
			0,               // bit (number)
			"<root></root>", // xmltype
			"{}",            // json (clob)
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
  include: ["testdb.all_data_types"]`

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))

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

		// Wait for snapshot to complete (should have 1 batch with min values)
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			return len(outBatches) == 1
		}, time.Second*30, time.Millisecond*100)
	}

	t.Log("Snapshot record(s) received, testing streaming...")
	{
		// insert max
		db.MustExecContext(t.Context(), query,
			255,                 // tinyint max
			32767,               // smallint max
			2147483647,          // int max
			9223372036854775807, // bigint max
			"9999999999999999999999999999.9999999999", // decimal max as string
			"999999999999999.99999",                   // numeric max as string
			1.79e+308,                                 // float max
			3.40e+38,                                  // real max
			"9999-12-31",                              // date max
			"9999-12-31 23:59:59.997",                 // datetime max (timestamp)
			"9999-12-31 23:59:59.9999999",             // datetime2 max (timestamp)
			"2079-06-06 23:59:00",                     // smalldatetime max (timestamp)
			"0001-01-01 23:59:59.9999999",             // time max (stored as timestamp)
			"9999-12-31 23:59:59.9999999 +14:00",      // timestamp with time zone max
			"ZZZZZZZZZZ",                              // char(10)
			"Max varchar value",                       // varchar2(255)
			"ZZZZZZZZZZ",                              // nchar(10)
			"Max nvarchar value",                      // nvarchar2(255)
			make([]byte, 16),                          // raw(16) filled with zeros
			make([]byte, 255),                         // raw(255) max
			"Max varchar(max)",                        // clob
			"Max nvarchar(max)",                       // nclob
			make([]byte, 255),                         // blob (big buffer for testing)
			1,                                         // bit max (number)
			"<root>max</root>",                        // xmltype
			`{"max": true}`,                           // json (clob)
		)

		// verify sum of records
		want := 2
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			return len(outBatches) == want
		}, time.Second*30, time.Millisecond*100)
		require.NoError(t, stream.StopWithin(time.Second*10))
		require.Lenf(t, outBatches, want, "Expected %d batches but got %d", want, len(outBatches))

		// assert min - Oracle-specific serialization
		require.JSONEq(t, `{
		"bigint_col": -9223372036854775808,
		"binary_col": "AAAAAAAAAAAAAAAAAAAAAA==",
		"bit_col": 0,
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
		}`, outBatches[0], "Failed to assert min result")

		// assert max - Oracle-specific serialization
		require.JSONEq(t, `{
		"bigint_col": 9223372036854775807,
		"binary_col": "AAAAAAAAAAAAAAAAAAAAAA==",
		"bit_col": 1,
		"char_col": "ZZZZZZZZZZ",
		"date_col": "9999-12-31T00:00:00Z",
		"datetime2_col": "9999-12-31T23:59:59.9999999Z",
		"datetime_col": "9999-12-31T23:59:59.997Z",
		"datetimeoffset_col": "9999-12-31T23:59:59.9999999+14:00",
		"decimal_col": 9999999999999999999999999999.9999999999,
		"float_col": 1.79e+308,
		"int_col": 2147483647,
		"json_col": "{\"max\": true}",
		"nchar_col": "ZZZZZZZZZZ",
		"numeric_col": 999999999999999.99999,
		"nvarchar_col": "Max nvarchar value",
		"nvarcharmax_col": "Max nvarchar(max)",
		"real_col": 3.3999999521443642e+38,
		"smalldatetime_col": "2079-06-06T23:59:00Z",
		"smallint_col": 32767,
		"time_col": "0001-01-01T23:59:59.9999999Z",
		"tinyint_col": 255,
		"varbinary_col": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"varbinarymax_col": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"varchar_col": "Max varchar value",
		"varcharmax_col": "Max varchar(max)",
		"xml_col": "\u003croot\u003emax\u003c/root\u003e"
		}`, outBatches[1], "Failed to assert max result")
	}
}
