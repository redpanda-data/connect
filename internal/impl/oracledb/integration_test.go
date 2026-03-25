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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestIntegrationOracleDBCDCSnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
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
    scn_window_size: 20000
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
				assert.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()

		t.Log("Verifying snapshot changes...")
		var got int
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got = len(outBatches)
			return got >= want
		}, time.Minute*5, time.Second*1)
		assert.Truef(t, (got == want), "Wanted %d snapshot messages but got %d", want, got)
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

		outBatchesMu.Lock()
		outBatches = nil
		outBatchesMu.Unlock()

		var got int
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got = len(outBatches)
			return got >= want
		}, time.Minute*5, time.Second*1)
		assert.Truef(t, (got == want), "Wanted %d streaming messages but got %d", want, got)
	}

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegrationOracleDBCDCConcurrentSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
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
    scn_window_size: 20000
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
				assert.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()

		t.Log("Verifying snapshot changes...")
		var got int
		assert.Eventually(t, func() bool {
			outBatchesMu.Lock()
			defer outBatchesMu.Unlock()
			got = len(outBatches)
			return got >= want
		}, time.Minute*5, time.Second*1)
		assert.Truef(t, (got == want), "Wanted %d snapshot messages but got %d", want, got)
	}

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegrationOracleDBCDCResumesFromCheckpoint(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create table
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
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
    scn_window_size: 20000
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
				assert.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
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
				assert.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		streamResume, err := streamBuilder2.Build()
		require.NoError(t, err)
		license.InjectTestService(streamResume.Resources())

		go func() {
			if err := streamResume.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
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

func TestIntegrationOracleDBCDCStreaming(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create tables
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo", "CREATE TABLE testdb.foo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val NUMBER)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.foo2", "CREATE TABLE testdb.foo2 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val NUMBER)"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb2.bar", "CREATE TABLE testdb2.bar (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val NUMBER)"))

	var (
		err     error
		stream  *service.Stream
		msgChan = make(chan *service.Message, 1)
	)

	cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.FOO", "TESTDB.FOO2", "TESTDB2.BAR"]
  exclude: ["TESTDB.DOESNOTEXIST"]
  batching:
    count: 500`

	t.Log("Launching component...")
	{
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			for _, msg := range mb {
				msgChan <- msg
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()
		go func() {
			<-t.Context().Done()
			close(msgChan)
		}()
	}

	// wait for component to start
	time.Sleep(10 * time.Second)

	// collectMessages reads messages from channel ready for assertion
	collectMessages := func(t *testing.T, want int) []*service.Message {
		t.Helper()
		msgs := make([]*service.Message, 0, want)
		for msg := range msgChan {
			msgs = append(msgs, msg)
			if len(msgs) == want {
				break
			}
			require.LessOrEqualf(t, len(msgs), want, "received too many messages")
		}
		require.Lenf(t, msgs, want, "channel closed before receiving %d messages, got %d", want, len(msgs))
		return msgs
	}

	// mustAssertMetadata ensures correct metadata exists in messages
	mustAssertMetadata := func(t *testing.T, operation string, msgs []*service.Message) {
		t.Helper()
		results := make(map[string][]*service.Message)
		for i, msg := range msgs {
			schema, ok := msg.MetaGet("database_schema")
			require.Truef(t, ok, "message %d missing 'database_schema' metadata", i)

			table, ok := msg.MetaGet("table_name")
			require.Truef(t, ok, "message %d missing 'table_name' metadata", i)

			key := fmt.Sprintf("%s.%s", schema, table)
			results[key] = append(results[key], msg)

			op, ok := msg.MetaGet("operation")
			require.Truef(t, ok, "message %d missing 'operation' metadata", i)
			assert.Equalf(t, operation, op, "message %d: expected operation '%s', got %q", i, operation, op)
		}

		for _, expectedKey := range []string{"TESTDB.FOO", "TESTDB.FOO2", "TESTDB2.BAR"} {
			assert.Containsf(t, results, expectedKey, "no messages received for table %q", expectedKey)
		}
	}

	// insert initial test data
	want := 3000
	for range 1000 {
		db.MustExec("INSERT INTO testdb.foo (val) VALUES (1)")
		db.MustExec("INSERT INTO testdb.foo2 (val) VALUES (1)")
		db.MustExec("INSERT INTO testdb2.bar (val) VALUES (1)")
	}

	t.Run("Streaming insert changes...", func(t *testing.T) {
		msgs := collectMessages(t, want)
		mustAssertMetadata(t, "insert", msgs)
	})

	t.Run("Streaming update changes...", func(t *testing.T) {
		db.MustExec("UPDATE testdb.foo SET val = 2")
		db.MustExec("UPDATE testdb.foo2 SET val = 2")
		db.MustExec("UPDATE testdb2.bar SET val = 2")

		msgs := collectMessages(t, want)
		mustAssertMetadata(t, "update", msgs)
	})

	t.Run("Streaming delete changes...", func(t *testing.T) {
		db.MustExec("DELETE FROM testdb.foo")
		db.MustExec("DELETE FROM testdb.foo2")
		db.MustExec("DELETE FROM testdb2.bar")

		msgs := collectMessages(t, want)
		mustAssertMetadata(t, "delete", msgs)
	})

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegrationOracleDBCDCLargeObjectColumnsToggle(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)

	sql := `CREATE TABLE testdb.lobdisabled (id NUMBER GENERATED ALWAYS AS IDENTITY (NOCACHE) PRIMARY KEY,varcharcol VARCHAR2(255),inlinelob NCLOB,outoflinelob NCLOB)`
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.lobdisabled", sql))
	sql = `CREATE TABLE testdb.lobenabled (id NUMBER GENERATED ALWAYS AS IDENTITY (NOCACHE) PRIMARY KEY,varcharcol VARCHAR2(255),inlinelob NCLOB,outoflinelob NCLOB)`
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.lobenabled", sql))

	var (
		inline       = strings.Repeat("A", 50)
		outofline    = strings.Repeat("B", 5000)
		snapshotRows = 50

		stream *service.Stream
		err    error
	)

	cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  logminer:
    lob_enabled: %s
  include: ["%s"]`

	t.Run("lob_enabled=false", func(t *testing.T) {
		for range snapshotRows {
			db.MustExec("INSERT INTO testdb.lobdisabled (varcharcol, inlinelob, outoflinelob) VALUES (:1, :2, :3)", "snapshot", inline, outofline)
		}

		var batch oracledbtest.Batch
		t.Logf("%s: Launching component...", t.Name())
		{
			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr, "false", "TESTDB.LOBDISABLED")))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: WARN`))

			require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				batch.Lock()
				defer batch.Unlock()
				for _, msg := range mb {
					msgBytes, err := msg.AsBytes()
					assert.NoError(t, err)
					batch.Msgs = append(batch.Msgs, string(msgBytes))
				}
				return nil
			}))

			stream, err = streamBuilder.Build()
			require.NoError(t, err)
			license.InjectTestService(stream.Resources())

			go func() {
				if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
		}

		t.Logf("%s: assert snapshot...", t.Name())
		{
			var got int
			assert.Eventually(t, func() bool {
				got = batch.Count()
				return got >= snapshotRows
			}, time.Minute*5, time.Second*1)

			require.Truef(t, (got == snapshotRows), "Wanted %d snapshot messages but got %d", snapshotRows, got)
			require.JSONEq(t, `{
		"ID": 1,
		"VARCHARCOL": "snapshot",
		"INLINELOB": null,
		"OUTOFLINELOB": null
		}`, batch.Clone()[0], "Failed to assert snapshot LOB columns")
		}

		batch.Reset()

		t.Logf("%s: assert streaming...", t.Name())
		{
			streamingRows := 50
			for range streamingRows {
				db.MustExec("INSERT INTO testdb.lobdisabled (varcharcol, inlinelob, outoflinelob) VALUES (:1, :2, :3)", "streaming", inline, outofline)
			}

			var got int
			assert.Eventually(t, func() bool {
				got = batch.Count()
				return got >= streamingRows
			}, time.Minute*5, time.Second*1)

			require.Truef(t, (got == streamingRows), "Wanted %d streaming messages but got %d", streamingRows, got)
			require.JSONEq(t, `{
		"ID": 51,
		"VARCHARCOL": "streaming",
		"INLINELOB": "",
		"OUTOFLINELOB": ""
		}`, batch.Clone()[0], "Failed to assert streaming LOB columns")
		}

		require.NoError(t, stream.StopWithin(time.Second*10))
	})

	db.MustExec(`TRUNCATE TABLE RPCN.CDC_CHECKPOINT_CACHE`)

	t.Run("lob_enabled=true", func(t *testing.T) {
		for range snapshotRows {
			db.MustExec("INSERT INTO testdb.lobenabled (varcharcol, inlinelob, outoflinelob) VALUES (:1, :2, :3)", "snapshot", inline, outofline)
		}

		var batch oracledbtest.Batch
		t.Logf("%s: Launching component...", t.Name())
		{
			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr, "true", "TESTDB.LOBENABLED")))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

			require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				batch.Lock()
				defer batch.Unlock()
				for _, msg := range mb {
					msgBytes, err := msg.AsBytes()
					assert.NoError(t, err)
					batch.Msgs = append(batch.Msgs, string(msgBytes))
				}
				return nil
			}))

			stream, err = streamBuilder.Build()
			require.NoError(t, err)
			license.InjectTestService(stream.Resources())

			go func() {
				if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
		}

		t.Logf("%s: assert snapshot...", t.Name())
		{
			var got int
			assert.Eventually(t, func() bool {
				got = batch.Count()
				return got >= snapshotRows
			}, time.Minute*5, time.Second*1)

			require.Truef(t, (got == snapshotRows), "Wanted %d snapshot messages but got %d", snapshotRows, got)
			require.JSONEq(t, `{
		"ID": 1,
		"VARCHARCOL": "snapshot",
		"INLINELOB": "`+inline+`",
		"OUTOFLINELOB": "`+outofline+`"
		}`, batch.Clone()[0], "Failed to snapshot LOB columns")
		}

		batch.Reset()

		t.Logf("%s: assert streaming...", t.Name())
		{
			streamingRows := 50
			for range streamingRows {
				db.MustExec("INSERT INTO testdb.lobenabled (varcharcol, inlinelob, outoflinelob) VALUES (:1, :2, :3)", "streaming", inline, outofline)
			}

			var got int
			assert.Eventually(t, func() bool {
				got = batch.Count()
				return got >= streamingRows
			}, time.Minute*5, time.Second*1)

			require.Truef(t, (got == streamingRows), "Wanted %d streaming messages but got %d", streamingRows, got)
			require.JSONEq(t, `{
		"ID": 51,
		"VARCHARCOL": "streaming",
		"INLINELOB": "`+inline+`",
		"OUTOFLINELOB": "`+outofline+`"
		}`, batch.Clone()[0], "Failed to assert streaming LOB columns")
		}
	})

	require.NoError(t, stream.StopWithin(time.Second*10))
}

func TestIntegrationOracleDBCDCSnapshotAndStreamingAllTypes(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
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
		oolvarcharmax_col CLOB, --out-of-line CLOB (LogMiner stores as a separate segement)
		nvarcharmax_col   NCLOB,
		varbinarymax_col  BLOB,

		-- Other Data Types
		bit_col           NUMBER(1),                    -- Boolean-like (0,1,NULL)
		-- xml_col           XMLTYPE,
		json_col          CLOB                          -- JSON stored as CLOB
	) LOB(oolvarcharmax_col) STORE AS BASICFILE (DISABLE STORAGE IN ROW NOCACHE LOGGING)`
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
		varcharmax_col, oolvarcharmax_col, nvarcharmax_col, varbinarymax_col,
		bit_col, json_col
	) VALUES (
		:1, :2, :3, :4,
		:5, :6, :7, :8,
		:9, :10, :11, :12,
		:13, :14, :15, :16,
		:17, :18, :19, :20,
		:21, :22, :23, :24,
		:25, :26
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
			nil,          // clob (varcharmax_col)
			nil,          // clob (oolvarcharmax_col)
			nil,          // nclob (nvarcharmax_col)
			nil,          // blob (varbinarymax_col)
			0,            // bit (number)
			nil,          // json (clob)
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
    lob_enabled: true
    scn_window_size: 20000
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
				assert.NoError(t, err)
				outBatches = append(outBatches, string(msgBytes))
			}
			return nil
		}))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
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

	largeClob := strings.Repeat("A", 5000)
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
			"Max varchar(max)",   // clob (varcharmax_col)
			largeClob,            // clob (oolvarcharmax_col)
			"Max nvarchar(max)",  // nclob (nvarcharmax_col)
			make([]byte, 255),    // blob (varbinarymax_col)
			1,                    // bit max (number)
			`{"max": true}`,      // json (clob)
		)

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
		"FLOAT_COL": -1.79e+100,
		"INT_COL": -2147483648,
		"JSON_COL": null,
		"NCHAR_COL": "АААААААААА",
		"NUMERIC_COL": -999999999999999.99999,
		"NVARCHAR_COL": null,
		"NVARCHARMAX_COL": null,
		"REAL_COL": -3.4e+37,
		"SMALLDATETIME_COL": "1900-01-01T00:00:00Z",
		"SMALLINT_COL": -32768,
		"TIME_COL": "0001-01-01T00:00:00Z",
		"TINYINT_COL": 0,
		"VARBINARY_COL": "AA==",
		"VARBINARYMAX_COL": null,
		"VARCHAR_COL": null,
		"OOLVARCHARMAX_COL": null,
		"VARCHARMAX_COL": null
		}`, outBatches[0], "Failed to assert min result from snapshot")
	}

	t.Log("Verifying values from streaming...")
	{
		// assert max - uppercase column names from Oracle
		require.JSONEq(t, `{
		"BIGINT_COL": 9223372036854775807,
		"BINARY_COL": "AAAAAAAAAAAAAAAAAAAAAA==",
		"BIT_COL": 1,
		"CHAR_COL": "ZZZZZZZZZZ",
		"DATE_COL": "9999-12-31T00:00:00Z",
		"DATETIME2_COL": "9999-12-31T23:59:59.9999999Z",
		"DATETIME_COL": "9999-12-31T23:59:59.997Z",
		"DATETIMEOFFSET_COL": "9999-12-31T23:59:59.9999999+14:00",
		"DECIMAL_COL": 9999999999999999999999999999.9999999999,
		"FLOAT_COL": 1.79e+100,
		"INT_COL": 2147483647,
		"JSON_COL": "{\"max\": true}",
		"NCHAR_COL": "ZZZZZZZZZZ",
		"NUMERIC_COL": 999999999999999.99999,
		"NVARCHAR_COL": "Max nvarchar value",
		"NVARCHARMAX_COL": "Max nvarchar(max)",
		"REAL_COL": 3.3999999e+37,
		"SMALLDATETIME_COL": "2079-06-06T23:59:00Z",
		"SMALLINT_COL": 32767,
		"TIME_COL": "0001-01-01T23:59:59.9999999Z",
		"TINYINT_COL": 255,
		"VARBINARY_COL": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"VARBINARYMAX_COL": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"VARCHAR_COL": "Max varchar value",
		"OOLVARCHARMAX_COL": "`+largeClob+`",
		"VARCHARMAX_COL": "Max varchar(max)"
		}`, outBatches[1], "Failed to assert max result from streaming")
	}
}

func TestIntegrationOracleDBCDCSnapshotSchema(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_snap",
		"CREATE TABLE testdb.schema_snap (id NUMBER(10) PRIMARY KEY, name VARCHAR2(100), created_at DATE, data RAW(16), score BINARY_FLOAT)"))

	db.MustExec("INSERT INTO testdb.schema_snap VALUES (1, 'Alice', SYSDATE, HEXTORAW('DEADBEEF'), 1.5)")
	db.MustExec("INSERT INTO testdb.schema_snap VALUES (2, 'Bob', SYSDATE, HEXTORAW('CAFEBABE'), 2.5)")

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_SNAP"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	// Collect 2 snapshot messages
	var msgs []*service.Message
	for msg := range msgChan {
		msgs = append(msgs, msg)
		if len(msgs) == 2 {
			break
		}
	}
	require.Len(t, msgs, 2)

	for i, msg := range msgs {
		s := oracledbtest.ExtractSchema(t, msg)
		assert.Equal(t, "SCHEMA_SNAP", s.Name, "msg %d", i)
		assert.Equal(t, schema.Object, s.Type, "msg %d", i)
		require.Len(t, s.Children, 5, "msg %d: expected 5 columns", i)

		id := oracledbtest.ChildByName(t, s, "ID")
		assert.Equal(t, schema.Int64, id.Type, "NUMBER(10) with scale=0 should be Int64")
		assert.True(t, id.Optional)

		name := oracledbtest.ChildByName(t, s, "NAME")
		assert.Equal(t, schema.String, name.Type)

		createdAt := oracledbtest.ChildByName(t, s, "CREATED_AT")
		assert.Equal(t, schema.Timestamp, createdAt.Type)

		data := oracledbtest.ChildByName(t, s, "DATA")
		assert.Equal(t, schema.ByteArray, data.Type)

		score := oracledbtest.ChildByName(t, s, "SCORE")
		assert.Equal(t, schema.Float32, score.Type)

		fp := oracledbtest.ExtractFingerprint(t, msg)
		assert.NotEmpty(t, fp, "msg %d: fingerprint should be present", i)
	}

	// Both snapshot messages should have the same fingerprint
	fp0 := oracledbtest.ExtractFingerprint(t, msgs[0])
	fp1 := oracledbtest.ExtractFingerprint(t, msgs[1])
	assert.Equal(t, fp0, fp1, "snapshot messages should have identical fingerprints")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCStreamingInsertSchema(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_ins",
		"CREATE TABLE testdb.schema_ins (id NUMBER(10) PRIMARY KEY, val VARCHAR2(50))"))

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_INS"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	time.Sleep(10 * time.Second)

	db.MustExec("INSERT INTO testdb.schema_ins VALUES (1, 'hello')")
	db.MustExec("INSERT INTO testdb.schema_ins VALUES (2, 'world')")

	var msgs []*service.Message
	for msg := range msgChan {
		msgs = append(msgs, msg)
		if len(msgs) == 2 {
			break
		}
	}
	require.Len(t, msgs, 2)

	for i, msg := range msgs {
		s := oracledbtest.ExtractSchema(t, msg)
		assert.Equal(t, "SCHEMA_INS", s.Name, "msg %d", i)
		require.Len(t, s.Children, 2, "msg %d", i)
	}

	// Fingerprint should be stable across inserts to the same table
	assert.Equal(t, oracledbtest.ExtractFingerprint(t, msgs[0]), oracledbtest.ExtractFingerprint(t, msgs[1]))

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCStreamingUpdateSchema(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_upd",
		"CREATE TABLE testdb.schema_upd (id NUMBER(10) PRIMARY KEY, a VARCHAR2(50), b VARCHAR2(50), c VARCHAR2(50))"))

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_UPD"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	time.Sleep(10 * time.Second)

	// INSERT a row (all columns), then UPDATE only column B
	db.MustExec("INSERT INTO testdb.schema_upd VALUES (1, 'x', 'y', 'z')")
	db.MustExec("UPDATE testdb.schema_upd SET b = 'updated' WHERE id = 1")

	var msgs []*service.Message
	for msg := range msgChan {
		msgs = append(msgs, msg)
		if len(msgs) == 2 {
			break
		}
	}
	require.Len(t, msgs, 2)

	// Both INSERT and UPDATE should carry the same full table schema
	insertSchema := oracledbtest.ExtractSchema(t, msgs[0])
	updateSchema := oracledbtest.ExtractSchema(t, msgs[1])

	assert.Equal(t, "SCHEMA_UPD", insertSchema.Name)
	assert.Equal(t, "SCHEMA_UPD", updateSchema.Name)
	require.Len(t, insertSchema.Children, 4, "full table schema should have 4 columns")
	require.Len(t, updateSchema.Children, 4, "UPDATE should carry full table schema, not just SET columns")

	assert.Equal(t, oracledbtest.ExtractFingerprint(t, msgs[0]), oracledbtest.ExtractFingerprint(t, msgs[1]),
		"INSERT and UPDATE on same table should have identical schema fingerprints")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCStreamingDeleteSchema(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_del",
		"CREATE TABLE testdb.schema_del (id NUMBER(10) PRIMARY KEY, val VARCHAR2(50))"))

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_DEL"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	time.Sleep(10 * time.Second)

	db.MustExec("INSERT INTO testdb.schema_del VALUES (1, 'doomed')")
	db.MustExec("DELETE FROM testdb.schema_del WHERE id = 1")

	var msgs []*service.Message
	for msg := range msgChan {
		msgs = append(msgs, msg)
		if len(msgs) == 2 {
			break
		}
	}
	require.Len(t, msgs, 2)

	insertSchema := oracledbtest.ExtractSchema(t, msgs[0])
	deleteSchema := oracledbtest.ExtractSchema(t, msgs[1])

	assert.Equal(t, "SCHEMA_DEL", insertSchema.Name)
	assert.Equal(t, "SCHEMA_DEL", deleteSchema.Name)
	require.Len(t, deleteSchema.Children, 2, "DELETE should carry full table schema")

	assert.Equal(t, oracledbtest.ExtractFingerprint(t, msgs[0]), oracledbtest.ExtractFingerprint(t, msgs[1]),
		"INSERT and DELETE on same table should have identical schema fingerprints")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCSchemaConsistentAcrossPhases(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_phases",
		"CREATE TABLE testdb.schema_phases (id NUMBER(10) PRIMARY KEY, val VARCHAR2(50))"))

	db.MustExec("INSERT INTO testdb.schema_phases VALUES (1, 'snapshot')")

	var (
		outMsgs   []*service.Message
		outMsgsMu sync.Mutex
	)

	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_PHASES"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		for _, msg := range mb {
			outMsgs = append(outMsgs, msg)
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	// Wait for snapshot
	assert.Eventually(t, func() bool {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		return len(outMsgs) >= 1
	}, 2*time.Minute, time.Second)

	outMsgsMu.Lock()
	snapshotMsg := outMsgs[0]
	outMsgs = nil
	outMsgsMu.Unlock()

	// Now insert via streaming
	db.MustExec("INSERT INTO testdb.schema_phases VALUES (2, 'streaming')")

	assert.Eventually(t, func() bool {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		return len(outMsgs) >= 1
	}, 2*time.Minute, time.Second)

	outMsgsMu.Lock()
	streamingMsg := outMsgs[0]
	outMsgsMu.Unlock()

	snapshotFP := oracledbtest.ExtractFingerprint(t, snapshotMsg)
	streamingFP := oracledbtest.ExtractFingerprint(t, streamingMsg)

	assert.NotEmpty(t, snapshotFP)
	assert.NotEmpty(t, streamingFP)
	assert.Equal(t, snapshotFP, streamingFP,
		"snapshot and streaming phases should produce identical schema fingerprints for the same table")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCSchemaColumnAdded(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_drift",
		"CREATE TABLE testdb.schema_drift (id NUMBER(10) PRIMARY KEY, name VARCHAR2(100))"))

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_DRIFT"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	time.Sleep(10 * time.Second)

	// INSERT before ALTER — schema has [ID, NAME]
	db.MustExec("INSERT INTO testdb.schema_drift VALUES (1, 'before')")

	msg1 := <-msgChan
	require.NotNil(t, msg1)
	fp1 := oracledbtest.ExtractFingerprint(t, msg1)
	s1 := oracledbtest.ExtractSchema(t, msg1)
	require.Len(t, s1.Children, 2)

	// ALTER TABLE to add a column, then drop and re-enable supplemental logging
	// to cover the new column (ORA-32588 if we just re-add without dropping first)
	db.MustExec("ALTER TABLE testdb.schema_drift ADD (email VARCHAR2(255))")
	db.MustDisableSupplementalLogging(t.Context(), "testdb.schema_drift")
	db.MustEnableSupplementalLogging(t.Context(), "testdb.schema_drift")

	// INSERT with new column — schema should now have [ID, NAME, EMAIL]
	db.MustExec("INSERT INTO testdb.schema_drift VALUES (2, 'after', 'test@example.com')")

	msg2 := <-msgChan
	require.NotNil(t, msg2)
	fp2 := oracledbtest.ExtractFingerprint(t, msg2)
	s2 := oracledbtest.ExtractSchema(t, msg2)

	require.Len(t, s2.Children, 3, "schema should include the new EMAIL column")
	email := oracledbtest.ChildByName(t, s2, "EMAIL")
	assert.Equal(t, schema.String, email.Type)

	assert.NotEqual(t, fp1, fp2, "fingerprint should change after column addition")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCMultiTableSchema(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_t1",
		"CREATE TABLE testdb.schema_t1 (id NUMBER(10) PRIMARY KEY, val VARCHAR2(50))"))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_t2",
		"CREATE TABLE testdb.schema_t2 (x DATE, y RAW(16), z BINARY_FLOAT)"))

	msgChan := make(chan *service.Message, 10)
	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: false
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_T1", "TESTDB.SCHEMA_T2"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgChan <- msg
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	go func() { <-t.Context().Done(); close(msgChan) }()

	time.Sleep(10 * time.Second)

	db.MustExec("INSERT INTO testdb.schema_t1 VALUES (1, 'hello')")
	db.MustExec("INSERT INTO testdb.schema_t2 VALUES (SYSDATE, HEXTORAW('DEADBEEFCAFEBABE0000000000000000'), 1.5)")

	// Collect 2 messages (one from each table)
	byTable := map[string]*service.Message{}
	for msg := range msgChan {
		table, _ := msg.MetaGet("table_name")
		byTable[table] = msg
		if len(byTable) == 2 {
			break
		}
	}
	require.Len(t, byTable, 2)

	s1 := oracledbtest.ExtractSchema(t, byTable["SCHEMA_T1"])
	s2 := oracledbtest.ExtractSchema(t, byTable["SCHEMA_T2"])

	assert.Equal(t, "SCHEMA_T1", s1.Name)
	require.Len(t, s1.Children, 2)

	assert.Equal(t, "SCHEMA_T2", s2.Name)
	require.Len(t, s2.Children, 3)

	fp1 := oracledbtest.ExtractFingerprint(t, byTable["SCHEMA_T1"])
	fp2 := oracledbtest.ExtractFingerprint(t, byTable["SCHEMA_T2"])
	assert.NotEqual(t, fp1, fp2, "different tables should have different fingerprints")

	require.NoError(t, stream.StopWithin(10*time.Second))
}

func TestIntegrationOracleDBCDCSchemaDataTypeConsistency(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.schema_types",
		`CREATE TABLE testdb.schema_types (
			int_col       NUMBER(10)      PRIMARY KEY,
			bigint_col    NUMBER(18),
			decimal_col   NUMBER(20, 5),
			float_col     BINARY_FLOAT,
			double_col    BINARY_DOUBLE,
			date_col      DATE,
			ts_col        TIMESTAMP,
			tstz_col      TIMESTAMP WITH TIME ZONE,
			char_col      CHAR(10),
			varchar_col   VARCHAR2(100),
			raw_col       RAW(16),
			bit_col       NUMBER(1)
		)`))

	// Disable supplemental logging before snapshot insert
	db.MustDisableSupplementalLogging(t.Context(), "testdb.schema_types")

	// Insert row for snapshot
	db.MustExecContext(t.Context(), `INSERT INTO testdb.schema_types VALUES (
		1, 999999999999999999, 12345.67890,
		1.5, 2.5,
		TO_DATE('2020-06-15','YYYY-MM-DD'),
		TO_TIMESTAMP('2020-06-15 10:30:00','YYYY-MM-DD HH24:MI:SS'),
		TO_TIMESTAMP_TZ('2020-06-15 10:30:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'),
		'AAAAAAAAAA', 'hello',
		HEXTORAW('DEADBEEFCAFEBABE0000000000000000'),
		1
	)`)

	db.MustEnableSupplementalLogging(t.Context(), "testdb.schema_types")

	var (
		outMsgs   []*service.Message
		outMsgsMu sync.Mutex
	)

	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    backoff_interval: 1s
  include: ["TESTDB.SCHEMA_TYPES"]`, connStr)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		for _, msg := range mb {
			outMsgs = append(outMsgs, msg)
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	// Wait for snapshot message
	t.Log("Waiting for snapshot...")
	assert.Eventually(t, func() bool {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		return len(outMsgs) >= 1
	}, 2*time.Minute, time.Second)

	outMsgsMu.Lock()
	snapshotMsg := outMsgs[0]
	outMsgs = nil
	outMsgsMu.Unlock()

	// Insert same row via DML for streaming
	t.Log("Inserting streaming row...")
	db.MustExecContext(t.Context(), `INSERT INTO testdb.schema_types VALUES (
		2, 999999999999999999, 12345.67890,
		1.5, 2.5,
		TO_DATE('2020-06-15','YYYY-MM-DD'),
		TO_TIMESTAMP('2020-06-15 10:30:00','YYYY-MM-DD HH24:MI:SS'),
		TO_TIMESTAMP_TZ('2020-06-15 10:30:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'),
		'AAAAAAAAAA', 'hello',
		HEXTORAW('DEADBEEFCAFEBABE0000000000000000'),
		1
	)`)

	t.Log("Waiting for streaming message...")
	assert.Eventually(t, func() bool {
		outMsgsMu.Lock()
		defer outMsgsMu.Unlock()
		return len(outMsgs) >= 1
	}, 2*time.Minute, time.Second)

	outMsgsMu.Lock()
	streamingMsg := outMsgs[0]
	outMsgsMu.Unlock()

	// Define expected CommonType per column
	expectedTypes := map[string]schema.CommonType{
		"INT_COL":     schema.Int64,
		"BIGINT_COL":  schema.Int64,
		"DECIMAL_COL": schema.String,
		"FLOAT_COL":   schema.Float32,
		"DOUBLE_COL":  schema.Float64,
		"DATE_COL":    schema.Timestamp,
		"TS_COL":      schema.Timestamp,
		"TSTZ_COL":    schema.Timestamp,
		"CHAR_COL":    schema.String,
		"VARCHAR_COL": schema.String,
		"RAW_COL":     schema.ByteArray,
		"BIT_COL":     schema.Int64,
	}

	// Verify schema metadata for both phases
	for phase, msg := range map[string]*service.Message{"snapshot": snapshotMsg, "streaming": streamingMsg} {
		s := oracledbtest.ExtractSchema(t, msg)
		assert.Equal(t, "SCHEMA_TYPES", s.Name, "%s schema name", phase)
		require.Len(t, s.Children, len(expectedTypes), "%s schema child count", phase)

		for colName, wantType := range expectedTypes {
			child := oracledbtest.ChildByName(t, s, colName)
			assert.Equal(t, wantType, child.Type, "%s: column %s type", phase, colName)
			assert.True(t, child.Optional, "%s: column %s should be optional", phase, colName)
		}
	}

	// Verify fingerprints match across phases
	assert.Equal(t, oracledbtest.ExtractFingerprint(t, snapshotMsg), oracledbtest.ExtractFingerprint(t, streamingMsg),
		"schema fingerprints should be identical across snapshot and streaming")

	// Verify data value types are consistent across phases.
	// With streaming value coercion, both snapshot and streaming should produce
	// the same Go types after JSON round-trip.
	snapshotData := make(map[string]any)
	streamingData := make(map[string]any)

	snapshotBytes, err := snapshotMsg.AsBytes()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(snapshotBytes, &snapshotData))

	streamingBytes, err := streamingMsg.AsBytes()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(streamingBytes, &streamingData))

	for colName := range expectedTypes {
		snapVal, snapOK := snapshotData[colName]
		streamVal, streamOK := streamingData[colName]

		if !snapOK || !streamOK {
			if !snapOK && !streamOK {
				continue
			}
			t.Errorf("column %s: present in snapshot=%v, streaming=%v", colName, snapOK, streamOK)
			continue
		}

		t.Logf("column %s: snapshot type=%T val=%v, streaming type=%T val=%v", colName, snapVal, snapVal, streamVal, streamVal)

		assert.IsTypef(t, snapVal, streamVal,
			"column %s: snapshot Go type %T != streaming Go type %T", colName, snapVal, streamVal)
	}

	require.NoError(t, stream.StopWithin(10*time.Second))
}
