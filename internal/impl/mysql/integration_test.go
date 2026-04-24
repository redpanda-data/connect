// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/license"
)

type testDB struct {
	*sql.DB

	t *testing.T
}

func (db *testDB) Exec(query string, args ...any) {
	_, err := db.DB.Exec(query, args...)
	require.NoError(db.t, err)
}

func setupTestWithMySQLVersion(t *testing.T, version string) (string, *testDB) {
	t.Parallel()
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mysql:"+version,
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MYSQL_ROOT_PASSWORD": "password",
			"MYSQL_DATABASE":      "testdb",
			"TZ":                  "UTC",
		}),
		testcontainers.WithCmd(
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
			"--log-slave-updates=ON",
			"--default-time-zone=+00:00",
		),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "3306/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	dsn := fmt.Sprintf(
		"root:password@tcp(localhost:%s)/testdb?timeout=30s&readTimeout=30s&writeTimeout=30s&multiStatements=true",
		port,
	)

	var db *sql.DB
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return false
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if db.Ping() != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, time.Minute, time.Second)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return dsn, &testDB{db, t}
}

func TestIntegrationMySQLCDC(t *testing.T) {
	integration.CheckSkip(t)
	mysqlTestVersions := []string{"8.0", "9.0", "9.1"}
	for _, version := range mysqlTestVersions {
		t.Run(version, func(t *testing.T) {
			dsn, db := setupTestWithMySQLVersion(t, version)
			// Create table
			db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT PRIMARY KEY
    )
`)
			template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: false
  checkpoint_cache: foocache
  tables:
    - foo
`, dsn)

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

			time.Sleep(time.Second * 5)
			for i := range 1000 {
				// Insert 10000 rows
				db.Exec("INSERT INTO foo VALUES (?)", i)
			}

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1000
			}, time.Minute*5, time.Millisecond*100)

			require.NoError(t, streamOut.StopWithin(time.Second*10))

			streamOutBuilder = service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
			require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			outBatches = nil
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				msgBytes, err := mb[0].AsBytes()
				require.NoError(t, err)
				outBatchMut.Lock()
				outBatches = append(outBatches, string(msgBytes))
				outBatchMut.Unlock()
				return nil
			}))

			streamOut, err = streamOutBuilder.Build()
			require.NoError(t, err)
			license.InjectTestService(streamOut.Resources())

			time.Sleep(time.Second)
			for i := 1001; i < 2001; i++ {
				db.Exec("INSERT INTO foo VALUES (?)", i)
			}

			go func() {
				err = streamOut.Run(t.Context())
				require.NoError(t, err)
			}()

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1000
			}, time.Minute*5, time.Millisecond*100)

			require.NoError(t, streamOut.StopWithin(time.Second*10))
		})
	}
}

func TestIntegrationMySQLSnapshotAndCDC(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")
	// Create table
	db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT PRIMARY KEY
    )
`)
	// Insert 1000 rows for initial snapshot streaming
	for i := range 1000 {
		db.Exec("INSERT INTO foo VALUES (?)", i)
	}

	template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 500
  checkpoint_cache: foocache
  tables:
    - foo
`, dsn)

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
		// Insert 10000 rows
		db.Exec("INSERT INTO foo VALUES (?)", i)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationMySQLCDCWithCompositePrimaryKeys(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")
	// Create table
	db.Exec(`
    CREATE TABLE IF NOT EXISTS ` + "`Foo`" + ` (
    ` + "`A`" + ` INT,
    ` + "`B`" + ` INT,
      PRIMARY KEY (
      ` + "`A`" + `,
      ` + "`B`" + `
      )
    )
`)
	// Create control table to ensure we don't stream it
	db.Exec(`
    CREATE TABLE IF NOT EXISTS foo_non_streamed (
        a INT,
        b INT,
        PRIMARY KEY (a, b)
    )
`)

	// Insert 1000 rows for initial snapshot streaming
	for i := range 1000 {
		db.Exec("INSERT INTO `Foo` VALUES (?, ?)", i, i)
		db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?)", i, i)
	}

	template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 500
  checkpoint_cache: foocache
  tables:
    - Foo
`, dsn)

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
		// Insert 10000 rows
		db.Exec("INSERT INTO `Foo` VALUES (?, ?)", i, i)
		db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?)", i, i)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)
	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationMySQLCDCAllTypes(t *testing.T) {
	os.Setenv("TZ", "UTC") //nolint:usetesting // t.Setenv panics with t.Parallel in setupTestWithMySQLVersion
	time.Local = time.UTC
	dsn, db := setupTestWithMySQLVersion(t, "8.0")
	// Create table
	db.Exec(`
    CREATE TABLE all_data_types (
    -- Numeric Data Types
    tinyint_col TINYINT PRIMARY KEY,
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    int_col INT,
    bigint_col BIGINT,
    decimal_col DECIMAL(38, 2),
    numeric_col NUMERIC(10, 2),
    float_col FLOAT,
    double_col DOUBLE,

    -- Date and Time Data Types
    date_col DATE,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP,
    time_col TIME,
    year_col YEAR,

    -- String Data Types
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    binary_col BINARY(10),
    varbinary_col VARBINARY(255),
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    enum_col ENUM('option1', 'option2', 'option3'),
    set_col SET('a', 'b', 'c', 'd'),
    json_col JSON

    -- TODO(cdc): Spatial Data Types
    -- geometry_col GEOMETRY,
    -- point_col POINT,
    -- linestring_col LINESTRING,
    -- polygon_col POLYGON,
    -- multipoint_col MULTIPOINT,
    -- multilinestring_col MULTILINESTRING,
    -- multipolygon_col MULTIPOLYGON,
    -- geometrycollection_col GEOMETRYCOLLECTION
);
`)

	db.Exec(`
INSERT INTO all_data_types (
    tinyint_col,
    smallint_col,
    mediumint_col,
    int_col,
    bigint_col,
    decimal_col,
    numeric_col,
    float_col,
    double_col,
    date_col,
    datetime_col,
    timestamp_col,
    time_col,
    year_col,
    char_col,
    varchar_col,
    binary_col,
    varbinary_col,
    tinyblob_col,
    blob_col,
    mediumblob_col,
    longblob_col,
    tinytext_col,
    text_col,
    mediumtext_col,
    longtext_col,
    enum_col,
    set_col,
    json_col
) VALUES (
    127,                    -- tinyint_col
    32767,                  -- smallint_col
    8388607,                -- mediumint_col
    2147483647,             -- int_col
    9223372036854775807,    -- bigint_col
    999999999999999999999999999999999999.99, -- decimal_col
    98765.43,               -- numeric_col
    3.14,                   -- float_col
    2.718281828,            -- double_col
    '2024-12-10',           -- date_col
    '2024-12-10 15:30:45',  -- datetime_col
    '2024-12-10 15:30:46',  -- timestamp_col
    '15:30:45',             -- time_col
    2024,                   -- year_col
    'char_data',            -- char_col
    'varchar_data',         -- varchar_col
    BINARY('binary'),       -- binary_col
    BINARY('varbinary'),    -- varbinary_col
    'small blob',           -- tinyblob_col
    'regular blob',         -- blob_col
    'medium blob',          -- mediumblob_col
    'large blob',           -- longblob_col
    'tiny text',            -- tinytext_col
    'regular text',         -- text_col
    'medium text',          -- mediumtext_col
    'large text',           -- longtext_col
    'option1',              -- enum_col
    'a,b',                  -- set_col
    '{"foo":5,"bar":[1,2,3]}' -- json_col
);

    `)

	template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 500
  checkpoint_cache: memcache
  tables:
    - all_data_types
`, dsn)

	cacheConf := `
label: memcache
memory: {}
`

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

	db.Exec(`
    INSERT INTO all_data_types (
    tinyint_col,
    smallint_col,
    mediumint_col,
    int_col,
    bigint_col,
    decimal_col,
    numeric_col,
    float_col,
    double_col,
    date_col,
    datetime_col,
    timestamp_col,
    time_col,
    year_col,
    char_col,
    varchar_col,
    binary_col,
    varbinary_col,
    tinyblob_col,
    blob_col,
    mediumblob_col,
    longblob_col,
    tinytext_col,
    text_col,
    mediumtext_col,
    longtext_col,
    enum_col,
    set_col,
    json_col
) VALUES (
    -128,                   -- tinyint_col
    -32768,                 -- smallint_col
    -8388608,               -- mediumint_col
    -2147483648,            -- int_col
    -9223372036854775808,   -- bigint_col
    888888888888888888888888888888888888.88, -- decimal_col
    87654.21,               -- numeric_col
    1.618,                  -- float_col
    3.141592653,            -- double_col
    '2023-01-01',           -- date_col
    '2023-01-01 12:00:00',  -- datetime_col
    '2023-01-01 12:00:00',  -- timestamp_col
    '23:59:59',             -- time_col
    2023,                   -- year_col
    'example',              -- char_col
    'another_example',      -- varchar_col
    BINARY('fixed'),        -- binary_col
    BINARY('dynamic'),      -- varbinary_col
    'tiny_blob_value',      -- tinyblob_col
    'blob_value',           -- blob_col
    'medium_blob_value',    -- mediumblob_col
    'long_blob_value',      -- longblob_col
    'tiny_text_value',      -- tinytext_col
    'text_value',           -- text_col
    'medium_text_value',    -- mediumtext_col
    'long_text_value',      -- longtext_col
    'option2',              -- enum_col
    'b,c',                   -- set_col
    '{"foo":-1,"bar":[3,2,1]}' -- json_col
);`)

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2
	}, time.Second*30, time.Millisecond*100)
	require.NoError(t, streamOut.StopWithin(time.Second*10))

	require.JSONEq(t, `{
  "tinyint_col": 127,
  "smallint_col": 32767,
  "mediumint_col": 8388607,
  "int_col": 2147483647,
  "bigint_col": 9223372036854775807,
  "decimal_col": "999999999999999999999999999999999999.99",
  "numeric_col": "98765.43",
  "float_col": 3.14,
  "double_col": 2.718281828,
  "date_col": "2024-12-10T00:00:00Z",
  "datetime_col": "2024-12-10T15:30:45Z",
  "timestamp_col": "2024-12-10T15:30:46Z",
  "time_col": "15:30:45",
  "year_col": 2024,
  "char_col": "char_data",
  "varchar_col": "varchar_data",
  "binary_col": "YmluYXJ5AAAAAA==",
  "varbinary_col": "dmFyYmluYXJ5",
  "tinyblob_col": "c21hbGwgYmxvYg==",
  "blob_col": "cmVndWxhciBibG9i",
  "mediumblob_col": "bWVkaXVtIGJsb2I=",
  "longblob_col": "bGFyZ2UgYmxvYg==",
  "tinytext_col": "tiny text",
  "text_col": "regular text",
  "mediumtext_col": "medium text",
  "longtext_col": "large text",
  "enum_col": "option1",
  "set_col": ["a", "b"],
  "json_col": {"foo":5, "bar":[1, 2, 3]}
}`, outBatches[0])
	require.JSONEq(t, `{
  "tinyint_col": -128,
  "smallint_col": -32768,
  "mediumint_col": -8388608,
  "int_col": -2147483648,
  "bigint_col": -9223372036854775808,
  "decimal_col": "888888888888888888888888888888888888.88",
  "numeric_col": "87654.21",
  "float_col": 1.618,
  "double_col": 3.141592653,
  "date_col": "2023-01-01T00:00:00Z",
  "datetime_col": "2023-01-01T12:00:00Z",
  "timestamp_col": "2023-01-01T12:00:00Z",
  "time_col": "23:59:59",
  "year_col": 2023,
  "char_col": "example",
  "varchar_col": "another_example",
  "binary_col": "Zml4ZWQ=",
  "varbinary_col": "ZHluYW1pYw==",
  "tinyblob_col": "dGlueV9ibG9iX3ZhbHVl",
  "blob_col": "YmxvYl92YWx1ZQ==",
  "mediumblob_col": "bWVkaXVtX2Jsb2JfdmFsdWU=",
  "longblob_col": "bG9uZ19ibG9iX3ZhbHVl",
  "tinytext_col": "tiny_text_value",
  "text_col": "text_value",
  "mediumtext_col": "medium_text_value",
  "longtext_col": "long_text_value",
  "enum_col": "option2",
  "set_col": ["b", "c"],
  "json_col": {"foo":-1,"bar":[3,2,1]}
}`, outBatches[1])
}

func TestIntegrationMySQLSnapshotConsistency(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")
	db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT AUTO_INCREMENT,
        PRIMARY KEY (a)
    )
`)

	template := strings.NewReplacer("$DSN", dsn).Replace(`
read_until:
  # Stop when we're idle for 3 seconds, which means our writer stopped
  idle_timeout: 3s
  input:
    mysql_cdc:
      dsn: $DSN
      stream_snapshot: true
      snapshot_max_batch_size: 500
      checkpoint_cache: foocache
      tables:
        - foo
`)

	cacheConf := `
label: foocache
file:
  directory: ` + t.TempDir()

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var ids []int64
	var batchMu sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		batchMu.Lock()
		defer batchMu.Unlock()
		for _, msg := range batch {
			data, err := msg.AsStructured()
			require.NoError(t, err)
			v, err := bloblang.ValueAsInt64(data.(map[string]any)["a"])
			require.NoError(t, err)
			ids = append(ids, v)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	// Continuously write so there is a chance we skip data between snapshot and stream hand off.
	var count atomic.Int64
	writer := asyncroutine.NewPeriodic(time.Microsecond, func() {
		db.Exec("INSERT INTO foo (a) VALUES (DEFAULT)")
		count.Add(1)
	})
	writer.Start()
	t.Cleanup(writer.Stop)

	// Wait to write some values so there are some values in the snapshot
	time.Sleep(time.Second)

	streamStopped := make(chan any, 1)
	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
		streamStopped <- nil
	}()

	// Let the writer write a little more
	time.Sleep(time.Second * 3)

	writer.Stop()

	// Okay now wait for the stream to finish (the stream auto closes after it gets nothing for 3 seconds)
	select {
	case <-streamStopped:
	case <-time.After(30 * time.Second):
		require.Fail(t, "stream did not complete in time")
	}
	require.NoError(t, streamOut.StopWithin(time.Second*10))
	expected := []int64{}
	for i := range count.Load() {
		expected = append(expected, i+1)
	}
	batchMu.Lock()
	require.Equal(t, expected, ids)
	batchMu.Unlock()
}

func TestIntegrationMySQLCDCSchemaMetadata(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")

	// Create a table with various data types to test schema metadata
	db.Exec(`
		CREATE TABLE IF NOT EXISTS test_schema (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			created_at TIMESTAMP,
			score FLOAT,
			data JSON,
			tags SET('tag1', 'tag2', 'tag3')
		)
	`)

	// Insert snapshot rows
	db.Exec("INSERT INTO test_schema VALUES (1, 'snapshot1', '2024-01-01 12:00:00', 95.5, '{\"key\":\"value1\"}', 'tag1')")
	db.Exec("INSERT INTO test_schema VALUES (2, 'snapshot2', '2024-01-02 12:00:00', 87.3, '{\"key\":\"value2\"}', 'tag1,tag2')")

	template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 100
  checkpoint_cache: schemacache
  tables:
    - test_schema
`, dsn)

	cacheConf := fmt.Sprintf(`
label: schemacache
file:
  directory: %s`, t.TempDir())

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	type messageMetadata struct {
		operation      string
		table          string
		binlogPosition string
		hasSchema      bool
		schema         map[string]any
		data           map[string]any
	}

	var messages []messageMetadata
	var msgMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgMut.Lock()

			operation, _ := msg.MetaGet("operation")
			table, _ := msg.MetaGet("table")
			binlogPosition, _ := msg.MetaGet("binlog_position")

			// Try to get schema metadata - mutable metadata is stored separately
			var schema map[string]any
			hasSchema := false
			err := msg.MetaWalkMut(func(key string, value any) error {
				if key == "schema" {
					hasSchema = true
					if schemaMap, ok := value.(map[string]any); ok {
						schema = schemaMap
					}
				}
				return nil
			})
			require.NoError(t, err)

			data, err := msg.AsStructured()
			require.NoError(t, err)

			messages = append(messages, messageMetadata{
				operation:      operation,
				table:          table,
				binlogPosition: binlogPosition,
				hasSchema:      hasSchema,
				schema:         schema,
				data:           data.(map[string]any),
			})

			msgMut.Unlock()
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	// Wait for stream to start and read snapshot
	time.Sleep(time.Second * 3)

	// Insert CDC rows
	db.Exec("INSERT INTO test_schema VALUES (3, 'cdc1', '2024-01-03 12:00:00', 92.1, '{\"key\":\"value3\"}', 'tag2')")
	db.Exec("INSERT INTO test_schema VALUES (4, 'cdc2', '2024-01-04 12:00:00', 88.7, '{\"key\":\"value4\"}', 'tag2,tag3')")

	// Wait for CDC events
	assert.Eventually(t, func() bool {
		msgMut.Lock()
		defer msgMut.Unlock()
		return len(messages) == 4
	}, time.Minute, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Verify messages
	msgMut.Lock()
	defer msgMut.Unlock()

	require.Len(t, messages, 4, "should have 4 messages total (2 snapshot + 2 CDC)")

	// Check snapshot messages (first 2)
	for i := range 2 {
		msg := messages[i]
		assert.Equal(t, "read", msg.operation, "snapshot message should have operation=read")
		assert.Equal(t, "test_schema", msg.table, "message should have correct table name")
		assert.Empty(t, msg.binlogPosition, "snapshot message should not have binlog_position")

		// Snapshot messages MUST have schema metadata
		require.True(t, msg.hasSchema, "snapshot message must have schema metadata")
		require.NotNil(t, msg.schema, "snapshot message schema must not be nil")
		validateSchemaStructure(t, msg.schema)

		// Verify specific field schemas match CDC schema
		children, ok := msg.schema["children"].([]any)
		require.True(t, ok, "schema should have children array")
		fieldSchemas := make(map[string]map[string]any)
		for _, child := range children {
			childMap := child.(map[string]any)
			fieldSchemas[childMap["name"].(string)] = childMap
		}
		for _, fieldName := range []string{"id", "name", "created_at", "score", "data", "tags"} {
			_, exists := fieldSchemas[fieldName]
			assert.True(t, exists, "snapshot schema should contain field %s", fieldName)
		}
		assert.Equal(t, "INT32", fieldSchemas["id"]["type"])
		assert.Equal(t, "STRING", fieldSchemas["name"]["type"])
		assert.Equal(t, "TIMESTAMP", fieldSchemas["created_at"]["type"])
		assert.Equal(t, "FLOAT32", fieldSchemas["score"]["type"])
		assert.Equal(t, "ANY", fieldSchemas["data"]["type"])
		assert.Equal(t, "ARRAY", fieldSchemas["tags"]["type"])
	}

	// Check CDC messages (last 2)
	for i := range 2 {
		msg := messages[i+2]
		assert.Equal(t, "insert", msg.operation, "CDC message should have operation=insert")
		assert.Equal(t, "test_schema", msg.table, "message should have correct table name")
		assert.NotEmpty(t, msg.binlogPosition, "CDC message should have binlog_position")

		// CDC messages MUST have schema metadata
		require.True(t, msg.hasSchema, "CDC message must have schema metadata")
		require.NotNil(t, msg.schema, "CDC message schema must not be nil")

		// Validate schema structure
		validateSchemaStructure(t, msg.schema)

		// Verify specific field schemas
		children, ok := msg.schema["children"].([]any)
		require.True(t, ok, "schema should have children array")
		require.NotEmpty(t, children, "schema children should not be empty")

		// Build a map of field names to field schemas for easier validation
		fieldSchemas := make(map[string]map[string]any)
		for _, child := range children {
			childMap := child.(map[string]any)
			fieldName := childMap["name"].(string)
			fieldSchemas[fieldName] = childMap
		}

		// Verify expected fields exist in schema
		expectedFields := []string{"id", "name", "created_at", "score", "data", "tags"}
		for _, fieldName := range expectedFields {
			_, exists := fieldSchemas[fieldName]
			assert.True(t, exists, "schema should contain field %s", fieldName)
		}

		// Verify field types (uppercase)
		assert.Equal(t, "INT32", fieldSchemas["id"]["type"], "id should be INT32")
		assert.Equal(t, "STRING", fieldSchemas["name"]["type"], "name should be STRING")
		assert.Equal(t, "TIMESTAMP", fieldSchemas["created_at"]["type"], "created_at should be TIMESTAMP")
		assert.Equal(t, "FLOAT32", fieldSchemas["score"]["type"], "score should be FLOAT32")
		assert.Equal(t, "ANY", fieldSchemas["data"]["type"], "json field should be ANY in schema")
		assert.Equal(t, "ARRAY", fieldSchemas["tags"]["type"], "set field should be ARRAY")

		// Verify array element type for tags
		tagsChildren, ok := fieldSchemas["tags"]["children"].([]any)
		require.True(t, ok, "tags field should have children")
		require.Len(t, tagsChildren, 1, "tags array should have one element type")
		elementType := tagsChildren[0].(map[string]any)
		assert.Equal(t, "STRING", elementType["type"], "tags array elements should be STRINGs")
	}
}

// validateSchemaStructure validates the basic structure of schema metadata
func validateSchemaStructure(t *testing.T, schema map[string]any) {
	t.Helper()

	// Verify schema has required fields
	require.Contains(t, schema, "name", "schema should have 'name' field")
	require.Contains(t, schema, "type", "schema should have 'type' field")
	require.Contains(t, schema, "children", "schema should have 'children' field")

	// Verify root schema is of type OBJECT (uppercase)
	assert.Equal(t, "OBJECT", schema["type"], "root schema should be of type 'OBJECT'")

	// Verify table name matches
	assert.Equal(t, "test_schema", schema["name"], "schema name should match table name")

	// Verify children is an array
	children, ok := schema["children"].([]any)
	require.True(t, ok, "children should be an array")
	require.NotEmpty(t, children, "children should not be empty")

	// Verify each child has required fields
	for _, child := range children {
		childMap, ok := child.(map[string]any)
		require.True(t, ok, "each child should be a map")
		require.Contains(t, childMap, "name", "child should have 'name' field")
		require.Contains(t, childMap, "type", "child should have 'type' field")
		require.Contains(t, childMap, "optional", "child should have 'optional' field")
	}
}

func TestIntegrationMySQLCDCSchemaInvalidationOnDDL(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")

	// Create a table with initial columns
	db.Exec(`
		CREATE TABLE IF NOT EXISTS ddl_test (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		)
	`)

	// Insert initial row before starting CDC
	db.Exec("INSERT INTO ddl_test VALUES (1, 'initial')")

	template := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: false
  checkpoint_cache: ddlcache
  tables:
    - ddl_test
`, dsn)

	cacheConf := fmt.Sprintf(`
label: ddlcache
file:
  directory: %s`, t.TempDir())

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	type messageWithSchema struct {
		operation string
		data      map[string]any
		schema    map[string]any
	}

	var messages []messageWithSchema
	var msgMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			msgMut.Lock()

			operation, _ := msg.MetaGet("operation")
			data, err := msg.AsStructured()
			require.NoError(t, err)

			// Extract schema metadata
			var schema map[string]any
			err = msg.MetaWalkMut(func(key string, value any) error {
				if key == "schema" {
					if schemaMap, ok := value.(map[string]any); ok {
						schema = schemaMap
					}
				}
				return nil
			})
			require.NoError(t, err)

			messages = append(messages, messageWithSchema{
				operation: operation,
				data:      data.(map[string]any),
				schema:    schema,
			})

			msgMut.Unlock()
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	// Wait for stream to start
	time.Sleep(time.Second * 2)

	// Insert a row - this should capture the initial schema
	db.Exec("INSERT INTO ddl_test VALUES (2, 'before_ddl')")

	// Wait for the message
	assert.Eventually(t, func() bool {
		msgMut.Lock()
		defer msgMut.Unlock()
		return len(messages) >= 1
	}, time.Second*10, time.Millisecond*100)

	msgMut.Lock()
	require.Len(t, messages, 1, "should have received first insert")
	firstMsg := messages[0]
	msgMut.Unlock()

	// Verify first message has schema with 2 fields (id, name)
	require.NotNil(t, firstMsg.schema, "first message should have schema")
	firstChildren, ok := firstMsg.schema["children"].([]any)
	require.True(t, ok, "schema should have children")
	require.Len(t, firstChildren, 2, "initial schema should have 2 fields")

	// Extract field names from first schema
	firstFieldNames := make([]string, 0, len(firstChildren))
	for _, child := range firstChildren {
		childMap := child.(map[string]any)
		firstFieldNames = append(firstFieldNames, childMap["name"].(string))
	}
	assert.ElementsMatch(t, []string{"id", "name"}, firstFieldNames, "initial schema should have id and name")

	// Now perform a DDL change - add a new column
	t.Log("Executing DDL: ALTER TABLE ADD COLUMN")
	db.Exec("ALTER TABLE ddl_test ADD COLUMN email VARCHAR(255)")

	// Give the DDL event time to be processed
	time.Sleep(time.Second * 2)

	// Insert another row with the new column
	db.Exec("INSERT INTO ddl_test (id, name, email) VALUES (3, 'after_ddl', 'test@example.com')")

	// Wait for the second message
	assert.Eventually(t, func() bool {
		msgMut.Lock()
		defer msgMut.Unlock()
		return len(messages) >= 2
	}, time.Second*10, time.Millisecond*100)

	msgMut.Lock()
	require.Len(t, messages, 2, "should have received second insert")
	secondMsg := messages[1]
	msgMut.Unlock()

	// Verify second message has updated schema with 3 fields (id, name, email)
	require.NotNil(t, secondMsg.schema, "second message should have schema")
	secondChildren, ok := secondMsg.schema["children"].([]any)
	require.True(t, ok, "schema should have children")
	require.Len(t, secondChildren, 3, "updated schema should have 3 fields after DDL")

	// Extract field names from second schema
	secondFieldNames := make([]string, 0, len(secondChildren))
	for _, child := range secondChildren {
		childMap := child.(map[string]any)
		secondFieldNames = append(secondFieldNames, childMap["name"].(string))
	}
	assert.ElementsMatch(t, []string{"id", "name", "email"}, secondFieldNames,
		"updated schema should include the new email column")

	// Verify the data includes the email field
	require.Contains(t, secondMsg.data, "email", "second message data should contain email field")
	assert.Equal(t, "test@example.com", secondMsg.data["email"], "email value should match")

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

// killReplicationConnections kills all binlog dump connections to force canal reconnection.
func killReplicationConnections(t *testing.T, db *testDB) {
	t.Helper()
	rows, err := db.Query("SHOW PROCESSLIST")
	require.NoError(t, err)
	defer rows.Close()

	var killed int
	for rows.Next() {
		var id int64
		var user, host, dbName, command, state, info sql.NullString
		var timeVal sql.NullInt64
		err := rows.Scan(&id, &user, &host, &dbName, &command, &timeVal, &state, &info)
		require.NoError(t, err)
		if command.String == "Binlog Dump" {
			_, err := db.DB.Exec(fmt.Sprintf("KILL %d", id))
			require.NoError(t, err)
			killed++
		}
	}
	require.NoError(t, rows.Err())
	require.Greater(t, killed, 0, "expected at least one binlog dump connection to kill")
}

type idCollector struct {
	mu  sync.Mutex
	ids []int64
}

func (c *idCollector) add(id int64) {
	c.mu.Lock()
	c.ids = append(c.ids, id)
	c.mu.Unlock()
}

func (c *idCollector) distinct() map[int64]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[int64]bool{}
	for _, id := range c.ids {
		seen[id] = true
	}
	return seen
}

func (c *idCollector) waitFor(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return len(c.distinct()) >= n
	}, timeout, 100*time.Millisecond, "expected at least %d distinct IDs", n)
}

func (c *idCollector) assertAll(t *testing.T, total int64) {
	t.Helper()
	seen := c.distinct()
	for i := int64(1); i <= total; i++ {
		assert.True(t, seen[i], "missing row with id=%d", i)
	}
}

func buildCDCStream(t *testing.T, dsn, table string, streamSnapshot bool, snapshotMaxBatchSize int) (*service.Stream, *idCollector) {
	t.Helper()

	inputYAML := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: %v
  checkpoint_cache: foocache
  tables:
    - %s`, dsn, streamSnapshot, table)
	if snapshotMaxBatchSize > 0 {
		inputYAML += fmt.Sprintf("\n  snapshot_max_batch_size: %d", snapshotMaxBatchSize)
	}

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	collector := &idCollector{}

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputYAML))

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			data, err := msg.AsStructured()
			if err != nil {
				return fmt.Errorf("AsStructured: %w", err)
			}
			m, ok := data.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected type %T", data)
			}
			id, err := bloblang.ValueAsInt64(m["a"])
			if err != nil {
				return fmt.Errorf("ValueAsInt64: %w", err)
			}
			collector.add(id)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())
	return streamOut, collector
}

func TestIntegrationMySQLCDCConnectionDrop(t *testing.T) {
	integration.CheckSkip(t)
	for _, version := range []string{"8.0", "9.0", "9.1"} {
		t.Run(version, func(t *testing.T) {
			dsn, db := setupTestWithMySQLVersion(t, version)
			db.Exec(`CREATE TABLE IF NOT EXISTS conn_drop (a INT PRIMARY KEY)`)

			streamOut, collector := buildCDCStream(t, dsn, "conn_drop", false, 0)

			go func() {
				if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
			time.Sleep(5 * time.Second)

			for i := 1; i <= 100; i++ {
				db.Exec("INSERT INTO conn_drop VALUES (?)", i)
			}
			collector.waitFor(t, 100, 2*time.Minute)

			killReplicationConnections(t, db)
			time.Sleep(3 * time.Second)

			for i := 101; i <= 200; i++ {
				db.Exec("INSERT INTO conn_drop VALUES (?)", i)
			}
			collector.waitFor(t, 200, 2*time.Minute)
			collector.assertAll(t, 200)

			require.NoError(t, streamOut.StopWithin(10*time.Second))
		})
	}
}

func TestIntegrationMySQLCDCConnectionDropWithBinlogRotation(t *testing.T) {
	integration.CheckSkip(t)
	for _, version := range []string{"8.0", "9.0", "9.1"} {
		t.Run(version, func(t *testing.T) {
			dsn, db := setupTestWithMySQLVersion(t, version)
			db.Exec(`CREATE TABLE IF NOT EXISTS conn_drop_rotate (a INT PRIMARY KEY)`)

			streamOut, collector := buildCDCStream(t, dsn, "conn_drop_rotate", false, 0)

			go func() {
				if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
			time.Sleep(5 * time.Second)

			// Before rotation
			for i := 1; i <= 100; i++ {
				db.Exec("INSERT INTO conn_drop_rotate VALUES (?)", i)
			}
			collector.waitFor(t, 100, 2*time.Minute)

			db.Exec("FLUSH BINARY LOGS")

			// In new binlog file
			for i := 101; i <= 200; i++ {
				db.Exec("INSERT INTO conn_drop_rotate VALUES (?)", i)
			}
			collector.waitFor(t, 200, 2*time.Minute)

			killReplicationConnections(t, db)
			time.Sleep(3 * time.Second)

			// After reconnection
			for i := 201; i <= 300; i++ {
				db.Exec("INSERT INTO conn_drop_rotate VALUES (?)", i)
			}
			collector.waitFor(t, 300, 2*time.Minute)
			collector.assertAll(t, 300)

			require.NoError(t, streamOut.StopWithin(10*time.Second))
		})
	}
}

func TestIntegrationMySQLCDCConnectionDropAfterSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	dsn, db := setupTestWithMySQLVersion(t, "8.0")
	db.Exec(`CREATE TABLE IF NOT EXISTS conn_drop_snap (a INT AUTO_INCREMENT PRIMARY KEY)`)

	for range 500 {
		db.Exec("INSERT INTO conn_drop_snap (a) VALUES (DEFAULT)")
	}

	streamOut, collector := buildCDCStream(t, dsn, "conn_drop_snap", true, 250)

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	collector.waitFor(t, 500, 5*time.Minute)

	for i := range 100 {
		db.Exec("INSERT INTO conn_drop_snap VALUES (?)", 501+i)
	}
	collector.waitFor(t, 600, 2*time.Minute)

	killReplicationConnections(t, db)
	time.Sleep(3 * time.Second)

	for i := range 100 {
		db.Exec("INSERT INTO conn_drop_snap VALUES (?)", 601+i)
	}
	collector.waitFor(t, 700, 2*time.Minute)
	collector.assertAll(t, 700)

	require.NoError(t, streamOut.StopWithin(10*time.Second))
}

func TestIntegrationMySQLCDCRepeatedConnectionDrops(t *testing.T) {
	integration.CheckSkip(t)
	for _, version := range []string{"8.0", "9.0", "9.1"} {
		t.Run(version, func(t *testing.T) {
			dsn, db := setupTestWithMySQLVersion(t, version)
			db.Exec(`CREATE TABLE IF NOT EXISTS conn_drop_repeat (a INT PRIMARY KEY)`)

			streamOut, collector := buildCDCStream(t, dsn, "conn_drop_repeat", false, 0)

			go func() {
				if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
			time.Sleep(5 * time.Second)

			for round := range 3 {
				start := round*50 + 1
				for i := start; i < start+50; i++ {
					db.Exec("INSERT INTO conn_drop_repeat VALUES (?)", i)
				}
				collector.waitFor(t, (round+1)*50, 2*time.Minute)
				killReplicationConnections(t, db)
				time.Sleep(3 * time.Second)
			}

			for i := 151; i <= 200; i++ {
				db.Exec("INSERT INTO conn_drop_repeat VALUES (?)", i)
			}
			collector.waitFor(t, 200, 2*time.Minute)
			collector.assertAll(t, 200)

			require.NoError(t, streamOut.StopWithin(10*time.Second))
		})
	}
}

func TestIntegrationMySQLChunkedSnapshot(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")

	db.Exec(`CREATE TABLE chunked_snap (a INT PRIMARY KEY)`)
	for i := 1; i <= 200; i++ {
		db.Exec("INSERT INTO chunked_snap VALUES (?)", i)
	}

	inputYAML := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 50
  snapshot_max_parallel_tables: 2
  snapshot_chunks_per_table: 4
  checkpoint_cache: foocache
  tables:
    - chunked_snap
`, dsn)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	collector := &idCollector{}
	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputYAML))
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			data, err := msg.AsStructured()
			if err != nil {
				return fmt.Errorf("AsStructured: %w", err)
			}
			m, ok := data.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected type %T", data)
			}
			id, err := bloblang.ValueAsInt64(m["a"])
			if err != nil {
				return fmt.Errorf("ValueAsInt64: %w", err)
			}
			collector.add(id)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	collector.waitFor(t, 200, 2*time.Minute)
	collector.assertAll(t, 200)
	require.NoError(t, streamOut.StopWithin(10*time.Second))
}

func TestIntegrationMySQLChunkedSnapshotNonNumericPKFallback(t *testing.T) {
	dsn, db := setupTestWithMySQLVersion(t, "8.0")

	db.Exec(`CREATE TABLE varchar_snap (id VARCHAR(36) PRIMARY KEY, val INT)`)
	for i := 1; i <= 50; i++ {
		db.Exec("INSERT INTO varchar_snap VALUES (?, ?)", fmt.Sprintf("key-%d", i), i)
	}

	inputYAML := fmt.Sprintf(`
mysql_cdc:
  dsn: %s
  stream_snapshot: true
  snapshot_chunks_per_table: 4
  checkpoint_cache: foocache
  tables:
    - varchar_snap
`, dsn)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, t.TempDir())

	var count atomic.Int64
	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputYAML))
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		count.Add(int64(len(mb)))
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	assert.Eventually(t, func() bool {
		return count.Load() >= 50
	}, 2*time.Minute, 100*time.Millisecond, "expected 50 rows from non-numeric PK table")
	require.NoError(t, streamOut.StopWithin(10*time.Second))
}
