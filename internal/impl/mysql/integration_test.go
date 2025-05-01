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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	// MySQL specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        version,
		Env: []string{
			"MYSQL_ROOT_PASSWORD=password",
			"MYSQL_DATABASE=testdb",
		},
		Cmd: []string{
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
			"--log-slave-updates=ON",
		},
		ExposedPorts: []string{"3306/tcp"},
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

	port := resource.GetPort("3306/tcp")
	dsn := fmt.Sprintf(
		"root:password@tcp(localhost:%s)/testdb?timeout=30s&readTimeout=30s&writeTimeout=30s&multiStatements=true",
		port,
	)

	var db *sql.DB
	err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		return db.Ping()
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return dsn, &testDB{db, t}
}

func TestIntegrationMySQLCDC(t *testing.T) {
	integration.CheckSkip(t)
	var mysqlTestVersions = []string{"8.0", "9.0", "9.1"}
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
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
			for i := 0; i < 1000; i++ {
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
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
	for i := 0; i < 1000; i++ {
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
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
	for i := 0; i < 1000; i++ {
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
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
  "decimal_col": 999999999999999999999999999999999999.99,
  "numeric_col": 98765.43,
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
  "decimal_col": 888888888888888888888888888888888888.88,
  "numeric_col": 87654.21,
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
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, batch service.MessageBatch) error {
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
