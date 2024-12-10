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
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		"root:password@tcp(localhost:%s)/testdb?parseTime=true&timeout=30s&readTimeout=30s&writeTimeout=30s&multiStatements=true",
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

		go func() {
			err = streamOut.Run(context.Background())
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

		time.Sleep(time.Second)
		for i := 1001; i < 2001; i++ {
			db.Exec("INSERT INTO foo VALUES (?)", i)
		}

		go func() {
			err = streamOut.Run(context.Background())
			require.NoError(t, err)
		}()

		assert.Eventually(t, func() bool {
			outBatchMut.Lock()
			defer outBatchMut.Unlock()
			return len(outBatches) == 1000
		}, time.Minute*5, time.Millisecond*100)

		require.NoError(t, streamOut.StopWithin(time.Second*10))
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

	go func() {
		err = streamOut.Run(context.Background())
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
    CREATE TABLE IF NOT EXISTS foo (
        a INT,
        b INT,
        v JSON,
        size ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
        PRIMARY KEY (a, b)
    )
`)
	// Create control table to ensure we don't stream it
	db.Exec(`
    CREATE TABLE IF NOT EXISTS foo_non_streamed (
        a INT,
        b INT,
        v JSON,
        PRIMARY KEY (a, b)
    )
`)

	// Insert 1000 rows for initial snapshot streaming
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO foo VALUES (?, ?, ?, ?)", i, i, `{"json":"data"}`, `large`)
		db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?, ?)", i, i, `{"json":"data"}`)
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

	go func() {
		err = streamOut.Run(context.Background())
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 5)
	for i := 1000; i < 2000; i++ {
		// Insert 10000 rows
		db.Exec("INSERT INTO foo VALUES (?, ?, ?, ?)", i, i, `{"json":"data"}`, `x-small`)
		db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?, ?)", i, i, `{"json":"data"}`)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)
	require.NoError(t, streamOut.StopWithin(time.Second*10))
}
