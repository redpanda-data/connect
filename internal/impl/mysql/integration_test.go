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

func TestIntegrationMySQLCDC(t *testing.T) {
	integration.CheckSkip(t)
	var mysqlTestVersions = []string{"8.0", "9.0", "9.1"}
	for _, version := range mysqlTestVersions {
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

		// Create table
		_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT PRIMARY KEY
    )
`)
		require.NoError(t, err)
		tmpDir := t.TempDir()

		template := fmt.Sprintf(`
mysql_stream:
  dsn: %s
  stream_snapshot: false
  checkpoint_key: foocache
  tables:
    - foo
`, dsn)

		cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, tmpDir)

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
			if _, err = db.Exec("INSERT INTO foo VALUES (?)", i); err != nil {
				require.NoError(t, err)
			}
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
			_, err = db.Exec("INSERT INTO foo VALUES (?)", i)
			require.NoError(t, err)
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
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	// MySQL specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0",
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

	// Create table
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT PRIMARY KEY
    )
`)
	require.NoError(t, err)
	tmpDir := t.TempDir()

	// Insert 1000 rows for initial snapshot streaming
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO foo VALUES (?)", i)
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
mysql_stream:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 500
  checkpoint_key: foocache
  tables:
    - foo
`, dsn)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, tmpDir)

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
		if _, err = db.Exec("INSERT INTO foo VALUES (?)", i); err != nil {
			require.NoError(t, err)
		}
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationMySQLCDCWithCompositePrimaryKeys(t *testing.T) {
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	// MySQL specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0",
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

	// Create table
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS foo (
        a INT,
        b INT,
        PRIMARY KEY (a, b)
    )
`)
	require.NoError(t, err)

	// Create controll table to ensure we don't stream it
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS foo_non_streamed (
        a INT,
        b INT,
        PRIMARY KEY (a, b)
    )
`)

	require.NoError(t, err)
	tmpDir := t.TempDir()

	// Insert 1000 rows for initial snapshot streaming
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO foo VALUES (?, ?)", i, i)
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?)", i, i)
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
mysql_stream:
  dsn: %s
  stream_snapshot: true
  snapshot_max_batch_size: 500
  checkpoint_key: foocache
  tables:
    - foo
`, dsn)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %s`, tmpDir)

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
		if _, err = db.Exec("INSERT INTO foo VALUES (?, ?)", i, i); err != nil {
			require.NoError(t, err)
		}

		_, err = db.Exec("INSERT INTO foo_non_streamed VALUES (?, ?)", i, i)
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}
