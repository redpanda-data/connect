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
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationMySQLCDC(t *testing.T) {
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

			if err != nil {
				return err
			}

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
  flavor: mysql
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
