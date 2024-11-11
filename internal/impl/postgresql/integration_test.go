// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/lib/pq"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type FakeFlightRecord struct {
	RealAddress faker.RealAddress `faker:"real_address"`
	CreatedAt   int64             `fake:"unix_time"`
}

func GetFakeFlightRecord() FakeFlightRecord {
	flightRecord := FakeFlightRecord{}
	err := faker.FakeData(&flightRecord)
	if err != nil {
		panic(err)
	}

	return flightRecord
}

func ResourceWithPostgreSQLVersion(t *testing.T, pool *dockertest.Pool, version string) (*dockertest.Resource, *sql.DB, error) {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        version,
		Env: []string{
			"POSTGRES_PASSWORD=l]YLSc|4[i56%{gY",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"
	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return err
		}

		if err = db.Ping(); err != nil {
			return err
		}

		var walLevel string
		if err = db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
			return err
		}

		var pgConfig string
		if err = db.QueryRow("SHOW config_file").Scan(&pgConfig); err != nil {
			return err
		}

		if walLevel != "logical" {
			return fmt.Errorf("wal_level is not logical")
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")
		if err != nil {
			return err
		}

		// flights_non_streamed is a control table with data that should not be streamed or queried by snapshot streaming
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights_non_streamed (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")

		return err
	}); err != nil {
		panic(fmt.Errorf("could not connect to docker: %w", err))
	}

	return resource, db, nil
}

func TestIntegrationPgCDC(t *testing.T) {
	integration.CheckSkip(t)

	tmpDir := t.TempDir()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Use custom PostgreSQL image with wal2json plugin compiled in
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "usedatabrew/pgwal2json",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=l]YLSc|4[i56%{gY",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"
	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB

	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return err
		}

		if err = db.Ping(); err != nil {
			return err
		}

		var walLevel string
		if err = db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
			return err
		}

		var pgConfig string
		if err = db.QueryRow("SHOW config_file").Scan(&pgConfig); err != nil {
			return err
		}

		if walLevel != "logical" {
			return fmt.Errorf("wal_level is not logical")
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")
		if err != nil {
			return err
		}

		// flights_non_streamed is a control table with data that should not be streamed or queried by snapshot streaming
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights_non_streamed (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")

		return err
	}); err != nil {
		panic(fmt.Errorf("could not connect to docker: %w", err))
	}

	for i := 0; i < 1000; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot
    decoding_plugin: wal2json
    stream_snapshot: true
    schema: public
    tables:
       - flights
`, databaseURL)

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

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
		_ = streamOut.Run(context.Background())
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1000
	}, time.Second*25, time.Millisecond*100)

	for i := 0; i < 1000; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2000
	}, time.Second, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Starting stream for the same replication slot should continue from the last LSN
	// Meaning we must not receive any old messages again

	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
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

	go func() {
		assert.NoError(t, streamOut.Run(context.Background()))
	}()

	time.Sleep(time.Second * 5)
	for i := 0; i < 50; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 50
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
	t.Log("All the conditions are met 🎉")

	t.Cleanup(func() {
		db.Close()
	})
}

func TestIntegrationPgCDCForPgOutputPlugin(t *testing.T) {
	integration.CheckSkip(t)
	tmpDir := t.TempDir()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    decoding_plugin: pgoutput
    schema: public
    tables:
       - flights
`, databaseURL)

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
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
		_ = streamOut.Run(context.Background())
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10
	}, time.Second*25, time.Millisecond*100)

	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 20
	}, time.Second*25, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Starting stream for the same replication slot should continue from the last LSN
	// Meaning we must not receive any old messages again

	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamOut.Run(context.Background()))
	}()

	time.Sleep(time.Second * 5)
	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
	t.Log("All the conditions are met 🎉")

	t.Cleanup(func() {
		db.Close()
	})
}

func TestIntegrationPgStreamingFromRemoteDB(t *testing.T) {
	t.Skip("This test requires a remote database to run. Aimed to test remote databases")
	tmpDir := t.TempDir()

	// tables: users, products, orders, order_items

	template := `
pg_stream:
    dsn: postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
    slot_name: test_slot_native_decoder
    snapshot_batch_size: 100000
    stream_snapshot: true
    decoding_plugin: pgoutput
    stream_uncommitted: false
    temporary_slot: true
    schema: public
    tables:
       - users
       - products
       - orders
       - order_items
`

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outMessages int64
	var outMessagesMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
		_, err := mb[0].AsBytes()
		require.NoError(t, err)
		outMessagesMut.Lock()
		outMessages += 1
		outMessagesMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		_ = streamOut.Run(context.Background())
	}()

	assert.Eventually(t, func() bool {
		outMessagesMut.Lock()
		defer outMessagesMut.Unlock()
		return outMessages == 200000
	}, time.Minute*15, time.Millisecond*100)

	t.Log("Backfill conditioins are met 🎉")

	// you need to start inserting the data somewhere in another place
	time.Sleep(time.Minute * 30)
	outMessages = 0
	assert.Eventually(t, func() bool {
		outMessagesMut.Lock()
		defer outMessagesMut.Unlock()
		return outMessages == 1000000
	}, time.Minute*15, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationPgCDCForPgOutputStreamUncommittedPlugin(t *testing.T) {
	integration.CheckSkip(t)
	tmpDir := t.TempDir()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	for i := 0; i < 10000; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    snapshot_batch_size: 100
    stream_snapshot: true
    decoding_plugin: pgoutput
    stream_uncommitted: true
    schema: public
    tables:
       - flights
`, databaseURL)

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

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

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10000
	}, time.Second*25, time.Millisecond*100)

	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10010
	}, time.Second*25, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Starting stream for the same replication slot should continue from the last LSN
	// Meaning we must not receive any old messages again

	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamOut.Run(context.Background()))
	}()

	time.Sleep(time.Second * 5)
	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
	t.Log("All the conditions are met 🎉")

	t.Cleanup(func() {
		db.Close()
	})
}

func TestIntegrationPgMultiVersionsCDCForPgOutputStreamUncomitedPlugin(t *testing.T) {
	integration.CheckSkip(t)
	// running tests in the look to test different PostgreSQL versions
	t.Parallel()
	for _, v := range []string{"17", "16", "15", "14", "13", "12", "11", "10"} {
		tmpDir := t.TempDir()
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		var (
			resource *dockertest.Resource
			db       *sql.DB
		)

		resource, db, err = ResourceWithPostgreSQLVersion(t, pool, v)
		require.NoError(t, err)
		require.NoError(t, resource.Expire(120))

		hostAndPort := resource.GetHostPort("5432/tcp")
		hostAndPortSplited := strings.Split(hostAndPort, ":")
		password := "l]YLSc|4[i56%{gY"

		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
		template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    decoding_plugin: pgoutput
    stream_uncommitted: true
    schema: public
    tables:
       - flights
`, databaseURL)

		cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

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
			_ = streamOut.Run(context.Background())
		}()

		assert.Eventually(t, func() bool {
			outBatchMut.Lock()
			defer outBatchMut.Unlock()
			return len(outBatches) == 1000
		}, time.Second*25, time.Millisecond*100)

		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			outBatchMut.Lock()
			defer outBatchMut.Unlock()
			return len(outBatches) == 2000
		}, time.Second*25, time.Millisecond*100)

		require.NoError(t, streamOut.StopWithin(time.Second*10))

		// Starting stream for the same replication slot should continue from the last LSN
		// Meaning we must not receive any old messages again

		streamOutBuilder = service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
		require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
		require.NoError(t, streamOutBuilder.AddInputYAML(template))

		outBatches = []string{}
		require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
			msgBytes, err := m.AsBytes()
			require.NoError(t, err)
			outBatchMut.Lock()
			outBatches = append(outBatches, string(msgBytes))
			outBatchMut.Unlock()
			return nil
		}))

		streamOut, err = streamOutBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamOut.Run(context.Background()))
		}()

		time.Sleep(time.Second * 5)
		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			outBatchMut.Lock()
			defer outBatchMut.Unlock()
			return len(outBatches) == 1000
		}, time.Second*20, time.Millisecond*100)

		require.NoError(t, streamOut.StopWithin(time.Second*10))
		t.Log("All the conditions are met 🎉")

		t.Cleanup(func() {
			db.Close()
		})
	}
}

func TestIntegrationPgMultiVersionsCDCForPgOutputStreamComittedPlugin(t *testing.T) {
	integration.CheckSkip(t)
	for _, v := range []string{"17", "16", "15", "14", "13", "12", "11", "10"} {
		tmpDir := t.TempDir()
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		var (
			resource *dockertest.Resource
			db       *sql.DB
		)

		resource, db, err = ResourceWithPostgreSQLVersion(t, pool, v)
		require.NoError(t, err)
		require.NoError(t, resource.Expire(120))

		hostAndPort := resource.GetHostPort("5432/tcp")
		hostAndPortSplited := strings.Split(hostAndPort, ":")
		password := "l]YLSc|4[i56%{gY"

		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
		template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    decoding_plugin: pgoutput
    stream_uncommitted: false
    schema: public
    tables:
       - flights
`, databaseURL)

		cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
		require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
		require.NoError(t, streamOutBuilder.AddInputYAML(template))

		var outMessages []string
		var outMessagesMut sync.Mutex

		require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
			msgBytes, err := m.AsBytes()
			require.NoError(t, err)
			outMessagesMut.Lock()
			outMessages = append(outMessages, string(msgBytes))
			outMessagesMut.Unlock()
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		go func() {
			_ = streamOut.Run(context.Background())
		}()

		assert.Eventually(t, func() bool {
			outMessagesMut.Lock()
			defer outMessagesMut.Unlock()
			return len(outMessages) == 1000
		}, time.Second*25, time.Millisecond*100)

		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			outMessagesMut.Lock()
			defer outMessagesMut.Unlock()
			return len(outMessages) == 2000
		}, time.Second*25, time.Millisecond*100)

		require.NoError(t, streamOut.StopWithin(time.Second*10))

		// Starting stream for the same replication slot should continue from the last LSN
		// Meaning we must not receive any old messages again

		streamOutBuilder = service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
		require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
		require.NoError(t, streamOutBuilder.AddInputYAML(template))

		outMessages = []string{}
		require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
			msgBytes, err := m.AsBytes()
			require.NoError(t, err)
			outMessagesMut.Lock()
			outMessages = append(outMessages, string(msgBytes))
			outMessagesMut.Unlock()
			return nil
		}))

		streamOut, err = streamOutBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamOut.Run(context.Background()))
		}()

		time.Sleep(time.Second * 5)
		for i := 0; i < 1000; i++ {
			f := GetFakeFlightRecord()
			_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
			require.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			outMessagesMut.Lock()
			defer outMessagesMut.Unlock()
			return len(outMessages) == 1000
		}, time.Second*20, time.Millisecond*100)

		require.NoError(t, streamOut.StopWithin(time.Second*10))
		t.Log("All the conditions are met 🎉")

		t.Cleanup(func() {
			db.Close()
		})
	}
}
