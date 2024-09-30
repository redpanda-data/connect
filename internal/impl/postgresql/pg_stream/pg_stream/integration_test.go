// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pg_stream

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaswdr/faker"
	_ "github.com/lib/pq"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func TestIntegrationPgCDC(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Use custom PostgreSQL image with wal2json plugin compiled in
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "usedatabrew/pgwal2json",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
		},
		ExposedPorts: []string{"5432"},
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
	databaseUrl := fmt.Sprintf("user=user_name password=secret dbname=dbname sslmode=disable host=%s port=%s", hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB

	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseUrl); err != nil {
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

		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	fake := faker.New()
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", fake.Address().City(), fake.Time().RFC1123(time.Now()))
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
pg_stream:
    host: %s
    slot_name: test_slot
    user: user_name
    password: secret
    port: %s
    schema: public
    tls: none
    stream_snapshot: true
    database: dbname
    tables:
       - flights
`, hostAndPortSplited[0], hostAndPortSplited[1])

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
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
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", fake.Address().City(), fake.Time().RFC1123(time.Now()))
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
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
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
	for i := 0; i < 50; i++ {
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", fake.Address().City(), fake.Time().RFC1123(time.Now()))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outMessagesMut.Lock()
		defer outMessagesMut.Unlock()
		return len(outMessages) == 50
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
	t.Log("All the conditions are met 🎉")

	t.Cleanup(func() {
		db.Close()
	})
}
