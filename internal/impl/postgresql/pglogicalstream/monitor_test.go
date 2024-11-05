// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jaswdr/faker"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func Test_MonitorReplorting(t *testing.T) {
	t.Skip("Skipping for now")
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
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
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	databaseURL := fmt.Sprintf("user=user_name password=secret dbname=dbname sslmode=disable host=%s port=%s", hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB
	pool.MaxWait = 120 * time.Second
	err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return err
		}

		if err = db.Ping(); err != nil {
			return err
		}

		return err
	})
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")
	require.NoError(t, err)

	fake := faker.New()
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", fake.Address().City(), fake.Time().RFC1123(time.Now()))
		require.NoError(t, err)
	}

	require.NoError(t, err)
	slotName := "test_slot"
	mon, err := NewMonitor(databaseURL, &service.Logger{}, []string{"flights"}, slotName)
	require.NoError(t, err)
	require.NotNil(t, mon)
}
