// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mssqlserver

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

type testDB struct {
	*sql.DB

	t *testing.T
}

func (db *testDB) Exec(query string, args ...any) {
	_, err := db.DB.Exec(query, args...)
	require.NoError(db.t, err)
}

func TestIntegrationMSSQLServerSnapshotAndCDC(t *testing.T) {
	_, db := setupTestWithMSSQLServerVersion(t, "2022-latest")
	require.NotNil(t, db)
}

func setupTestWithMSSQLServerVersion(t *testing.T, version string) (string, *testDB) {
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute
	// MS SQL Server specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server",
		Tag:        version,
		Env: []string{
			"ACCEPT_EULA=y",
			"MSSQL_SA_PASSWORD=YourStrong!Passw0rd",
			"MSSQL_AGENT_ENABLED=true",
		},
		Cmd:          []string{},
		ExposedPorts: []string{"1433:1433"},
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

	port := resource.GetPort("1433/tcp")
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "master")

	var db *sql.DB
	err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		_, err = db.Exec(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
			BEGIN
				CREATE DATABASE testdb;
			END;`)
		if err != nil {
			return err
		}
		db.Close()

		connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "testdb")
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		// enable CDC on database
		if _, err = db.Exec("EXEC sys.sp_cdc_enable_db;"); err != nil {
			return err
		}

		_, err = db.Exec(`		
			IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'users' AND schema_id = SCHEMA_ID('dbo'))
			BEGIN
				CREATE TABLE dbo.users (
					id INT PRIMARY KEY,
					name NVARCHAR(100)
				);
			END;`)
		return err
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, &testDB{db, t}
}
