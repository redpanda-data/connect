// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_MicrosoftSQLServerCDC_CheckpointCache(t *testing.T) {
	integration.CheckSkip(t)
	connStr, db := mustSetupTestWithMicrosoftSQLServerVersion(t, "2022-latest")

	t.Run("cache initialises checkpoint table", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn.CdcCheckpointCache"
		_, err = newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		// verify table is created
		var exists bool
		q := `SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID(?) AND name = ?;`
		require.NoError(t, db.QueryRowContext(t.Context(), q, "rpcn", "CdcCheckpointCache").Scan(&exists))
		require.Truef(t, exists, "expected table '%s' to exist but it does not", cacheTableToCreate)

		// verify stored procedure is created
		exists = false
		q = `SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type = 'P';`
		require.NoError(t, db.QueryRowContext(t.Context(), q, fmt.Sprintf("%s.%s", "rpcn", "CdcCheckpointCacheUpdate")).Scan(&exists))
		require.True(t, exists, "expected stored procedure to exist")
	})

	t.Run("can set and get cache entries", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn1;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn1.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		// verify set
		var wanted replication.LSN
		require.NoError(t, wanted.Scan([]byte("0x0000002d000004b00003")))
		require.NoError(t, cache.Set(t.Context(), "", wanted, nil))

		// verify get
		lsn, err := cache.Get(t.Context(), "")
		require.NoError(t, err)
		var got replication.LSN

		require.NoError(t, got.Scan(lsn))
		require.Equal(t, wanted, got)
	})

	t.Run("get reports empty cache as key not found", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn2;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn2.empty_cache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		lsn, err := cache.Get(t.Context(), "")
		require.ErrorIs(t, err, service.ErrKeyNotFound)
		require.Nil(t, lsn)
	})

	t.Run("closes gracefully", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn3;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn3.closing_cache"
		cache, err := newCheckpointCache(t.Context(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		require.NoError(t, cache.Close(t.Context()))

		_, err = cache.cacheSetStmt.Exec()
		require.Error(t, err)
		require.Contains(t, err.Error(), "sql: statement is closed")

		err = cache.db.PingContext(t.Context())
		require.Contains(t, err.Error(), "sql: database is closed")
	})
}

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		expectedErr error
	}{
		// Valid cases
		{name: "Valid simple table name", tableName: "dbo.users", expectedErr: nil},
		{name: "Valid table name with numbers", tableName: "dbo.orders_2024", expectedErr: nil},
		{name: "Valid table name with underscore prefix", tableName: "dbo._temp_table", expectedErr: nil},
		{name: "Valid table name with dollar sign", tableName: "dbo.user$data", expectedErr: nil},
		{name: "Valid table name with mixed case", tableName: "dbo.UserProfiles", expectedErr: nil},
		// Invalid cases
		{name: "Empty table name not allowed", tableName: "", expectedErr: errEmptyTableName},
		{name: "Schema is required", tableName: "users", expectedErr: errInvalidTableFormat},
		{name: "Missing schema", tableName: ".users", expectedErr: errInvalidSchemaLength},
		{name: "Table name starting with number not allowed", tableName: "dbo.2users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name starting with # sign not allowed", tableName: "dbo.#users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name starting with @ sign not allowed", tableName: "dbo.@users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with special characters not allowed", tableName: "dbo.users@table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with spaces not allowed", tableName: "dbo.user table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with hyphens not allowed", tableName: "dbo.user-table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name is no more than 128 characters", tableName: "dbo." + strings.Repeat("a", 129), expectedErr: errInvalidTableLength},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateCacheTableName(tc.tableName)

			if tc.expectedErr == nil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tc.expectedErr != nil && err == nil {
				t.Errorf("expected error %v, got nil", tc.expectedErr)
			}
			if tc.expectedErr != nil && err != nil && tc.expectedErr.Error() != err.Error() {
				t.Errorf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func mustSetupTestWithMicrosoftSQLServerVersion(t *testing.T, version string) (string, *sql.DB) {
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
		if db, err = sql.Open("mssql", connectionString); err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return connectionString, db
}
