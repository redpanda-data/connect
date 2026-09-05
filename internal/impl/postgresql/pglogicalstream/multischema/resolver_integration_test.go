// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package multischema

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq" // registers "postgres" driver for sql.Open in tests

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationResolveSchemasReportsInaccessibleSchemas(t *testing.T) {
	integration.CheckSkip(t)

	_, adminURL := createDockerInstance(t)

	adminDB, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)
	defer adminDB.Close()

	_, err = adminDB.Exec("CREATE SCHEMA visible_schema")
	require.NoError(t, err)
	_, err = adminDB.Exec("CREATE SCHEMA hidden_schema")
	require.NoError(t, err)

	_, err = adminDB.Exec("CREATE ROLE restricted_role LOGIN PASSWORD 'restricted_pw'")
	require.NoError(t, err)
	_, err = adminDB.Exec("GRANT CONNECT ON DATABASE dbname TO restricted_role")
	require.NoError(t, err)
	_, err = adminDB.Exec("GRANT USAGE ON SCHEMA visible_schema TO restricted_role")
	require.NoError(t, err)
	// Deliberately no GRANT on hidden_schema.

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	restrictedConfig, err := pgconn.ParseConfig(adminURL)
	require.NoError(t, err)
	restrictedConfig.User = "restricted_role"
	restrictedConfig.Password = "restricted_pw"
	delete(restrictedConfig.RuntimeParams, "replication")

	restrictedConn, err := pgconn.ConnectConfig(ctx, restrictedConfig)
	require.NoError(t, err)
	defer closeConn(t, restrictedConn)

	visible, inaccessible, err := resolveSchemas(ctx, restrictedConn, "*_schema")
	require.NoError(t, err)

	assert.Equal(t, []string{`"visible_schema"`}, visible)
	assert.Equal(t, []string{`"hidden_schema"`}, inaccessible)
}

func TestIntegrationResolveSchemasUUIDSuffixedSchemas(t *testing.T) {
	integration.CheckSkip(t)

	_, adminURL := createDockerInstance(t)

	adminDB, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)
	defer adminDB.Close()

	const (
		lowerCaseSchema = `"tenant_a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`
		mixedCaseSchema = `"Tenant_9c0b4ef8-bb6d-6bb9-bd38-0a11a0eebc99"`
	)
	_, err = adminDB.Exec(`CREATE SCHEMA ` + lowerCaseSchema)
	require.NoError(t, err)
	_, err = adminDB.Exec(`CREATE SCHEMA ` + mixedCaseSchema)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	visible, inaccessible, err := resolveSchemas(ctx, conn, "tenant_*")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{lowerCaseSchema, mixedCaseSchema}, visible)
	assert.Empty(t, inaccessible)

	exactVisible, _, err := resolveSchemas(ctx, conn, mixedCaseSchema)
	require.NoError(t, err)
	assert.Equal(t, []string{mixedCaseSchema}, exactVisible)
}

func TestIntegrationResolveSchemasBareUUIDSchema(t *testing.T) {
	integration.CheckSkip(t)

	_, adminURL := createDockerInstance(t)

	adminDB, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)
	defer adminDB.Close()

	// Upper-case hex digits, no prefix - quoting is mandatory purely because
	// of the hyphens, not because of anything alphabetic.
	const bareUUIDSchema = `"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"`
	_, err = adminDB.Exec(`CREATE SCHEMA ` + bareUUIDSchema)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	visible, _, err := resolveSchemas(ctx, conn, "*")
	require.NoError(t, err)
	assert.Contains(t, visible, bareUUIDSchema)

	visible, _, err = resolveSchemas(ctx, conn, "a0eebc99-*")
	require.NoError(t, err)
	assert.Equal(t, []string{bareUUIDSchema}, visible)

	visible, _, err = resolveSchemas(ctx, conn, "*-bb6d-*")
	require.NoError(t, err)
	assert.Equal(t, []string{bareUUIDSchema}, visible)

	visible, _, err = resolveSchemas(ctx, conn, `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`)
	require.NoError(t, err)
	assert.Empty(t, visible)
}

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func createDockerInstance(t *testing.T) (cleanup func(), dbURL string) {
	ctr, err := testcontainers.Run(t.Context(), "postgres:16",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "secret",
			"POSTGRES_USER":     "user_name",
			"POSTGRES_DB":       "dbname",
		}),
		testcontainers.WithCmd("postgres", "-c", "wal_level=logical"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	databaseURL := fmt.Sprintf("user=user_name password=secret dbname=dbname sslmode=disable host=%s port=%s replication=database", host, mp.Port())

	var db *sql.DB
	require.Eventually(t, func() bool {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return false
		}
		return db.Ping() == nil
	}, 2*time.Minute, time.Second)

	cleanup = func() {
		// Container cleanup is handled by testcontainers.CleanupContainer
	}

	return cleanup, databaseURL
}
