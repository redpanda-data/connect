// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq" // registers "postgres" driver for sql.Open in tests

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// TestIntegrationResolveSchemasReportsInaccessibleSchemas verifies that a
// schema pattern matching a schema the connecting role lacks USAGE on is
// reported via inaccessibleSchemas rather than silently dropped, since
// information_schema.schemata alone would make it indistinguishable from a
// schema that simply doesn't exist.
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
