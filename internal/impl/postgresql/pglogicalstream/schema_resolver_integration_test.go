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

func TestIntegrationResolveSchemasUUIDSuffixedSchemas(t *testing.T) {
	integration.CheckSkip(t)

	_, adminURL := createDockerInstance(t)

	adminDB, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)
	defer adminDB.Close()

	// Quoting is mandatory here because of the UUID's hyphens, regardless of
	// case - so both of these preserve their literal casing exactly as written.
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

	// Both schemas match the unquoted glob despite the case difference in
	// their literal prefix - matching is case-insensitive independent of how
	// the schema itself was created.
	assert.ElementsMatch(t, []string{lowerCaseSchema, mixedCaseSchema}, visible)
	assert.Empty(t, inaccessible)

	// A quoted pattern is still exact and case-sensitive: it picks out only
	// the schema whose case matches the pattern, with no wildcard expansion.
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

	// A bare wildcard has no literal characters to case-compare, so this is
	// unaffected by hex-digit casing either way - included as a baseline.
	visible, _, err := resolveSchemas(ctx, conn, "*")
	require.NoError(t, err)
	assert.Contains(t, visible, bareUUIDSchema)

	// The interesting case: an unquoted pattern whose only literal portion is
	// a lower-case chunk of the UUID itself (no prefix) must still match the
	// upper-case schema case-insensitively.
	visible, _, err = resolveSchemas(ctx, conn, "a0eebc99-*")
	require.NoError(t, err)
	assert.Equal(t, []string{bareUUIDSchema}, visible)

	// Same, but the literal chunk sits in the middle rather than at the start.
	visible, _, err = resolveSchemas(ctx, conn, "*-bb6d-*")
	require.NoError(t, err)
	assert.Equal(t, []string{bareUUIDSchema}, visible)

	// A quoted pattern remains an exact, case-sensitive lookup: the
	// differently-cased quoted form matches nothing.
	visible, _, err = resolveSchemas(ctx, conn, `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`)
	require.NoError(t, err)
	assert.Empty(t, visible)
}
