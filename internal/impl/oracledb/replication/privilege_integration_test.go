// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// TestIntegrationProbeStatement makes sure that ProbeStatement returns an
// error that IsMissingPrivilege recognizes when the user lacks a privilege, and
// that it leaves no row in the table.
func TestIntegrationProbeStatement(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	table := db.Schema + ".PROBE_TARGET"
	db.MustExecContext(t.Context(), "CREATE TABLE "+table+" (id NUMBER PRIMARY KEY)")

	tests := []struct {
		name        string
		grants      string
		wantMissing bool
	}{
		{
			name:        "no insert",
			grants:      "SELECT, UPDATE",
			wantMissing: true,
		},
		{
			name:        "insert",
			grants:      "SELECT, INSERT, UPDATE",
			wantMissing: false,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Given a user with %s on the table", tc.grants)
			user := fmt.Sprintf("%s_P%d", db.Schema, i)
			userConnStr := db.CreateUserWithGrants(t, connStr, user, "GRANT "+tc.grants+" ON "+table+" TO "+user)
			userDB, err := sql.Open("oracle", userConnStr)
			require.NoError(t, err)
			// Registered after the user, so it runs before the drop of the user.
			t.Cleanup(func() { assert.NoError(t, userDB.Close()) })

			t.Log("When the user probes an INSERT")
			err = replication.ProbeStatement(t.Context(), userDB, "INSERT INTO "+table+" (id) VALUES (1)")

			t.Log("Then a missing INSERT is a missing privilege error, and no row stays")
			if tc.wantMissing {
				_, missing := replication.IsMissingPrivilege(err)
				assert.Truef(t, missing, "expected a missing privilege error, got: %v", err)
			} else {
				assert.NoError(t, err)
			}
			var rows int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&rows))
			assert.Zero(t, rows)
		})
	}
}
