// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	restrictedUsername = "C##REDPANDA"
	restrictedPassword = "redpanda123"
)

// TestIntegrationOracleDBCDCWithRestrictedPermissions checks the restricted
// grant set two ways: snapshot and streaming.
//
// The streaming subtest uses the base grant list only. The snapshot
// subtest adds one grant, SELECT ANY TABLE, that only it needs.
func TestIntegrationOracleDBCDCWithRestrictedPermissions(t *testing.T) {
	integration.CheckSkip(t)

	t.Run("snapshot", func(t *testing.T) {
		cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)

		createRestrictedCDCUser(t, cdbConnStr)
		grantSnapshotPermission(t, cdbConnStr)
		createRestrictedCheckpointCacheTable(t, cdbConnStr, pdbName)
		restrictedConnStr := restrictedConnectionString(t, cdbConnStr)

		require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.mtfoo",
			"CREATE TABLE testdb.mtfoo (id NUMBER PRIMARY KEY, val NUMBER)"))

		// Seed rows before the stream starts. The snapshot must read them.
		seedRows := map[string]string{"1": "10", "2": "20", "3": "30"}
		for id, val := range seedRows {
			pdbDB.MustExec("INSERT INTO testdb.mtfoo (id, val) VALUES (:1, :2)", id, val)
		}

		type capturedEvent struct {
			operation string
		}
		var (
			outEvents   []capturedEvent
			outEventsMu sync.Mutex
		)

		// snapshot_mode is snapshot_only. It reads every row once, then stops.
		cfg := `
oracledb_cdc:
  connection_string: ` + restrictedConnStr + `
  pdb_name: ` + pdbName + `
  snapshot_mode: snapshot_only
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["TESTDB.MTFOO"]
  batching:
    count: 1`

		t.Log("Launching component with snapshot permissions...")
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(cfg))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outEventsMu.Lock()
			defer outEventsMu.Unlock()
			for _, msg := range mb {
				op, ok := msg.MetaGet("operation")
				assert.True(t, ok, "message missing 'operation' metadata")

				outEvents = append(outEvents, capturedEvent{operation: op})
			}
			return nil
		}))

		stream, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()

		t.Log("Verifying snapshot rows are captured under restricted permissions...")
		want := len(seedRows)
		assert.Eventually(t, func() bool {
			outEventsMu.Lock()
			defer outEventsMu.Unlock()
			return len(outEvents) >= want
		}, time.Minute*5, time.Second*1)

		outEventsMu.Lock()
		events := append([]capturedEvent{}, outEvents...)
		outEventsMu.Unlock()
		require.Lenf(t, events, want, "Wanted %d snapshot rows but got %d", want, len(events))

		for _, e := range events {
			assert.Equalf(t, "read", e.operation, "snapshot row has operation %q, want \"read\"", e.operation)
		}

		require.NoError(t, stream.StopWithin(time.Second*10))
	})

	t.Run("streaming", func(t *testing.T) {
		// SetupCDBTestWithPDB connects to CDB$ROOT as SYSTEM. It also returns
		// a FREEPDB1 connection for test data. The connector uses the
		// restricted user below.
		cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)

		createRestrictedCDCUser(t, cdbConnStr)
		createRestrictedCheckpointCacheTable(t, cdbConnStr, pdbName)
		restrictedConnStr := restrictedConnectionString(t, cdbConnStr)

		require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.mtfoo",
			"CREATE TABLE testdb.mtfoo (id NUMBER PRIMARY KEY, val NUMBER)"))

		// Seed one row before the stream starts. The UPDATE and DELETE below
		// need this row. Snapshotting is off, so this row is not a change event.
		pdbDB.MustExec("INSERT INTO testdb.mtfoo (id, val) VALUES (1, 10)")

		type capturedEvent struct {
			operation string
		}
		var (
			outEvents   []capturedEvent
			outEventsMu sync.Mutex
		)

		// snapshot_mode is none. The restricted user has no table read
		// privilege, so a snapshot would fail at once with ORA-41900.
		cfg := `
oracledb_cdc:
  connection_string: ` + restrictedConnStr + `
  pdb_name: ` + pdbName + `
  snapshot_mode: none
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["TESTDB.MTFOO"]
  batching:
    count: 1`

		t.Log("Launching component with restricted permissions...")
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(cfg))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			outEventsMu.Lock()
			defer outEventsMu.Unlock()
			for _, msg := range mb {
				op, ok := msg.MetaGet("operation")
				assert.True(t, ok, "message missing 'operation' metadata")

				outEvents = append(outEvents, capturedEvent{operation: op})
			}
			return nil
		}))

		stream, err := streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()

		// Wait for the component to start mining redo logs.
		time.Sleep(5 * time.Second)

		t.Log("Verifying streaming changes are captured under restricted permissions...")
		pdbDB.MustExec("INSERT INTO testdb.mtfoo (id, val) VALUES (2, 20)")
		pdbDB.MustExec("UPDATE testdb.mtfoo SET val = 99 WHERE id = 1")
		pdbDB.MustExec("DELETE FROM testdb.mtfoo WHERE id = 2")

		want := 3
		assert.Eventually(t, func() bool {
			outEventsMu.Lock()
			defer outEventsMu.Unlock()
			return len(outEvents) >= want
		}, time.Minute*5, time.Second*1)

		outEventsMu.Lock()
		events := append([]capturedEvent{}, outEvents...)
		outEventsMu.Unlock()
		require.Lenf(t, events, want, "Wanted %d streaming messages (insert/update/delete) but got %d", want, len(events))

		// Index events by operation. Do not assume arrival order. This makes
		// a swapped or duplicate event fail clearly.
		byOp := make(map[string]capturedEvent, len(events))
		for _, e := range events {
			_, dup := byOp[e.operation]
			require.Falsef(t, dup, "received more than one %q event: %+v", e.operation, events)
			byOp[e.operation] = e
		}

		require.Contains(t, byOp, "insert")
		require.Contains(t, byOp, "update")
		require.Contains(t, byOp, "delete")

		require.NoError(t, stream.StopWithin(time.Second*10))
	})
}

// sysConnectionString changes the user in cdbConnStr to SYS.
// The host, port, service, and password do not change.
//
// go-ora gives SYS the SYSDBA privilege by default.
// SYSTEM cannot grant privileges on SYS-owned views like V_$DATABASE,
// even with the DBA role. This connection must use SYS.
func sysConnectionString(t *testing.T, cdbConnStr string) string {
	t.Helper()

	u, err := url.Parse(cdbConnStr)
	require.NoError(t, err)
	password, _ := u.User.Password()
	u.User = url.UserPassword("sys", password)
	return u.String()
}

// createRestrictedCDCUser creates the C##REDPANDA user in CDB$ROOT.
// The grant list does not have SELECT ANY TABLE or LOCK ANY TABLE.
//
// This function opens its own SYSDBA connection. You can call it once
// per test run.
func createRestrictedCDCUser(t *testing.T, cdbConnStr string) {
	t.Helper()
	ctx := t.Context()

	cdbDB, err := sql.Open("oracle", sysConnectionString(t, cdbConnStr))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cdbDB.Close()) })
	require.NoError(t, cdbDB.PingContext(ctx))

	_, err = cdbDB.ExecContext(ctx, `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'C##REDPANDA';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER C##REDPANDA IDENTIFIED BY redpanda123 CONTAINER=ALL';
		END IF;
	END;`)
	require.NoError(t, err, "creating restricted C##REDPANDA user")

	// This list skips two statements: ALTER DATABASE (SetupCDBTestWithPDB
	// already runs those) and SELECT ANY TABLE / LOCK ANY TABLE (excluded
	// on purpose, to keep this a restricted grant set).
	grants := []string{
		"GRANT CREATE SESSION TO C##REDPANDA CONTAINER=ALL",
		"GRANT SET CONTAINER TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$DATABASE TO C##REDPANDA CONTAINER=ALL",
		"GRANT FLASHBACK ANY TABLE TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT_CATALOG_ROLE TO C##REDPANDA CONTAINER=ALL",
		"GRANT EXECUTE_CATALOG_ROLE TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ANY TRANSACTION TO C##REDPANDA CONTAINER=ALL",
		"GRANT LOGMINING TO C##REDPANDA CONTAINER=ALL",
		"GRANT CREATE TABLE TO C##REDPANDA CONTAINER=ALL",
		"GRANT CREATE SEQUENCE TO C##REDPANDA CONTAINER=ALL",
		"GRANT UNLIMITED TABLESPACE TO C##REDPANDA CONTAINER=ALL",
		"GRANT EXECUTE ON DBMS_LOGMNR TO C##REDPANDA CONTAINER=ALL",
		"GRANT EXECUTE ON DBMS_LOGMNR_D TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOG TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOG_HISTORY TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOGMNR_LOGS TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOGMNR_CONTENTS TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOGMNR_PARAMETERS TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$LOGFILE TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$ARCHIVED_LOG TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$TRANSACTION TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$MYSTAT TO C##REDPANDA CONTAINER=ALL",
		"GRANT SELECT ON V_$STATNAME TO C##REDPANDA CONTAINER=ALL",
	}
	for _, grant := range grants {
		_, err := cdbDB.ExecContext(ctx, grant)
		require.NoError(t, err, "executing grant: %s", grant)
	}
}

// restrictedConnectionString changes the user in cdbConnStr to
// C##REDPANDA. The host, port, and service do not change.
func restrictedConnectionString(t *testing.T, cdbConnStr string) string {
	t.Helper()

	u, err := url.Parse(cdbConnStr)
	require.NoError(t, err)
	u.User = url.UserPassword(restrictedUsername, restrictedPassword)
	return u.String()
}

// grantSnapshotPermission adds SELECT ANY TABLE to C##REDPANDA.
// A snapshot needs this grant. It runs a plain SELECT, so it does not
// need LOCK ANY TABLE.
func grantSnapshotPermission(t *testing.T, cdbConnStr string) {
	t.Helper()
	ctx := t.Context()

	cdbDB, err := sql.Open("oracle", sysConnectionString(t, cdbConnStr))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cdbDB.Close()) })
	require.NoError(t, cdbDB.PingContext(ctx))

	_, err = cdbDB.ExecContext(ctx, "GRANT SELECT ANY TABLE TO C##REDPANDA CONTAINER=ALL")
	require.NoError(t, err, "granting SELECT ANY TABLE")
}

func createRestrictedCheckpointCacheTable(t *testing.T, cdbConnStr, pdbName string) {
	t.Helper()
	ctx := t.Context()

	sysDB, err := sql.Open("oracle", sysConnectionString(t, cdbConnStr))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sysDB.Close()) })
	require.NoError(t, sysDB.PingContext(ctx))

	const cpSchema = "C##RPCN"
	tableName := cpSchema + ".CDC_CHECKPOINT_" + strings.ToUpper(pdbName)
	procName := cpSchema + ".CDC_CHECKPOINT_CACHE_UPDATE"

	_, err = sysDB.ExecContext(ctx, fmt.Sprintf(`
	CREATE TABLE %s (
		cache_key VARCHAR2(128) NOT NULL PRIMARY KEY,
		cache_val RAW(8)
	)`, tableName))
	require.NoError(t, err, "creating checkpoint cache table %q", tableName)

	_, err = sysDB.ExecContext(ctx, fmt.Sprintf(`
	CREATE PROCEDURE %s (
		p_key IN VARCHAR2,
		p_value IN RAW
	)
	AS
		v_count NUMBER;
	BEGIN
		SELECT COUNT(*) INTO v_count FROM %s WHERE cache_key = p_key;

		IF v_count > 0 THEN
			UPDATE %s SET cache_val = p_value WHERE cache_key = p_key;
		ELSE
			INSERT INTO %s (cache_key, cache_val) VALUES (p_key, p_value);
		END IF;

		COMMIT;
	END;`, procName, tableName, tableName, tableName))
	require.NoError(t, err, "creating checkpoint cache upsert procedure %q", procName)

	for _, grant := range []string{
		fmt.Sprintf("GRANT SELECT, UPDATE ON %s TO %s", tableName, restrictedUsername),
		fmt.Sprintf("GRANT EXECUTE ON %s TO %s", procName, restrictedUsername),
	} {
		_, err := sysDB.ExecContext(ctx, grant)
		require.NoError(t, err, "executing grant: %s", grant)
	}
}
