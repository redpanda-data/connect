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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
)

const permsPassword = "Perms_pw_123"

// restrictedGrants mirrors oracle-cdc-perms-testkit/sql/10-cdb-restricted-user.sql:
// no SELECT ANY TABLE or LOCK ANY TABLE. SetupCDBTestWithPDB runs its ALTER
// DATABASE statements.
var restrictedGrants = []string{
	"CREATE SESSION",
	"SET CONTAINER",
	"SELECT ON V_$DATABASE",
	"FLASHBACK ANY TABLE",
	"SELECT_CATALOG_ROLE",
	"EXECUTE_CATALOG_ROLE",
	"SELECT ANY TRANSACTION",
	"LOGMINING",
	"CREATE TABLE",
	"CREATE SEQUENCE",
	"UNLIMITED TABLESPACE",
	"EXECUTE ON DBMS_LOGMNR",
	"EXECUTE ON DBMS_LOGMNR_D",
	"SELECT ON V_$LOG",
	"SELECT ON V_$LOG_HISTORY",
	"SELECT ON V_$LOGMNR_LOGS",
	"SELECT ON V_$LOGMNR_CONTENTS",
	"SELECT ON V_$LOGMNR_PARAMETERS",
	"SELECT ON V_$LOGFILE",
	"SELECT ON V_$ARCHIVED_LOG",
	"SELECT ON V_$ARCHIVE_DEST_STATUS",
	"SELECT ON V_$TRANSACTION",
	"SELECT ON V_$MYSTAT",
	"SELECT ON V_$STATNAME",
}

// logminerDocGrants is the LogMiner-only route from the draft permissions doc
// (sql/13-cdb-logminer-doc-user.sql). SELECT ANY DICTIONARY stands in for the
// catalog roles and V_$ grants.
var logminerDocGrants = []string{
	"CREATE SESSION",
	"SET CONTAINER",
	"LOGMINING",
	"SELECT ANY DICTIONARY",
	"EXECUTE ON DBMS_LOGMNR",
	"EXECUTE ON DBMS_LOGMNR_D",
}

// without returns grants with the given privileges removed.
func without(grants []string, drop ...string) []string {
	out := make([]string, 0, len(grants))
	for _, g := range grants {
		if !slices.Contains(drop, g) {
			out = append(out, g)
		}
	}
	return out
}

// with returns grants with the given privileges added.
func with(grants []string, add ...string) []string {
	return append(slices.Clone(grants), add...)
}

// renderGrant returns a common-user GRANT for privilege.
func renderGrant(privilege, user string) string {
	return fmt.Sprintf("GRANT %s TO %s CONTAINER=ALL", privilege, user)
}

type permissionsCase struct {
	name          string
	grants        []string
	pdbTableGrant bool // GRANT FLASHBACK ON the table in the PDB
	snapshotMode  string
	oracleCache   bool // default Oracle checkpoint cache instead of memory
	visible       bool // table expected in the user's all_tables

	dml         func(db *oracledbtest.TestDB, table string) // run once streaming starts
	wantOps     []string                                    // operations, any order
	wantLogs    []string                                    // must appear; implies no messages
	notWantLogs []string
}

// The connector retries errors quietly, so a passing case could still hide
// one. These must not appear, and neither failure mode may pose as the other.
var (
	streamingLogErrors = []string{"ORA-01031", "ORA-00942", "ORA-41900"}
	snapshotLogErrors  = []string{"no user tables found"}
	noTablesLogErrors  = []string{"ORA-41900"}
)

// TestIntegrationOracleDBCDCWithRestrictedPermissions encodes the permission
// matrix in oracle-cdc-perms-testkit/README.md. Each case gets a fresh user so
// grants cannot leak between cases.
func TestIntegrationOracleDBCDCWithRestrictedPermissions(t *testing.T) {
	integration.CheckSkip(t)

	streamOps := []string{"insert", "update", "delete"}
	// streamDML produces streamOps. It relies on the seed rows.
	streamDML := func(db *oracledbtest.TestDB, table string) {
		db.MustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (4, 40)", table))
		db.MustExec(fmt.Sprintf("UPDATE %s SET val = 99 WHERE id = 1", table))
		db.MustExec(fmt.Sprintf("DELETE FROM %s WHERE id = 4", table))
	}
	readOps := []string{"read", "read", "read"}
	noFlashback := without(restrictedGrants, "FLASHBACK ANY TABLE")

	// Not parallel: SetupCDBTestWithPDB shares C##RPCN.
	for _, test := range []permissionsCase{
		{
			// README #1: SELECT ANY TABLE is not needed to stream.
			name:         "restricted streaming",
			grants:       restrictedGrants,
			snapshotMode: "none",
			visible:      true,
			dml:          streamDML,
			wantOps:      streamOps,
			notWantLogs:  streamingLogErrors,
		},
		{
			// README #2: snapshot fails loudly, reads no rows.
			name:         "restricted snapshot blocked",
			grants:       restrictedGrants,
			snapshotMode: "snapshot_and_stream",
			visible:      true,
			wantLogs:     []string{"Snapshotting failed", "ORA-41900"},
			notWantLogs:  snapshotLogErrors,
		},
		{
			// README #3: table drops out of ALL_TABLES, so startup fails.
			name:         "no FLASHBACK ANY TABLE",
			grants:       noFlashback,
			snapshotMode: "none",
			visible:      false,
			wantLogs:     []string{"no user tables found"},
			notWantLogs:  noTablesLogErrors,
		},
		{
			// README #4: per-table FLASHBACK gives visibility but no row read.
			name:          "per-table FLASHBACK streaming",
			grants:        noFlashback,
			pdbTableGrant: true,
			snapshotMode:  "none",
			visible:       true,
			dml:           streamDML,
			wantOps:       streamOps,
			notWantLogs:   streamingLogErrors,
		},
		{
			// README #4b: same profile, snapshot still blocked.
			name:          "per-table FLASHBACK snapshot blocked",
			grants:        noFlashback,
			pdbTableGrant: true,
			snapshotMode:  "snapshot_and_stream",
			visible:       true,
			wantLogs:      []string{"Snapshotting failed", "ORA-41900"},
			notWantLogs:   snapshotLogErrors,
		},
		{
			// README #5: exposes DBA_TABLES only; the connector reads ALL_TABLES.
			name:         "SELECT ANY DICTIONARY instead of FLASHBACK ANY TABLE",
			grants:       with(noFlashback, "SELECT ANY DICTIONARY"),
			snapshotMode: "none",
			visible:      false,
			wantLogs:     []string{"no user tables found"},
			notWantLogs:  noTablesLogErrors,
		},
		{
			// README C2: known-good baseline, full snapshot.
			name:         "control snapshot",
			grants:       with(restrictedGrants, "SELECT ANY TABLE", "LOCK ANY TABLE"),
			snapshotMode: "snapshot_and_stream",
			visible:      true,
			wantOps:      readOps,
			notWantLogs:  streamingLogErrors,
		},
		{
			// README joedoc: doc's LogMiner-only route plus per-table FLASHBACK.
			name:          "LogMiner doc route",
			grants:        logminerDocGrants,
			pdbTableGrant: true,
			snapshotMode:  "none",
			visible:       true,
			dml:           streamDML,
			wantOps:       streamOps,
			notWantLogs:   streamingLogErrors,
		},
		{
			// README caveat: the Oracle checkpoint cache, with its table and
			// procedure pre-created.
			name:         "Oracle checkpoint cache",
			grants:       restrictedGrants,
			snapshotMode: "none",
			oracleCache:  true,
			visible:      true,
			dml:          streamDML,
			wantOps:      streamOps,
			notWantLogs:  streamingLogErrors,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runPermissionsCase(t, test)
		})
	}
}

func runPermissionsCase(t *testing.T, test permissionsCase) {
	cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)
	ctx := t.Context()

	user := permsUsername(t)
	table := strings.ToUpper(pdbDB.Schema) + ".MTFOO"
	sysDB := openDB(t, sysConnectionString(t, cdbConnStr))

	createCommonUser(t, sysDB, user, test.grants)

	require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(ctx, table,
		fmt.Sprintf("CREATE TABLE %s (id NUMBER PRIMARY KEY, val NUMBER)", table)))
	for id := 1; id <= 3; id++ {
		pdbDB.MustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (:1, :2)", table), id, id*10)
	}

	if test.pdbTableGrant {
		grantInPDB(t, cdbConnStr, pdbName, fmt.Sprintf("GRANT FLASHBACK ON %s TO %s", table, user))
	}
	if test.oracleCache {
		createCheckpointCacheTable(t, sysDB, pdbName, user)
	}

	var initialSCN uint64
	require.NoError(t, sysDB.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&initialSCN))

	// Separates grant problems from connector problems.
	assert.Equalf(t, test.visible, tableVisibleTo(t, cdbConnStr, pdbName, user, table),
		"visibility of %s in all_tables for %s", table, user)

	cacheLine := "checkpoint_cache: cp"
	if test.oracleCache {
		cacheLine = ""
	}
	cfg := `
oracledb_cdc:
  connection_string: ` + userConnectionString(t, cdbConnStr, user) + `
  pdb_name: ` + pdbName + `
  snapshot_mode: ` + test.snapshotMode + `
  ` + cacheLine + `
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + table + `"]
  batching:
    count: 1`

	var (
		logs  oracledbtest.SyncBuffer
		batch oracledbtest.MsgBatch
	)
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, &logs), &slog.HandlerOptions{Level: slog.LevelInfo}))
	stream := oracledbtest.StartPipelineWithBuilder(t, cfg, batch.Consumer(), func(sb *service.StreamBuilder) {
		sb.SetLogger(logger)
		if !test.oracleCache {
			// Keeps the checkpoint table out of the grant results.
			require.NoError(t, sb.AddCacheYAML("label: cp\nmemory: {}"))
		}
	})
	// The Oracle cache case stops the stream itself before reading the checkpoint.
	if !test.oracleCache {
		t.Cleanup(func() { require.NoError(t, stream.StopWithin(10*time.Second)) })
	}

	assertNotWantLogs := func() {
		for _, bad := range test.notWantLogs {
			assert.NotContains(t, logs.String(), bad)
		}
	}

	if len(test.wantLogs) > 0 {
		// Connect errors are retried, not returned, so logs are the only signal.
		for _, want := range test.wantLogs {
			require.Eventuallyf(t, func() bool { return strings.Contains(logs.String(), want) },
				2*time.Minute, 500*time.Millisecond, "logs never contained %q", want)
		}
		time.Sleep(5 * time.Second)
		assert.Zero(t, batch.Count(), "no messages must be read")
		assertNotWantLogs()
		return
	}

	if test.dml != nil {
		oracledbtest.WaitForStreaming(t, &logs)
		test.dml(pdbDB, table)
	}

	oracledbtest.WaitForCount(t, batch.Count, len(test.wantOps), 2*time.Minute)
	got := make([]string, 0, len(test.wantOps))
	for _, msg := range batch.Clone() {
		op, ok := msg.MetaGet("operation")
		assert.True(t, ok, "message missing 'operation' metadata")
		got = append(got, op)
	}
	slices.Sort(got)
	want := slices.Clone(test.wantOps)
	slices.Sort(want)
	require.Equal(t, want, got)
	assertNotWantLogs()

	if test.oracleCache {
		require.NoError(t, stream.StopWithin(10*time.Second))

		// The checkpoint is written on ack, after consumption, so check it
		// here: EXECUTE on the procedure must be enough to advance it.
		finalSCN := readCheckpointSCN(t, sysDB, "C##RPCN.CDC_CHECKPOINT_"+strings.ToUpper(pdbName), "oracledb_cdc")
		assert.Greaterf(t, finalSCN, initialSCN,
			"expected checkpoint SCN (%d) to have advanced beyond initial SCN (%d)", finalSCN, initialSCN)
	}
}

// readCheckpointSCN returns the SCN stored under key.
func readCheckpointSCN(t *testing.T, db *sql.DB, table, key string) uint64 {
	t.Helper()
	var val []byte
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT cache_val FROM %s WHERE cache_key = :1", table), key).Scan(&val))
	return binary.LittleEndian.Uint64(val)
}

// permsUsername returns a common user name unique to the test.
func permsUsername(t *testing.T) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name()))
	return fmt.Sprintf("C##PERM_%08X", h.Sum32())
}

// openDB opens a pool that closes when the test ends.
func openDB(t *testing.T, connStr string) *sql.DB {
	t.Helper()
	db, err := sql.Open("oracle", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

// sysConnectionString swaps the user in cdbConnStr for SYS (SYSDBA in go-ora).
// SYSTEM cannot grant on SYS-owned views such as V_$DATABASE.
func sysConnectionString(t *testing.T, cdbConnStr string) string {
	t.Helper()

	u, err := url.Parse(cdbConnStr)
	require.NoError(t, err)
	password, _ := u.User.Password()
	u.User = url.UserPassword("sys", password)
	return u.String()
}

// userConnectionString swaps the user in cdbConnStr for user, escaping C##.
func userConnectionString(t *testing.T, cdbConnStr, user string) string {
	t.Helper()

	u, err := url.Parse(cdbConnStr)
	require.NoError(t, err)
	u.User = url.UserPassword(user, permsPassword)
	return u.String()
}

// createCommonUser recreates user with privileges and drops it when the test
// ends.
func createCommonUser(t *testing.T, sysDB *sql.DB, user string, privileges []string) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, oracledbtest.DropUser(ctx, sysDB.ExecContext, user), "dropping stale user %s", user)

	// Registered first so it runs after the stream stops, and drops the user
	// even if a grant fails. Retry ORA-01940 while connector sessions close.
	t.Cleanup(func() {
		var err error
		for range 10 {
			if err = oracledbtest.DropUser(context.Background(), sysDB.ExecContext, user); err == nil {
				return
			}
			time.Sleep(time.Second)
		}
		t.Errorf("dropping user %s: %v", user, err)
	})

	_, err := sysDB.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY \"%s\" CONTAINER=ALL", user, permsPassword))
	require.NoError(t, err, "creating user %s", user)
	for _, p := range privileges {
		_, err := sysDB.ExecContext(ctx, renderGrant(p, user))
		require.NoError(t, err, "executing grant: %s", renderGrant(p, user))
	}
}

// grantInPDB runs stmt as SYS in the PDB, on a pinned connection because
// ALTER SESSION SET CONTAINER is per session.
func grantInPDB(t *testing.T, cdbConnStr, pdbName, stmt string) {
	t.Helper()
	db, err := sql.Open("oracle", sysConnectionString(t, cdbConnStr))
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(t.Context(), "ALTER SESSION SET CONTAINER = "+pdbName)
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), stmt)
	require.NoError(t, err, "executing in %s: %s", pdbName, stmt)
}

// tableVisibleTo reports whether user sees fullTable in the PDB's all_tables.
func tableVisibleTo(t *testing.T, cdbConnStr, pdbName, user, fullTable string) bool {
	t.Helper()
	owner, name, _ := strings.Cut(fullTable, ".")

	db, err := sql.Open("oracle", userConnectionString(t, cdbConnStr, user))
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(t.Context(), "ALTER SESSION SET CONTAINER = "+pdbName)
	require.NoError(t, err)

	var count int
	require.NoError(t, conn.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2", owner, name).Scan(&count))
	return count > 0
}

// createCheckpointCacheTable pre-creates the default checkpoint table and
// upsert procedure and grants user access, so the connector needs neither
// CREATE TABLE nor CREATE PROCEDURE.
func createCheckpointCacheTable(t *testing.T, sysDB *sql.DB, pdbName, user string) {
	t.Helper()
	ctx := t.Context()

	const cpSchema = "C##RPCN"
	tableName := cpSchema + ".CDC_CHECKPOINT_" + strings.ToUpper(pdbName)
	procName := cpSchema + ".CDC_CHECKPOINT_CACHE_UPDATE"

	_, err := sysDB.ExecContext(ctx, fmt.Sprintf(`
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
		fmt.Sprintf("GRANT SELECT, UPDATE ON %s TO %s", tableName, user),
		fmt.Sprintf("GRANT EXECUTE ON %s TO %s", procName, user),
	} {
		_, err := sysDB.ExecContext(ctx, grant)
		require.NoError(t, err, "executing grant: %s", grant)
	}
}
