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

// restrictedGrants is a least-privilege grant list for streaming. It leaves
// out SELECT ANY TABLE and LOCK ANY TABLE, so the user cannot read table rows.
// FLASHBACK ANY TABLE makes the tables visible in ALL_TABLES without that.
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

// minimalLogMinerGrants is a minimal LogMiner-only grant list. SELECT ANY
// DICTIONARY stands in for the catalog roles and V_$ grants. It gives no table
// visibility, so the tests pair it with a per-table FLASHBACK grant.
var minimalLogMinerGrants = []string{
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

// The connector retries errors quietly, so a passing case could still hide
// one. These must not appear, and neither failure mode may pose as the other.
var (
	streamingLogErrors = []string{"ORA-01031", "ORA-00942", "ORA-41900"}
	snapshotLogErrors  = []string{"no user tables found"}
	noTablesLogErrors  = []string{"ORA-41900"}
)

var (
	streamOps       = []string{"insert", "update", "delete"}
	readOps         = []string{"read", "read", "read"}
	snapshotFailed  = []string{"Snapshotting failed", "ORA-41900"}
	noTablesFailure = []string{"no user tables found"}
)

// TestIntegrationOracleDBCDCWithRestrictedPermissions checks which Oracle grants
// the connector needs. Each subtest runs as a fresh common user with one grant
// set, so grants cannot leak between subtests. Streaming needs only catalog
// visibility of the table, from FLASHBACK ANY TABLE or a per-table FLASHBACK
// grant. A snapshot also needs table read access, and without it the snapshot
// fails and reads no rows.
func TestIntegrationOracleDBCDCWithRestrictedPermissions(t *testing.T) {
	integration.CheckSkip(t)

	noFlashback := without(restrictedGrants, "FLASHBACK ANY TABLE")

	// Not parallel: SetupCDBTestWithPDB shares C##RPCN.

	// Streaming does not need SELECT ANY TABLE.
	t.Run("restricted streaming", func(t *testing.T) {
		env := setupPerms(t, restrictedGrants)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none"})
		assertStreams(t, env, p, streamOps, streamingLogErrors)
	})

	// Without table read access, a snapshot fails and reads no rows.
	t.Run("restricted snapshot blocked", func(t *testing.T) {
		env := setupPerms(t, restrictedGrants)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "snapshot_and_stream"})
		assertBlocked(t, p, snapshotFailed, snapshotLogErrors)
	})

	// Without FLASHBACK ANY TABLE the table is not in ALL_TABLES, so startup
	// finds no tables to capture.
	t.Run("no FLASHBACK ANY TABLE", func(t *testing.T) {
		env := setupPerms(t, noFlashback)
		assertTableVisible(t, env, false)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none"})
		assertBlocked(t, p, noTablesFailure, noTablesLogErrors)
	})

	// A per-table FLASHBACK grant gives visibility without row access, so
	// streaming works with no ANY TABLE privilege.
	t.Run("per-table FLASHBACK streaming", func(t *testing.T) {
		env := setupPerms(t, noFlashback)
		grantTableFlashback(t, env)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none"})
		assertStreams(t, env, p, streamOps, streamingLogErrors)
	})

	// The per-table FLASHBACK grant does not allow a snapshot.
	t.Run("per-table FLASHBACK snapshot blocked", func(t *testing.T) {
		env := setupPerms(t, noFlashback)
		grantTableFlashback(t, env)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "snapshot_and_stream"})
		assertBlocked(t, p, snapshotFailed, snapshotLogErrors)
	})

	// SELECT ANY DICTIONARY exposes DBA_TABLES, but the connector reads
	// ALL_TABLES, so it does not replace FLASHBACK ANY TABLE.
	t.Run("SELECT ANY DICTIONARY instead of FLASHBACK ANY TABLE", func(t *testing.T) {
		env := setupPerms(t, with(noFlashback, "SELECT ANY DICTIONARY"))
		assertTableVisible(t, env, false)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none"})
		assertBlocked(t, p, noTablesFailure, noTablesLogErrors)
	})

	// Control: with SELECT ANY TABLE and LOCK ANY TABLE, the snapshot works.
	t.Run("control snapshot", func(t *testing.T) {
		env := setupPerms(t, with(restrictedGrants, "SELECT ANY TABLE", "LOCK ANY TABLE"))
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "snapshot_and_stream"})
		assertOperations(t, p, readOps, streamingLogErrors)
	})

	// The minimal LogMiner grants plus a per-table FLASHBACK grant can stream.
	t.Run("minimal LogMiner grants", func(t *testing.T) {
		env := setupPerms(t, minimalLogMinerGrants)
		grantTableFlashback(t, env)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none"})
		assertStreams(t, env, p, streamOps, streamingLogErrors)
	})

	// The redo_volume window strategy also queries V$LOG and V$THREAD.
	t.Run("restricted redo_volume streaming", func(t *testing.T) {
		env := setupPerms(t, restrictedGrants)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none", redoVolume: true})
		assertStreams(t, env, p, streamOps, streamingLogErrors)
	})

	// redo_volume with the minimal grants, which have no explicit V_$LOG grant.
	t.Run("minimal LogMiner grants redo_volume", func(t *testing.T) {
		env := setupPerms(t, minimalLogMinerGrants)
		grantTableFlashback(t, env)
		assertTableVisible(t, env, true)

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none", redoVolume: true})
		assertStreams(t, env, p, streamOps, streamingLogErrors)
	})

	// The default Oracle checkpoint cache works with a pre-created table and
	// procedure, without CREATE TABLE or CREATE PROCEDURE on the user.
	t.Run("Oracle checkpoint cache", func(t *testing.T) {
		env := setupPerms(t, restrictedGrants)
		createCheckpointCacheTable(t, env.sysDB, env.pdbName, env.user)
		assertTableVisible(t, env, true)

		var initialSCN uint64
		require.NoError(t, env.sysDB.QueryRowContext(t.Context(), "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&initialSCN))

		p := startPerms(t, env, pipelineOpts{snapshotMode: "none", oracleCache: true})
		assertStreams(t, env, p, streamOps, streamingLogErrors)

		// The checkpoint is written on ack, after consumption, so check it
		// here: EXECUTE on the procedure must be enough to advance it.
		require.NoError(t, p.stream.StopWithin(10*time.Second))
		finalSCN := readCheckpointSCN(t, env.sysDB, "C##RPCN.CDC_CHECKPOINT_"+strings.ToUpper(env.pdbName), "oracledb_cdc")
		assert.Greaterf(t, finalSCN, initialSCN,
			"expected checkpoint SCN (%d) to have advanced beyond initial SCN (%d)", finalSCN, initialSCN)
	})
}

// permsEnv is one subtest's database fixture.
type permsEnv struct {
	cdbConnStr string
	pdbName    string
	user       string // fresh common user under test
	table      string // SCHEMA.MTFOO in the PDB, seeded with rows 1-3
	pdbDB      *oracledbtest.TestDB
	sysDB      *sql.DB
}

// setupPerms creates the common user with grants and the seeded source table.
func setupPerms(t *testing.T, grants []string) *permsEnv {
	t.Helper()
	cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)

	env := &permsEnv{
		cdbConnStr: cdbConnStr,
		pdbName:    pdbName,
		user:       permsUsername(t),
		table:      strings.ToUpper(pdbDB.Schema) + ".MTFOO",
		pdbDB:      pdbDB,
		sysDB:      openDB(t, sysConnectionString(t, cdbConnStr)),
	}
	createCommonUser(t, env.sysDB, env.user, grants)

	require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), env.table,
		fmt.Sprintf("CREATE TABLE %s (id NUMBER PRIMARY KEY, val NUMBER)", env.table)))
	for id := 1; id <= 3; id++ {
		pdbDB.MustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (:1, :2)", env.table), id, id*10)
	}
	return env
}

// grantTableFlashback runs GRANT FLASHBACK ON the table inside the PDB.
func grantTableFlashback(t *testing.T, env *permsEnv) {
	t.Helper()
	grantInPDB(t, env.cdbConnStr, env.pdbName, fmt.Sprintf("GRANT FLASHBACK ON %s TO %s", env.table, env.user))
}

// assertTableVisible separates grant problems from connector problems.
func assertTableVisible(t *testing.T, env *permsEnv, want bool) {
	t.Helper()
	assert.Equalf(t, want, tableVisibleTo(t, env.cdbConnStr, env.pdbName, env.user, env.table),
		"visibility of %s in all_tables for %s", env.table, env.user)
}

type pipelineOpts struct {
	snapshotMode string
	oracleCache  bool // default Oracle checkpoint cache instead of memory
	redoVolume   bool // window_strategy: redo_volume
}

// permsPipeline is a running pipeline with its captured logs and messages.
type permsPipeline struct {
	stream *service.Stream
	logs   *oracledbtest.SyncBuffer
	batch  *oracledbtest.MsgBatch
}

// startPerms runs oracledb_cdc as the env user. The stream stops on cleanup,
// except with oracleCache, where the test stops it before reading the checkpoint.
func startPerms(t *testing.T, env *permsEnv, opts pipelineOpts) *permsPipeline {
	t.Helper()

	cacheLine := "checkpoint_cache: cp"
	if opts.oracleCache {
		cacheLine = ""
	}
	windowLines := "    scn_window_size: 20000"
	if opts.redoVolume {
		// redo_volume rejects a non-default scn_window_size.
		windowLines = "    window_strategy: redo_volume\n    redo_volume_min: 2\n    redo_volume_growth_max: 4"
	}
	cfg := `
oracledb_cdc:
  connection_string: ` + userConnectionString(t, env.cdbConnStr, env.user) + `
  pdb_name: ` + env.pdbName + `
  snapshot_mode: ` + opts.snapshotMode + `
  ` + cacheLine + `
  logminer:
` + windowLines + `
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + env.table + `"]
  batching:
    count: 1`

	p := &permsPipeline{logs: &oracledbtest.SyncBuffer{}, batch: &oracledbtest.MsgBatch{}}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, p.logs), &slog.HandlerOptions{Level: slog.LevelInfo}))
	p.stream = oracledbtest.StartPipelineWithBuilder(t, cfg, p.batch.Consumer(), func(sb *service.StreamBuilder) {
		sb.SetLogger(logger)
		if !opts.oracleCache {
			// Keeps the checkpoint table out of the grant results.
			require.NoError(t, sb.AddCacheYAML("label: cp\nmemory: {}"))
		}
	})
	if !opts.oracleCache {
		t.Cleanup(func() { require.NoError(t, p.stream.StopWithin(10*time.Second)) })
	}
	return p
}

// assertStreams waits for streaming to start, runs insert/update/delete
// against the table, and checks the operations captured.
func assertStreams(t *testing.T, env *permsEnv, p *permsPipeline, wantOps, notWantLogs []string) {
	t.Helper()
	oracledbtest.WaitForStreaming(t, p.logs)
	env.pdbDB.MustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (4, 40)", env.table))
	env.pdbDB.MustExec(fmt.Sprintf("UPDATE %s SET val = 99 WHERE id = 1", env.table))
	env.pdbDB.MustExec(fmt.Sprintf("DELETE FROM %s WHERE id = 4", env.table))
	assertOperations(t, p, wantOps, notWantLogs)
}

// assertOperations waits for len(wantOps) messages and compares their
// operation metadata, in any order. It then checks the logs for errors.
func assertOperations(t *testing.T, p *permsPipeline, wantOps, notWantLogs []string) {
	t.Helper()
	oracledbtest.WaitForCount(t, p.batch.Count, len(wantOps), 2*time.Minute)
	got := make([]string, 0, len(wantOps))
	for _, msg := range p.batch.Clone() {
		op, ok := msg.MetaGet("operation")
		assert.True(t, ok, "message missing 'operation' metadata")
		got = append(got, op)
	}
	slices.Sort(got)
	want := slices.Clone(wantOps)
	slices.Sort(want)
	require.Equal(t, want, got)
	assertLogsExclude(t, p.logs, notWantLogs)
}

// assertBlocked waits for every wantLogs entry, then checks that no message
// was read.
func assertBlocked(t *testing.T, p *permsPipeline, wantLogs, notWantLogs []string) {
	t.Helper()
	// Connect errors are retried, not returned, so logs are the only signal.
	for _, want := range wantLogs {
		require.Eventuallyf(t, func() bool { return strings.Contains(p.logs.String(), want) },
			2*time.Minute, 500*time.Millisecond, "logs never contained %q", want)
	}
	time.Sleep(5 * time.Second)
	assert.Zero(t, p.batch.Count(), "no messages must be read")
	assertLogsExclude(t, p.logs, notWantLogs)
}

func assertLogsExclude(t *testing.T, logs *oracledbtest.SyncBuffer, notWant []string) {
	t.Helper()
	for _, bad := range notWant {
		assert.NotContains(t, logs.String(), bad)
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
