// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chunkedTableRows is large enough that, with statistics gathered, the table spans
// hundreds of blocks and keyspace sampling returns thousands of keys, so a snapshot with
// chunkedWorkers workers reliably splits it into more than one range. It also allows 10
// ranges of minRowsPerChunk rows, enough for the outlier key to leave most of them empty.
const (
	chunkedTableRows = 100000
	chunkedWorkers   = 4

	// unenforcedNullKeyRows and unenforcedDuplicateKeyRows are the extra rows added to
	// chunk_unenforced_key_test that its unenforced primary key allows: rows with a NULL
	// key, and rows repeating an existing key.
	unenforcedNullKeyRows      = 10
	unenforcedDuplicateKeyRows = 10

	// outlierKey is the sentinel key added to chunk_outlier_key_test, far beyond its other
	// keys 1 to chunkedTableRows.
	outlierKey = 999999999999
)

func TestIntegrationSnapshotChunked(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)

	// Create and populate every table upfront: AS OF SCN reads raise ORA-01466 against a
	// table whose definition changed after the SCN, so DDL has to settle before Prepare.
	t.Log("Given tables populated with enough rows to be split into chunks")
	{
		createChunkedTable(t, db, "chunk_single_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_single_key_test (
			id   NUMBER(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_single_key_test (id, data) SELECT LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// A low-cardinality leading column means chunk bounds fall in the middle of a col1
		// prefix, exercising the full lexicographic expansion of the composite-key bounds.
		createChunkedTable(t, db, "chunk_composite_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_composite_key_test (
			col1 NUMBER(10) NOT NULL,
			col2 NUMBER(10) NOT NULL,
			data NVARCHAR2(100),
			CONSTRAINT chunk_composite_key_test_pk PRIMARY KEY (col1, col2)
		)`,
			"INSERT INTO "+db.Schema+".chunk_composite_key_test (col1, col2, data) SELECT MOD(LEVEL, 4), LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// Fractional-second TIMESTAMP keys depend on the session TIME_ZONE when bound back
		// into the keyset predicate, so this catches any page that runs on a connection
		// without the snapshot's NLS settings.
		createChunkedTable(t, db, "chunk_timestamp_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_timestamp_key_test (
			ts   TIMESTAMP(6) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_timestamp_key_test (ts, data) SELECT TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(LEVEL * 1.5, 'SECOND'), RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		createChunkedTable(t, db, "chunk_date_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_date_key_test (
			dt   DATE NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_date_key_test (dt, data) SELECT DATE '2024-01-01' + LEVEL * 2 / 86400, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// One sentinel key far beyond the rest leaves almost every equal-width interpolated
		// range empty, so the key range is sampled instead.
		createChunkedTable(t, db, "chunk_outlier_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_outlier_key_test (
			id   NUMBER(15) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_outlier_key_test (id, data) SELECT LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// String keys can't be interpolated between MIN and MAX, so they're sampled.
		createChunkedTable(t, db, "chunk_string_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_string_key_test (
			id   VARCHAR2(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_string_key_test (id, data) SELECT LPAD(LEVEL, 10, '0'), RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// Lower-case and upper-case prefixes sort in a different order under binary and
		// linguistic collations: binary puts 'B' before 'a', GERMAN puts 'a' first.
		createChunkedTable(t, db, "chunk_mixed_case_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_mixed_case_key_test (
			id   VARCHAR2(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_mixed_case_key_test (id, data) SELECT CASE WHEN MOD(LEVEL, 2) = 0 THEN 'a' ELSE 'B' END || LPAD(LEVEL, 9, '0'), RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		// Without statistics a string key can't be sampled, but a numeric key can still be
		// interpolated, since that only needs its MIN and MAX.
		createChunkedTable(t, db, "chunk_no_stats_test", `
		CREATE TABLE `+db.Schema+`.chunk_no_stats_test (
			id   VARCHAR2(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_no_stats_test (id, data) SELECT LPAD(LEVEL, 10, '0'), RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
		createChunkedTable(t, db, "chunk_no_stats_numeric_test", `
		CREATE TABLE `+db.Schema+`.chunk_no_stats_numeric_test (
			id   NUMBER(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_no_stats_numeric_test (id, data) SELECT LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)

		// A RELY DISABLE NOVALIDATE primary key, as is common in warehouse schemas, is declared
		// but not enforced, so the table can hold rows with NULL and duplicate keys.
		createChunkedTable(t, db, "chunk_unenforced_key_test", `
		CREATE TABLE `+db.Schema+`.chunk_unenforced_key_test (
			id   NUMBER(10),
			data NVARCHAR2(100)
		)`,
			"INSERT INTO "+db.Schema+".chunk_unenforced_key_test (id, data) SELECT LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1",
		)
	}
	db.MustExec("INSERT INTO "+db.Schema+".chunk_unenforced_key_test (id, data) SELECT NULL, 'null-key' FROM dual CONNECT BY LEVEL <= :1", unenforcedNullKeyRows)
	db.MustExec("INSERT INTO "+db.Schema+".chunk_unenforced_key_test (id, data) SELECT LEVEL * 1000, 'duplicate-key' FROM dual CONNECT BY LEVEL <= :1", unenforcedDuplicateKeyRows)
	db.MustExec("ALTER TABLE " + db.Schema + ".chunk_unenforced_key_test ADD CONSTRAINT chunk_unenforced_key_test_pk PRIMARY KEY (id) RELY DISABLE NOVALIDATE")
	db.MustExec("INSERT INTO "+db.Schema+".chunk_outlier_key_test (id, data) VALUES (:1, 'sentinel')", outlierKey)

	for _, table := range []string{"chunk_single_key_test", "chunk_composite_key_test", "chunk_timestamp_key_test", "chunk_date_key_test", "chunk_string_key_test", "chunk_mixed_case_key_test", "chunk_unenforced_key_test", "chunk_outlier_key_test"} {
		db.MustExec(fmt.Sprintf("BEGIN DBMS_STATS.GATHER_TABLE_STATS('%s', '%s'); END;", db.Schema, table))
	}
	// Delete explicitly rather than relying on statistics never having been gathered, so
	// the fallback is exercised even if the database collects them automatically.
	// Lock them too, so an automatic statistics job can't gather them mid-test.
	for _, table := range []string{"CHUNK_NO_STATS_TEST", "CHUNK_NO_STATS_NUMERIC_TEST"} {
		db.MustExec(fmt.Sprintf("BEGIN DBMS_STATS.DELETE_TABLE_STATS('%s', '%s'); END;", db.Schema, table))
		db.MustExec(fmt.Sprintf("BEGIN DBMS_STATS.LOCK_TABLE_STATS('%s', '%s'); END;", db.Schema, table))
	}

	waitForFlashbackReads(t, db,
		"chunk_single_key_test", "chunk_composite_key_test", "chunk_timestamp_key_test", "chunk_date_key_test",
		"chunk_string_key_test", "chunk_mixed_case_key_test", "chunk_no_stats_test", "chunk_no_stats_numeric_test",
		"chunk_unenforced_key_test", "chunk_outlier_key_test")

	t.Run("SinglePrimaryKey", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_SINGLE_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		scn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When the table is changed after the snapshot SCN is captured")
		db.MustExec("INSERT INTO "+db.Schema+".chunk_single_key_test (id, data) SELECT :1 + LEVEL, 'added' FROM dual CONNECT BY LEVEL <= 100", chunkedTableRows)
		db.MustExec("DELETE FROM " + db.Schema + ".chunk_single_key_test WHERE id <= 50")
		db.MustExec("UPDATE " + db.Schema + ".chunk_single_key_test SET data = 'changed' WHERE id = 100")

		t.Log("And the snapshot is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then the table is split into chunks that together return every row exactly once, as of the snapshot SCN")
		assertSplitIntoChunks(t, logs, table, planInterpolated)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, strconv.Itoa)
		for i, msg := range publisher.messagesCopy() {
			assert.Equalf(t, scn, msg.SCN, "Expected snapshot message[%d] to carry the captured SCN", i)
			if row := msg.Data.(map[string]any); row["ID"] == int64(100) {
				assert.NotEqual(t, "changed", row["DATA"], "Expected the row as of the snapshot SCN, not the later update")
			}
		}
	})

	t.Run("CompositeKeyWithLowCardinalityPrefix", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_COMPOSITE_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When the snapshot is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 333))

		t.Log("Then chunks bounded within a shared col1 prefix return every row exactly once")
		assertSplitIntoChunks(t, logs, table, planSampled)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprintf("%v/%v", row["COL1"], row["COL2"])
		}, func(i int) string {
			return fmt.Sprintf("%d/%d", i%4, i)
		})
	})

	t.Run("TimestampPrimaryKey", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_TIMESTAMP_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When the snapshot is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then every TIMESTAMP key is returned exactly once")
		assertSplitIntoChunks(t, logs, table, planInterpolated)
		base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			ts, ok := row["TS"].(time.Time)
			require.Truef(t, ok, "Expected TS to be a time.Time, got %T", row["TS"])
			return strconv.FormatInt(ts.Sub(base).Milliseconds(), 10)
		}, func(i int) string {
			return strconv.Itoa(i * 1500)
		})
	})

	t.Run("DatePrimaryKey", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_DATE_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When the snapshot is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then the DATE key range is interpolated and every key is returned exactly once")
		assertSplitIntoChunks(t, logs, table, planInterpolated)
		base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			dt, ok := row["DT"].(time.Time)
			require.Truef(t, ok, "Expected DT to be a time.Time, got %T", row["DT"])
			return strconv.FormatInt(int64(dt.Sub(base).Seconds()), 10)
		}, func(i int) string {
			return strconv.Itoa(i * 2)
		})
	})

	t.Run("OutlierKeyIsSampled", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_OUTLIER_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a numeric key with one far outlier is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then the mostly empty interpolated ranges are rejected, the key range is sampled, and every key is returned exactly once")
		assert.Contains(t, logs.String(), "interpolated key ranges of table '"+table.FullName()+"' hold rows")
		assertSplitIntoChunks(t, logs, table, planSampled)
		assertEveryKeyOnce(t, publisher, chunkedTableRows+1, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, func(i int) string {
			if i == chunkedTableRows+1 {
				return strconv.Itoa(outlierKey)
			}
			return strconv.Itoa(i)
		})
	})

	t.Run("StringPrimaryKeyIsSampled", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_STRING_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When the snapshot is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then the string key range is sampled and every key is returned exactly once")
		assertSplitIntoChunks(t, logs, table, planSampled)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, paddedKey)
	})

	t.Run("LinguisticSessionSortStillReturnsEveryKeyOnce", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_MIXED_CASE_KEY_TEST"}
		user := db.Schema + "_NLS"
		userConnStr := createSelectOnlyUser(t, db, connStr, user, table,
			"GRANT FLASHBACK ON "+table.FullName()+" TO "+user,
			"CREATE OR REPLACE TRIGGER "+user+".nls_german_trg AFTER LOGON ON "+user+".SCHEMA BEGIN EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_SORT = GERMAN'; END;",
		)
		snapshot, publisher, logs := newChunkedSnapshot(t, userConnStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a user whose logon trigger sets a linguistic NLS_SORT reads a mixed-case string key with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then pages and chunk bounds still compare in binary order, and every key is returned exactly once")
		assertSplitIntoChunks(t, logs, table, planSampled)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, mixedCaseKey)
	})

	t.Run("NumericKeyWithoutStatisticsIsInterpolated", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_NO_STATS_NUMERIC_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a table with a numeric key and no optimizer statistics is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then it is still chunked, and every row is returned exactly once")
		assertSplitIntoChunks(t, logs, table, planInterpolated)
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, strconv.Itoa)
	})

	t.Run("UnenforcedPrimaryKeyFallsBackToFullScan", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_UNENFORCED_KEY_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a table whose primary key is not enforced is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then it is not chunked, and rows with NULL and duplicate keys are still returned")
		assert.Contains(t, logs.String(), "is not enabled and validated")
		assert.NotRegexp(t, chunkedLogPattern(table), logs.String())
		assert.Equal(t, chunkedTableRows+unenforcedNullKeyRows+unenforcedDuplicateKeyRows, publisher.count())

		var nullKeys, duplicateKeys int
		for _, msg := range publisher.messagesCopy() {
			row, ok := msg.Data.(map[string]any)
			require.Truef(t, ok, "Expected message data to be a map[string]any, got %T", msg.Data)
			switch row["DATA"] {
			case "null-key":
				nullKeys++
				assert.Nil(t, row["ID"])
			case "duplicate-key":
				duplicateKeys++
			}
		}
		assert.Equal(t, unenforcedNullKeyRows, nullKeys, "Expected every row with a NULL key")
		assert.Equal(t, unenforcedDuplicateKeyRows, duplicateKeys, "Expected every row with a duplicate key")
	})

	t.Run("NoStatisticsFallsBackToFullScan", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_NO_STATS_TEST"}
		snapshot, publisher, logs := newChunkedSnapshot(t, connStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a table with a string key and no optimizer statistics is read with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then it is not chunked, but every row is still returned exactly once")
		assert.Contains(t, logs.String(), "has no optimizer statistics")
		assert.NotRegexp(t, chunkedLogPattern(table), logs.String())
		assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
			return fmt.Sprint(row["ID"])
		}, paddedKey)
	})

	t.Run("MissingFlashbackPrivilegeFallsBackToFullScan", func(t *testing.T) {
		table := replication.UserTable{Schema: db.Schema, Name: "CHUNK_TIMESTAMP_KEY_TEST"}
		userConnStr := createSelectOnlyUser(t, db, connStr, db.Schema+"_NOFB", table)
		snapshot, publisher, logs := newChunkedSnapshot(t, userConnStr, "", table)

		_, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)

		t.Log("When a user with SELECT but not FLASHBACK on the table reads it with multiple workers")
		require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

		t.Log("Then it is not chunked, a warning names the missing grant, and every row is still returned")
		assert.Contains(t, logs.String(), "can't be split into parallel chunks without the FLASHBACK privilege")
		assert.NotRegexp(t, chunkedLogPattern(table), logs.String())
		assert.Len(t, publisher.messagesCopy(), chunkedTableRows)
	})
}

// createSelectOnlyUser creates a user that can log in, read the data dictionary and SELECT
// from table, but has no FLASHBACK privilege unless extra grants it, and returns connStr with
// its credentials. extra are further statements to run as the admin user, such as grants or a
// logon trigger for the new user.
func createSelectOnlyUser(t *testing.T, db *oracledbtest.TestDB, connStr, user string, table replication.UserTable, extra ...string) string {
	t.Helper()
	const password = "testdb123"

	// Local users in CDB$ROOT need _ORACLE_SCRIPT, a session setting, so use one connection.
	exec := func(ctx context.Context, queries ...string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`); err != nil {
			return err
		}
		for _, q := range queries {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
		return nil
	}
	dropUser := func(ctx context.Context) error {
		err := exec(ctx, "DROP USER "+user+" CASCADE")
		if err != nil && strings.Contains(err.Error(), "ORA-01918") {
			return nil
		}
		return err
	}

	require.NoError(t, dropUser(t.Context()))
	require.NoError(t, exec(t.Context(), append([]string{
		"CREATE USER " + user + " IDENTIFIED BY " + password,
		"GRANT CREATE SESSION, SELECT ANY DICTIONARY TO " + user,
		"GRANT SELECT ON " + table.FullName() + " TO " + user,
	}, extra...)...))
	t.Cleanup(func() {
		// t.Context() is cancelled before cleanup runs.
		if err := dropUser(context.Background()); err != nil {
			t.Errorf("dropping user %s: %v", user, err)
		}
	})

	u, err := url.Parse(connStr)
	require.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}

func TestIntegrationSnapshotChunkedCDB(t *testing.T) {
	integration.CheckSkip(t)

	connStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)

	t.Log("Given a PDB table populated with enough rows to be split into chunks")
	require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), pdbDB.Schema+".chunk_pdb_test", `
		CREATE TABLE `+pdbDB.Schema+`.chunk_pdb_test (
			id   NUMBER(10) NOT NULL PRIMARY KEY,
			data NVARCHAR2(100)
		)`))
	pdbDB.MustExec("INSERT INTO "+pdbDB.Schema+".chunk_pdb_test (id, data) SELECT LEVEL, RPAD('x', 100, 'x') FROM dual CONNECT BY LEVEL <= :1", chunkedTableRows)
	pdbDB.MustExec(fmt.Sprintf("BEGIN DBMS_STATS.GATHER_TABLE_STATS('%s', 'CHUNK_PDB_TEST'); END;", pdbDB.Schema))

	waitForFlashbackReads(t, pdbDB, "chunk_pdb_test")

	table := replication.UserTable{Schema: pdbDB.Schema, Name: "CHUNK_PDB_TEST"}
	snapshot, publisher, logs := newChunkedSnapshot(t, connStr, pdbName, table)

	_, err := snapshot.Prepare(t.Context())
	require.NoError(t, err)

	t.Log("When the snapshot is read from CDB$ROOT with multiple workers")
	require.NoError(t, snapshot.Read(t.Context(), chunkedWorkers, 500))

	t.Log("Then each chunk reads the PDB table and every row is returned exactly once")
	assertSplitIntoChunks(t, logs, table, planInterpolated)
	assertEveryKeyOnce(t, publisher, chunkedTableRows, func(row map[string]any) string {
		return fmt.Sprint(row["ID"])
	}, strconv.Itoa)
}

// createChunkedTable creates table in db.Schema and fills it with chunkedTableRows rows
// using insertQuery, which takes the row count as its only bind parameter.
func createChunkedTable(t *testing.T, db *oracledbtest.TestDB, table, createQuery, insertQuery string) {
	t.Helper()
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), db.Schema+"."+table, createQuery))
	db.MustExec(insertQuery, chunkedTableRows)
}

// waitForFlashbackReads waits until each table in db.Schema can be read AS OF the current
// SCN. Oracle raises ORA-01466 for a flashback read at an SCN too close to the table's
// CREATE TABLE, so a snapshot taken straight after creating the tables can fail.
func waitForFlashbackReads(t *testing.T, db *oracledbtest.TestDB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		require.EventuallyWithTf(t, func(c *assert.CollectT) {
			var scn uint64
			require.NoError(c, db.QueryRowContext(t.Context(), `SELECT CURRENT_SCN FROM V$DATABASE`).Scan(&scn))
			var n int
			require.NoError(c, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s.%s AS OF SCN %d WHERE ROWNUM = 1", db.Schema, table, scn)).Scan(&n))
		}, 30*time.Second, 500*time.Millisecond, "Expected table %s.%s to be readable with a flashback query", db.Schema, table)
	}
}

// newChunkedSnapshot returns a snapshot of tables whose Info-level logs are captured, so
// tests can tell from the planning log line whether a table was split into chunks.
func newChunkedSnapshot(t *testing.T, connStr, pdbName string, tables ...replication.UserTable) (*replication.Snapshot, *publisherStub, *oracledbtest.SyncBuffer) {
	t.Helper()
	logs := &oracledbtest.SyncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	publisher := &publisherStub{}

	snapshot, err := replication.NewSnapshot(t.Context(), connStr, tables, nil, publisher, false, pdbName, nil, service.NewLoggerFromSlog(log), service.MockResources().Metrics())
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot, publisher, logs
}

const (
	planInterpolated = "interpolating its key range"
	planSampled      = "sampling its key range"
)

func chunkedLogPattern(table replication.UserTable) *regexp.Regexp {
	return regexp.MustCompile(`Launching snapshot of table '` + regexp.QuoteMeta(table.FullName()) + `' split into (\d+) parallel chunks by ([a-z ]+)`)
}

func assertSplitIntoChunks(t *testing.T, logs *oracledbtest.SyncBuffer, table replication.UserTable, method string) {
	t.Helper()
	m := chunkedLogPattern(table).FindStringSubmatch(logs.String())
	require.NotNilf(t, m, "Expected table '%s' to be split into chunks, logs:\n%s", table.FullName(), logs.String())
	chunks, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	assert.Greater(t, chunks, 1)
	assert.Equal(t, method, m[2], "Expected table '%s' to be planned by %s", table.FullName(), method)
	t.Logf("Table '%s' was split into %d chunks by %s", table.FullName(), chunks, m[2])
}

// mixedCaseKey is the VARCHAR2 key of row i in chunk_mixed_case_key_test.
func mixedCaseKey(i int) string {
	if i%2 == 0 {
		return fmt.Sprintf("a%09d", i)
	}
	return fmt.Sprintf("B%09d", i)
}

// paddedKey is the VARCHAR2 key of row i in the string key tables.
func paddedKey(i int) string {
	return fmt.Sprintf("%010d", i)
}

// assertEveryKeyOnce asserts that the published rows are exactly the keys wantKey(1) to
// wantKey(n): none missing (a gap between chunks) and none repeated (overlapping chunks or
// a page re-read).
func assertEveryKeyOnce(t *testing.T, publisher *publisherStub, n int, gotKey func(row map[string]any) string, wantKey func(i int) string) {
	t.Helper()
	msgs := publisher.messagesCopy()

	seen := make(map[string]int, len(msgs))
	for _, msg := range msgs {
		row, ok := msg.Data.(map[string]any)
		require.Truef(t, ok, "Expected message data to be a map[string]any, got %T", msg.Data)
		seen[gotKey(row)]++
	}

	var missing, duplicated []string
	for i := 1; i <= n; i++ {
		k := wantKey(i)
		switch seen[k] {
		case 0:
			missing = append(missing, k)
		case 1:
		default:
			duplicated = append(duplicated, k)
		}
		delete(seen, k)
	}
	unexpected := make([]string, 0, len(seen))
	for k := range seen {
		unexpected = append(unexpected, k)
	}

	assert.Len(t, msgs, n, "Expected exactly one message per row")
	assert.Empty(t, missing, "Expected no rows to be missing")
	assert.Empty(t, duplicated, "Expected no rows to be published more than once")
	assert.Empty(t, unexpected, "Expected no rows outside the snapshot")
}
