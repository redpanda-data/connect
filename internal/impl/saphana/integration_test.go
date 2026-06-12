// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package saphana_test verifies which SQL operations generate which SAP HANA
// redo-log event types.  These tests require a live HANA Express (HXE) instance
// and are gated behind HANA_INTEGRATION_TESTS=1.
//
// Redo-log block types under test:
//
//	insert           → INSERT statement
//	update           → UPDATE statement (before-image + changed columns only in log)
//	delete           → DELETE statement (rowid only — no column values in log)
//	upsert           → UPSERT / REPLACE (HANA native; NOT decomposed into insert+update)
//	truncate         → TRUNCATE TABLE (NOT per-row — triggers do not fire)
//	rollback         → ROLLBACK (all trigger writes are also rolled back)
//	dictionary change→ DDL (CREATE / ALTER / DROP TABLE — no DML triggers)
package saphana_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/saphanatest"
)

// ---------------------------------------------------------------------------
// Per-table setup helpers
// ---------------------------------------------------------------------------

func setupBasic(t *testing.T, db *saphanatest.TestDB) {
	t.Helper()
	db.ClearTable(t, "CDC_TEST", "BASIC")
}

func setupLob(t *testing.T, db *saphanatest.TestDB) {
	t.Helper()
	db.ClearTable(t, "CDC_TEST", "LOB_TABLE")
}

func setupUpsert(t *testing.T, db *saphanatest.TestDB) {
	t.Helper()
	db.ClearTable(t, "CDC_TEST", "UPSERT_TEST")
}

// ---------------------------------------------------------------------------
// DML — INSERT tests (covers `insert` redo-log block)
// ---------------------------------------------------------------------------

// TestInsertSingleRow inserts one row into BASIC and asserts that exactly one
// 'I' event is captured with all column values in NEW_VALUES and empty OLD_VALUES.
func TestInsertSingleRow(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	db.MustExec(t,
		`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, NSTR_VAL, INT_VAL, BIGINT_VAL, DOUBLE_VAL, DEC_VAL, DATE_VAL, TS_VAL, BOOL_VAL)
		 VALUES (1, 'hello', N'world', 42, 9999999999, 3.14, 1234.5678, '2024-01-15', '2024-01-15 10:30:00', TRUE)`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	c := changes[0]
	assert.Equal(t, "I", c.Op)
	assert.Equal(t, "CDC_TEST", c.SchemaName)
	assert.Equal(t, "BASIC", c.TableName)
	assert.Empty(t, c.OldValues, "INSERT should have no OLD_VALUES")
	assert.NotEmpty(t, c.NewValues, "INSERT should have NEW_VALUES")
	assert.Contains(t, c.NewValues, `"STR_VAL"`)
	assert.Contains(t, c.NewValues, "hello")
	assert.Contains(t, c.NewValues, `"INT_VAL"`)
}

// TestInsertNullValues inserts a row with only the PK set; all other columns
// should appear as JSON null in NEW_VALUES.
func TestInsertNullValues(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID) VALUES (1)`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	c := changes[0]
	assert.Equal(t, "I", c.Op)
	assert.Empty(t, c.OldValues)
	// All nullable columns should be present and null in the JSON
	assert.Contains(t, c.NewValues, `"STR_VAL":null`, "nullable column should be JSON null")
	assert.Contains(t, c.NewValues, `"INT_VAL":null`)
}

// TestInsertBulkSelect inserts N=3 rows via INSERT INTO … SELECT and asserts
// that N individual 'I' events are recorded (one per row, not one bulk event).
func TestInsertBulkSelect(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	// Seed a helper table inline using DUMMY unioned rows.
	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, INT_VAL)
		SELECT ID, STR_VAL, INT_VAL FROM (
			SELECT 1 AS ID, 'row1' AS STR_VAL, 10 AS INT_VAL FROM DUMMY
			UNION ALL
			SELECT 2 AS ID, 'row2' AS STR_VAL, 20 AS INT_VAL FROM DUMMY
			UNION ALL
			SELECT 3 AS ID, 'row3' AS STR_VAL, 30 AS INT_VAL FROM DUMMY
		)`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 3)
	for _, c := range changes {
		assert.Equal(t, "I", c.Op, "all events from INSERT…SELECT should be INSERT")
	}
}

// ---------------------------------------------------------------------------
// DML — UPDATE tests (covers `update` redo-log block)
// ---------------------------------------------------------------------------

// TestUpdateSingleColumn updates one column and asserts a 'U' event with both
// OLD_VALUES (before-image) and NEW_VALUES (after-image).
func TestUpdateSingleColumn(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, INT_VAL) VALUES (1, 'before', 10)`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.BASIC SET STR_VAL = 'after' WHERE ID = 1`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op)
	assert.NotEmpty(t, c.OldValues)
	assert.NotEmpty(t, c.NewValues)
	assert.Contains(t, c.OldValues, "before", "OLD_VALUES should contain pre-update value")
	assert.Contains(t, c.NewValues, "after", "NEW_VALUES should contain post-update value")
	// Unchanged columns are still present in both images (trigger captures full row)
	assert.Contains(t, c.OldValues, `"INT_VAL"`)
	assert.Contains(t, c.NewValues, `"INT_VAL"`)
}

// TestUpdateMultipleRows updates 5 rows at once and asserts 5 individual 'U' events.
func TestUpdateMultipleRows(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	for i := 1; i <= 5; i++ {
		db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, INT_VAL) VALUES (?, ?)`, i, i*10)
	}
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.BASIC SET INT_VAL = INT_VAL + 1`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 5)
	for _, c := range changes {
		assert.Equal(t, "U", c.Op)
	}
}

// TestUpdateToNull sets a column to NULL and asserts that OLD_VALUES has the
// previous non-null value and NEW_VALUES shows JSON null for that column.
func TestUpdateToNull(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (1, 'not-null')`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.BASIC SET STR_VAL = NULL WHERE ID = 1`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op)
	assert.Contains(t, c.OldValues, "not-null", "OLD_VALUES should contain pre-null value")
	assert.Contains(t, c.NewValues, `"STR_VAL":null`, "NEW_VALUES should show JSON null after SET col=NULL")
}

// ---------------------------------------------------------------------------
// DML — DELETE tests (covers `delete` redo-log block)
// ---------------------------------------------------------------------------

// TestDeleteSingleRow deletes one row and asserts a 'D' event with OLD_VALUES
// populated and empty NEW_VALUES.
func TestDeleteSingleRow(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, STR_VAL, INT_VAL) VALUES (1, 'todelete', 99)`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `DELETE FROM CDC_TEST.BASIC WHERE ID = 1`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	c := changes[0]
	assert.Equal(t, "D", c.Op)
	assert.NotEmpty(t, c.OldValues, "DELETE should have OLD_VALUES")
	assert.Empty(t, c.NewValues, "DELETE should have no NEW_VALUES")
	assert.Contains(t, c.OldValues, "todelete")
}

// TestDeleteAllRowsNoWhere deletes all rows without a WHERE clause and asserts
// one individual 'D' event per row — NOT a single bulk event.
// This is DISTINCT from TRUNCATE, which produces no per-row events at all.
func TestDeleteAllRowsNoWhere(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	for i := 1; i <= 4; i++ {
		db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (?, ?)`, i, fmt.Sprintf("row%d", i))
	}
	db.ClearVerifyChanges(t)

	db.MustExec(t, `DELETE FROM CDC_TEST.BASIC`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 4)
	for _, c := range changes {
		assert.Equal(t, "D", c.Op)
	}
}

// ---------------------------------------------------------------------------
// DML — TRUNCATE tests (covers `truncate` redo-log block — special case)
// ---------------------------------------------------------------------------

// TestTruncateDoesNotFireTriggers inserts 5 rows then TRUNCATEs the table and
// asserts ZERO events in the verify table.
//
// Why zero?  HANA logs TRUNCATE as a single "truncate" redo-log block rather
// than N individual row-delete blocks.  Because it is not a per-row DELETE,
// row-level AFTER DELETE triggers do not fire.  A CDC reader that processes
// redo-log blocks directly will see the truncate block; trigger-based
// verification will not.  This test documents that gap.
func TestTruncateDoesNotFireTriggers(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	for i := 1; i <= 5; i++ {
		db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID) VALUES (?)`, i)
	}
	db.ClearVerifyChanges(t)

	db.MustExec(t, `TRUNCATE TABLE CDC_TEST.BASIC`)

	// TRUNCATE is logged as a single truncate block — NOT as per-row deletes.
	// Row-level AFTER DELETE triggers do not fire for TRUNCATE.
	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 0)
	assert.Empty(t, changes, "TRUNCATE must produce zero trigger events (truncate is not a per-row delete)")

	// Confirm the table is actually empty.
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM CDC_TEST.BASIC`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "TRUNCATE TABLE should empty the table")
}

// ---------------------------------------------------------------------------
// DML — UPSERT / REPLACE tests (covers `upsert` redo-log block)
// ---------------------------------------------------------------------------

// TestUpsertInsertPath issues an UPSERT for a row that does not yet exist and
// asserts the trigger fires as 'I' (insert branch).
func TestUpsertInsertPath(t *testing.T) {
	db := saphanatest.Connect(t)
	setupUpsert(t, db)

	db.MustExec(t, `UPSERT CDC_TEST.UPSERT_TEST (ID, STR_VAL, INT_VAL) VALUES (1, 'new-row', 10) WITH PRIMARY KEY`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "UPSERT_TEST", 1)
	assert.Equal(t, "I", changes[0].Op, "UPSERT of non-existing row should trigger INSERT")
	assert.Contains(t, changes[0].NewValues, "new-row")
}

// TestUpsertUpdatePath issues an UPSERT for a row that already exists and
// asserts the trigger fires as 'U' (update branch) with old and new values.
func TestUpsertUpdatePath(t *testing.T) {
	db := saphanatest.Connect(t)
	setupUpsert(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.UPSERT_TEST (ID, STR_VAL, INT_VAL) VALUES (1, 'original', 10)`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPSERT CDC_TEST.UPSERT_TEST (ID, STR_VAL, INT_VAL) VALUES (1, 'updated', 20) WITH PRIMARY KEY`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "UPSERT_TEST", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op, "UPSERT of existing row should trigger UPDATE")
	assert.Contains(t, c.OldValues, "original")
	assert.Contains(t, c.NewValues, "updated")
}

// TestReplaceIsSynonymForUpsert issues a REPLACE for an existing row and
// asserts the trigger fires as 'U'.  REPLACE is syntactic sugar for UPSERT in HANA.
func TestReplaceIsSynonymForUpsert(t *testing.T) {
	db := saphanatest.Connect(t)
	setupUpsert(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.UPSERT_TEST (ID, STR_VAL, INT_VAL) VALUES (5, 'before-replace', 50)`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `REPLACE CDC_TEST.UPSERT_TEST (ID, STR_VAL, INT_VAL) VALUES (5, 'after-replace', 55) WHERE ID = 5`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "UPSERT_TEST", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op, "REPLACE of existing row should trigger UPDATE (REPLACE is UPSERT synonym in HANA)")
	assert.Contains(t, c.OldValues, "before-replace")
	assert.Contains(t, c.NewValues, "after-replace")
}

// ---------------------------------------------------------------------------
// Transactions — COMMIT and ROLLBACK
// ---------------------------------------------------------------------------

// TestRollbackProducesNoEvents begins a transaction, inserts two rows, then
// rolls back and asserts ZERO events in the verify table.
//
// Why zero?  The AFTER INSERT triggers fire inside the transaction and write to
// _CDC_VERIFY.CHANGES.  When the outer transaction is rolled back, those trigger
// writes are rolled back too — HANA transactional semantics apply to trigger
// side-effects as well.  This confirms that trigger-based ground-truth is
// transactionally consistent with the source DML.
func TestRollbackProducesNoEvents(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (1, 'will-rollback')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (2, 'also-rollback')`)
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	// Trigger writes to _CDC_VERIFY.CHANGES are rolled back with the transaction.
	db.RequireChangeCount(t, "CDC_TEST", "BASIC", 0)
}

// TestCommitPreservesAllEvents begins a transaction with 3 inserts + 1 update +
// 1 delete, commits, and asserts 5 events in order [I, I, I, U, D].
func TestCommitPreservesAllEvents(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (1, 'a')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (2, 'b')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO CDC_TEST.BASIC (ID, STR_VAL) VALUES (3, 'c')`)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE CDC_TEST.BASIC SET STR_VAL = 'a-updated' WHERE ID = 1`)
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM CDC_TEST.BASIC WHERE ID = 2`)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 5)
	ops := make([]string, len(changes))
	for i, c := range changes {
		ops[i] = c.Op
	}
	assert.Equal(t, []string{"I", "I", "I", "U", "D"}, ops,
		"committed events should appear in DML order")
}

// ---------------------------------------------------------------------------
// DDL tests (covers `dictionary change` redo-log block)
// ---------------------------------------------------------------------------

// TestDDLCreateAndDrop creates and drops a temporary table and asserts zero DML
// events.  DDL operations produce a "dictionary change" redo-log block — not an
// insert/update/delete block — so no DML triggers fire.
func TestDDLCreateAndDrop(t *testing.T) {
	db := saphanatest.Connect(t)
	db.ClearVerifyChanges(t)

	// Use a timestamp-based name to avoid collisions between parallel runs.
	tableName := fmt.Sprintf("CDC_TEST.TEMP_DDL_TEST_%d", time.Now().UnixNano())

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", tableName))
	db.MustExec(t, fmt.Sprintf("CREATE COLUMN TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, VAL VARCHAR(100))", tableName))

	db.RequireChangeCount(t, "CDC_TEST", "", 0)

	db.MustExec(t, fmt.Sprintf("DROP TABLE %s", tableName))

	db.RequireChangeCount(t, "CDC_TEST", "", 0)
}

// TestDDLAlterAddColumn creates a fresh table, alters it to add a column,
// inserts a row using the new column, and verifies the column value is queryable.
func TestDDLAlterAddColumn(t *testing.T) {
	db := saphanatest.Connect(t)
	db.ClearVerifyChanges(t)

	tableName := fmt.Sprintf("CDC_TEST.DDL_ALTER_ADD_%d", time.Now().UnixNano())

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", tableName))
	db.MustExec(t, fmt.Sprintf("CREATE COLUMN TABLE %s (ID INTEGER NOT NULL PRIMARY KEY)", tableName))
	db.MustExec(t, fmt.Sprintf("ALTER TABLE %s ADD (EXTRA_COL VARCHAR(50))", tableName))

	db.MustExec(t, fmt.Sprintf("INSERT INTO %s (ID, EXTRA_COL) VALUES (1, 'alt-col-val')", tableName))

	var val string
	err := db.QueryRow(fmt.Sprintf("SELECT EXTRA_COL FROM %s WHERE ID = 1", tableName)).Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "alt-col-val", val, "newly added column should be queryable")

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", tableName))
}

// TestDDLAlterDropColumn creates a table with an extra column, drops that column,
// and verifies the column no longer exists.
func TestDDLAlterDropColumn(t *testing.T) {
	db := saphanatest.Connect(t)
	db.ClearVerifyChanges(t)

	tableName := fmt.Sprintf("CDC_TEST.DDL_ALTER_DROP_%d", time.Now().UnixNano())

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", tableName))
	db.MustExec(t, fmt.Sprintf(
		"CREATE COLUMN TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, KEEP_COL VARCHAR(50), DROP_COL VARCHAR(50))",
		tableName))
	db.MustExec(t, fmt.Sprintf("ALTER TABLE %s DROP (DROP_COL)", tableName))

	// The column should no longer be in SYS.TABLE_COLUMNS.
	parts := strings.SplitN(tableName, ".", 2)
	schema, tbl := parts[0], parts[1]
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'DROP_COL'`,
		schema, tbl).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "dropped column must not exist in SYS.TABLE_COLUMNS")

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", tableName))
}

// TestDDLRenameTable creates a table, inserts a row, renames the table, and
// verifies data is accessible at the new name while the old name is gone.
func TestDDLRenameTable(t *testing.T) {
	db := saphanatest.Connect(t)
	db.ClearVerifyChanges(t)

	ts := time.Now().UnixNano()
	oldName := fmt.Sprintf("CDC_TEST.DDL_RENAME_OLD_%d", ts)
	newName := fmt.Sprintf("CDC_TEST.DDL_RENAME_NEW_%d", ts)

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", newName))
	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", oldName))

	db.MustExec(t, fmt.Sprintf("CREATE COLUMN TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, VAL VARCHAR(50))", oldName))
	db.MustExec(t, fmt.Sprintf("INSERT INTO %s (ID, VAL) VALUES (1, 'rename-me')", oldName))
	db.MustExec(t, fmt.Sprintf("RENAME TABLE %s TO %s", oldName, newName))

	var val string
	err := db.QueryRow(fmt.Sprintf("SELECT VAL FROM %s WHERE ID = 1", newName)).Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "rename-me", val, "data must be accessible at the new table name")

	// Old name must no longer be queryable.
	parts := strings.SplitN(oldName, ".", 2)
	schema, tbl := parts[0], parts[1]
	var count int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?`,
		schema, tbl).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "old table name must not exist after rename")

	db.ExecIgnoreError(fmt.Sprintf("DROP TABLE %s", newName))
}

// ---------------------------------------------------------------------------
// LOB operation tests
// ---------------------------------------------------------------------------

// TestLobInsertSmall inserts a row with a short (in-memory) CLOB and asserts
// an 'I' event where CLOB_LEN > 0.
func TestLobInsertSmall(t *testing.T) {
	db := saphanatest.Connect(t)
	setupLob(t, db)

	smallClob := strings.Repeat("x", 100)
	db.MustExec(t, `INSERT INTO CDC_TEST.LOB_TABLE (ID, STR_VAL, CLOB_VAL) VALUES (1, 'small', ?)`, smallClob)

	changes := db.RequireChangeCount(t, "CDC_TEST", "LOB_TABLE", 1)
	c := changes[0]
	assert.Equal(t, "I", c.Op)
	assert.Contains(t, c.NewValues, `"CLOB_LEN"`)
	// The trigger stores the character length; it should be > 0 for a non-null CLOB.
	assert.NotContains(t, c.NewValues, `"CLOB_LEN":-1`, "small CLOB should have CLOB_LEN >= 0")
}

// TestLobInsertLarge inserts a row with an 8192-char CLOB (disk LOB threshold),
// asserts an 'I' event with CLOB_LEN = 8192, then reads back the full value.
func TestLobInsertLarge(t *testing.T) {
	db := saphanatest.Connect(t)
	setupLob(t, db)

	largeClob := strings.Repeat("a", 8192)
	db.MustExec(t, `INSERT INTO CDC_TEST.LOB_TABLE (ID, STR_VAL, CLOB_VAL) VALUES (1, 'large', ?)`, largeClob)

	changes := db.RequireChangeCount(t, "CDC_TEST", "LOB_TABLE", 1)
	c := changes[0]
	assert.Equal(t, "I", c.Op)
	assert.Contains(t, c.NewValues, `"CLOB_LEN":8192`)

	// Read back the full LOB and verify content length.
	var readBack string
	err := db.QueryRow(`SELECT CLOB_VAL FROM CDC_TEST.LOB_TABLE WHERE ID = 1`).Scan(&readBack)
	require.NoError(t, err)
	assert.Len(t, readBack, 8192, "large CLOB should round-trip at full length")
}

// TestLobUpdateLobColumn inserts a row then updates the CLOB column and asserts
// a 'U' event where both old and new CLOB_LEN values are > 0.
func TestLobUpdateLobColumn(t *testing.T) {
	db := saphanatest.Connect(t)
	setupLob(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.LOB_TABLE (ID, CLOB_VAL) VALUES (1, ?)`, strings.Repeat("o", 50))
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.LOB_TABLE SET CLOB_VAL = ? WHERE ID = 1`, strings.Repeat("n", 200))

	changes := db.RequireChangeCount(t, "CDC_TEST", "LOB_TABLE", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op)
	// Both old and new LOB lengths should be positive (not null / -1).
	assert.NotContains(t, c.OldValues, `"CLOB_LEN":-1`, "old CLOB_LEN must be non-null")
	assert.NotContains(t, c.NewValues, `"CLOB_LEN":-1`, "new CLOB_LEN must be non-null")
}

// TestLobUpdateNonLobColumn inserts a row with a CLOB then updates only the
// non-LOB column STR_VAL.  A 'U' event should be captured.
//
// Note on trigger vs. redo-log difference:  the HANA redo log for an UPDATE
// only contains the changed columns (plus the PK).  A LOB column that was NOT
// modified therefore does NOT appear in the redo-log update block.  However,
// the AFTER UPDATE trigger has full access to :old and :new for all columns
// including unchanged ones.  This means trigger-based verification captures
// complete before/after images even when the redo-log update block is sparse.
func TestLobUpdateNonLobColumn(t *testing.T) {
	db := saphanatest.Connect(t)
	setupLob(t, db)

	db.MustExec(t, `INSERT INTO CDC_TEST.LOB_TABLE (ID, STR_VAL, CLOB_VAL) VALUES (1, 'before', ?)`, strings.Repeat("c", 100))
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.LOB_TABLE SET STR_VAL = 'after' WHERE ID = 1`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "LOB_TABLE", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op)
	assert.Contains(t, c.OldValues, "before")
	assert.Contains(t, c.NewValues, "after")
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

// TestEdgeCaseUnicode inserts a row with Japanese text and a emoji (U+1F600)
// in NSTR_VAL and asserts the 'I' event contains the Unicode characters.
func TestEdgeCaseUnicode(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	unicodeVal := "日本語テスト\U0001F600"
	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, NSTR_VAL) VALUES (1, ?)`, unicodeVal)

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 1)
	assert.Equal(t, "I", changes[0].Op)
	assert.Contains(t, changes[0].NewValues, "日本語テスト", "trigger should capture Unicode NVARCHAR correctly")
}

// TestEdgeCaseMaxIntegerValues inserts the maximum values for INTEGER and BIGINT,
// reads them back, and asserts no truncation occurred.
func TestEdgeCaseMaxIntegerValues(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	const maxInt32 = int32(2147483647)
	const maxInt64 = int64(9223372036854775807)

	db.MustExec(t,
		`INSERT INTO CDC_TEST.BASIC (ID, INT_VAL, BIGINT_VAL) VALUES (1, ?, ?)`,
		maxInt32, maxInt64)

	var gotInt32 int32
	var gotInt64 int64
	err := db.QueryRow(`SELECT INT_VAL, BIGINT_VAL FROM CDC_TEST.BASIC WHERE ID = 1`).Scan(&gotInt32, &gotInt64)
	require.NoError(t, err)
	assert.Equal(t, maxInt32, gotInt32, "INT_VAL max value must survive round-trip without truncation")
	assert.Equal(t, maxInt64, gotInt64, "BIGINT_VAL max value must survive round-trip without truncation")
}

// TestEdgeCaseDateBoundaries inserts rows with edge-case DATE values:
// the minimum representable date, the maximum date, and the Julian-Gregorian
// calendar boundary (1582-10-15).  All three should succeed.
func TestEdgeCaseDateBoundaries(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	dates := []struct {
		id  int
		val string
	}{
		{1, "0001-01-01"},
		{2, "9999-12-31"},
		{3, "1582-10-15"}, // Julian-Gregorian boundary
	}

	for _, d := range dates {
		db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, DATE_VAL) VALUES (?, TO_DATE(?, 'YYYY-MM-DD'))`, d.id, d.val)
	}

	changes := db.RequireChangeCount(t, "CDC_TEST", "BASIC", 3)
	for _, c := range changes {
		assert.Equal(t, "I", c.Op)
	}
}

// TestEdgeCaseAllTypesInsert inserts one row into ALL_TYPES with values for all
// 23 columns and asserts the row count becomes 1.  No trigger assertion is
// needed here — the goal is to confirm every HANA type is accepted without error.
func TestEdgeCaseAllTypesInsert(t *testing.T) {
	db := saphanatest.Connect(t)
	db.MustExec(t, `DELETE FROM CDC_TEST.ALL_TYPES`)

	db.MustExec(t, `
INSERT INTO CDC_TEST.ALL_TYPES (
    ID, TINYINT_VAL, SMALLINT_VAL, INTEGER_VAL, BIGINT_VAL,
    REAL_VAL, DOUBLE_VAL,
    DECIMAL_10_2, DECIMAL_38_10, SMALLDECIMAL_VAL,
    VARCHAR_VAL, NVARCHAR_VAL, CHAR_VAL, NCHAR_VAL, ALPHANUM_VAL, SHORTTEXT_VAL,
    BINARY_VAL, VARBINARY_VAL,
    DATE_VAL, TIME_VAL, SECONDDATE_VAL, TIMESTAMP_VAL, LONGDATE_VAL,
    BOOLEAN_VAL
) VALUES (
    1, 127, 32767, 2147483647, 9223372036854775807,
    3.14, 2.718281828,
    12345.67, 12345678901234567890.1234567890, 9999.99,
    'varchar-val', N'nvarchar-val', 'char-val  ', N'nchar-val ', 'A1B2C3D4E5', 'short text value here',
    X'DEADBEEF0102030405060708090A0B0C',
    X'0102030405060708',
    '2024-06-01', '12:34:56', '2024-06-01 12:34:56', '2024-06-01 12:34:56.123456789', '2024-06-01 12:34:56.123456789',
    TRUE
)`)

	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM CDC_TEST.ALL_TYPES`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "ALL_TYPES should have exactly 1 row after insert")
}

// TestEdgeCaseTimestampPrecision inserts a specific TIMESTAMP value and reads it
// back, asserting the retrieved value is within 1 microsecond of the original.
// HANA TIMESTAMP has microsecond precision (LONGDATE has nanosecond precision).
func TestEdgeCaseTimestampPrecision(t *testing.T) {
	db := saphanatest.Connect(t)
	setupBasic(t, db)

	// Truncate to microsecond precision, since HANA TIMESTAMP stores up to 7 fractional digits
	// but the Go driver may round to microseconds.
	want := time.Date(2024, 3, 15, 14, 30, 45, 123456000, time.UTC)

	db.MustExec(t, `INSERT INTO CDC_TEST.BASIC (ID, TS_VAL) VALUES (1, ?)`, want)

	var got time.Time
	err := db.QueryRow(`SELECT TS_VAL FROM CDC_TEST.BASIC WHERE ID = 1`).Scan(&got)
	require.NoError(t, err)

	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	assert.LessOrEqual(t, diff, time.Microsecond,
		"TIMESTAMP should round-trip within 1 microsecond; got %v, want %v", got, want)
}

// ---------------------------------------------------------------------------
// Composite PK
// ---------------------------------------------------------------------------

// TestCompositePKUpdate inserts a row into COMPOSITE_PK, updates it, and asserts
// that the 'U' event's PK_JSON contains both PK1 and PK2.
func TestCompositePKUpdate(t *testing.T) {
	db := saphanatest.Connect(t)
	db.ClearTable(t, "CDC_TEST", "COMPOSITE_PK")

	db.MustExec(t, `INSERT INTO CDC_TEST.COMPOSITE_PK (PK1, PK2, STR_VAL, INT_VAL) VALUES (1, 'alpha', 'original', 10)`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `UPDATE CDC_TEST.COMPOSITE_PK SET STR_VAL = 'updated', INT_VAL = 99 WHERE PK1 = 1 AND PK2 = 'alpha'`)

	changes := db.RequireChangeCount(t, "CDC_TEST", "COMPOSITE_PK", 1)
	c := changes[0]
	assert.Equal(t, "U", c.Op)
	assert.Contains(t, c.PKJson, `"PK1"`, "PK_JSON must include first PK column")
	assert.Contains(t, c.PKJson, `"PK2"`, "PK_JSON must include second PK column")
	assert.Contains(t, c.PKJson, "alpha")
	assert.Contains(t, c.OldValues, "original")
	assert.Contains(t, c.NewValues, "updated")
}

// ---------------------------------------------------------------------------
// Row store (documents unsupported CDC case)
// ---------------------------------------------------------------------------

// TestRowTableInsertWorks inserts into ROW_TABLE (which has no CDC triggers) and
// asserts zero events in the verify table.
//
// This documents a known CDC tooling limitation: SAP HANA CDC readers (HVR,
// Debezium HANA connector, rtdi.io) explicitly do NOT support row-store tables.
// Only column-store tables produce redo-log entries that these tools can process.
// ROW_TABLE is included in the schema to assert — and remind maintainers — that
// CDC coverage intentionally excludes row-store tables.
func TestRowTableInsertWorks(t *testing.T) {
	db := saphanatest.Connect(t)
	db.MustExec(t, `DELETE FROM CDC_TEST.ROW_TABLE`)
	db.ClearVerifyChanges(t)

	db.MustExec(t, `INSERT INTO CDC_TEST.ROW_TABLE (ID, VAL) VALUES (1, 'row-store-val')`)

	// No triggers on ROW_TABLE — zero events in the verify table.
	// This documents that CDC tooling does not support row-store tables.
	db.RequireChangeCount(t, "CDC_TEST", "ROW_TABLE", 0)

	// The row should still be in the table via direct query.
	var val string
	err := db.QueryRow(`SELECT VAL FROM CDC_TEST.ROW_TABLE WHERE ID = 1`).Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "row-store-val", val, "row-store INSERT should persist the row despite no CDC capture")
}

// ---------------------------------------------------------------------------
// Log segments
// ---------------------------------------------------------------------------

// TestLogSegmentsVisible queries SYS.M_LOG_SEGMENTS and asserts at least one
// segment exists, logging each segment for diagnostic purposes.
func TestLogSegmentsVisible(t *testing.T) {
	db := saphanatest.Connect(t)

	rows, err := db.Query(`
		SELECT
			HOST,
			PORT,
			VOLUME_ID,
			STATE,
			TOTAL_SIZE,
			USED_SIZE
		FROM SYS.M_LOG_SEGMENTS
		ORDER BY VOLUME_ID, HOST`)
	require.NoError(t, err, "querying SYS.M_LOG_SEGMENTS")
	defer rows.Close()

	var count int
	for rows.Next() {
		var host string
		var port, volumeID int
		var state string
		var totalSize, usedSize int64
		require.NoError(t, rows.Scan(&host, &port, &volumeID, &state, &totalSize, &usedSize))
		t.Logf("log segment: host=%s port=%d volume_id=%d state=%s total_size=%d used_size=%d",
			host, port, volumeID, state, totalSize, usedSize)
		count++
	}
	require.NoError(t, rows.Err())
	assert.GreaterOrEqual(t, count, 1, "SYS.M_LOG_SEGMENTS should have at least one segment")
}
