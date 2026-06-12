package replication_test

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func TestSnapshotReturnsReadEvents(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(ID\), 0\) FROM _RPCN_CDC\.CHANGES`).
		WillReturnRows(sqlmock.NewRows([]string{"M"}).AddRow(100))
	// Snapshot now paginates with LIMIT/OFFSET.
	// Batch size 2 → first page returns 2 rows (= batch size), second page returns 0 (stops).
	mock.ExpectQuery(`SELECT "ID", "NAME" FROM "HR"\."EMPLOYEES" ORDER BY "ID" LIMIT`).
		WithArgs(2, 0).
		WillReturnRows(sqlmock.NewRows([]string{"ID", "NAME"}).AddRow(1, "Alice").AddRow(2, "Bob"))
	mock.ExpectQuery(`SELECT "ID", "NAME" FROM "HR"\."EMPLOYEES" ORDER BY "ID" LIMIT`).
		WithArgs(2, 2).
		WillReturnRows(sqlmock.NewRows([]string{"ID", "NAME"}))

	snap := replication.NewSnapshot(db, replication.SnapshotConfig{PKColumns: []string{"ID"}, MaxBatchSize: 2})
	events, wm, err := snap.Read(context.Background(), "HR", "EMPLOYEES", []string{"ID", "NAME"})
	require.NoError(t, err)
	assert.Equal(t, replication.NewLogPos(100), wm)
	require.Len(t, events, 2)
	assert.Equal(t, replication.OpTypeRead, events[0].Operation)
	assert.True(t, events[0].LogPos.IsNull())
	assert.Equal(t, []string{"ID"}, events[0].PKColumns)
	assert.Equal(t, "Alice", events[0].Data["NAME"])
	assert.False(t, events[0].Timestamp.IsZero())
	assert.Equal(t, "Bob", events[1].Data["NAME"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSnapshotWatermarkBeforeRows(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT COALESCE`).WillReturnRows(sqlmock.NewRows([]string{"M"}).AddRow(999))
	// One row returned (< batch size 1024) → single page, loop ends.
	mock.ExpectQuery(`SELECT "ID" FROM "HR"\."T" ORDER BY "ID" LIMIT`).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	snap := replication.NewSnapshot(db, replication.SnapshotConfig{PKColumns: []string{"ID"}})
	_, wm, err := snap.Read(context.Background(), "HR", "T", []string{"ID"})
	require.NoError(t, err)
	assert.Equal(t, replication.NewLogPos(999), wm)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSnapshotEmptyTable(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT COALESCE`).WillReturnRows(sqlmock.NewRows([]string{"M"}).AddRow(0))
	// Empty table: first (and only) page returns 0 rows → loop ends immediately.
	mock.ExpectQuery(`SELECT`).WillReturnRows(sqlmock.NewRows([]string{"ID"}))
	snap := replication.NewSnapshot(db, replication.SnapshotConfig{})
	events, wm, err := snap.Read(context.Background(), "HR", "EMPTY", []string{"ID"})
	require.NoError(t, err)
	assert.Empty(t, events)
	assert.True(t, wm.IsNull())
}

func TestSnapshotNoPKOrder(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT COALESCE`).WillReturnRows(sqlmock.NewRows([]string{"M"}).AddRow(5))
	// Query now includes LIMIT/OFFSET pagination; match with a prefix pattern.
	mock.ExpectQuery(`SELECT "VAL" FROM "S"\."T" LIMIT`).WillReturnRows(sqlmock.NewRows([]string{"VAL"}).AddRow("x"))
	// Second page returns empty (terminates pagination loop).
	mock.ExpectQuery(`SELECT "VAL" FROM "S"\."T" LIMIT`).WillReturnRows(sqlmock.NewRows([]string{"VAL"}))
	snap := replication.NewSnapshot(db, replication.SnapshotConfig{MaxBatchSize: 1})
	events, _, err := snap.Read(context.Background(), "S", "T", []string{"VAL"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Empty(t, events[0].PKColumns)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSnapshotDefaultBatchSize(_ *testing.T) {
	snap := replication.NewSnapshot(nil, replication.SnapshotConfig{MaxBatchSize: 0})
	_ = snap // verify no panic; default 1024 is applied internally
}

func TestSnapshotTimestampSet(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	before := time.Now()
	mock.ExpectQuery(`SELECT COALESCE`).WillReturnRows(sqlmock.NewRows([]string{"M"}).AddRow(1))
	// 1 row returned < default batch size (1024) → single page, loop ends.
	mock.ExpectQuery(`SELECT "ID" FROM "S"\."T"`).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(42))
	snap := replication.NewSnapshot(db, replication.SnapshotConfig{})
	events, _, err := snap.Read(context.Background(), "S", "T", []string{"ID"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.False(t, events[0].Timestamp.Before(before))
}
