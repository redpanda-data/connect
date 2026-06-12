package saphana_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana"
	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

// ──────────────────────────────────────────────
// CheckpointCache unit tests (mock DB)
// ──────────────────────────────────────────────

func TestCheckpointCacheLoadNoRows(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	// NewCheckpointCache runs 2 CREATE statements (matched loosely)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved with a Load (returns no-row → null)
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("my_key").
		WillReturnError(sql.ErrNoRows)
	// Explicit Load call from test: no row
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("my_key").
		WillReturnError(sql.ErrNoRows)

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "my_key")
	require.NoError(t, err)
	pos, err := cc.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, pos.IsNull(), "no row → null LogPos")
}

func TestCheckpointCacheLoadReturnsStoredValue(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved with a Load
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("k").
		WillReturnRows(sqlmock.NewRows([]string{"CACHE_VAL"}).AddRow(uint64(42)))
	// Explicit Load call from test
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("k").
		WillReturnRows(sqlmock.NewRows([]string{"CACHE_VAL"}).AddRow(uint64(42)))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)
	pos, err := cc.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "42", pos.String())
}

func TestCheckpointCacheLoadDBError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved; ignore the error gracefully (lastSaved stays null)
	mock.ExpectQuery("SELECT CACHE_VAL").WillReturnError(sql.ErrNoRows)
	// Explicit Load call returns a real error
	mock.ExpectQuery("SELECT CACHE_VAL").WillReturnError(errors.New("db error"))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)
	_, err = cc.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading checkpoint")
}

func TestCheckpointCacheSaveNullIsNoop(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved with a Load (no row → null)
	mock.ExpectQuery("SELECT CACHE_VAL").WithArgs("k").WillReturnError(sql.ErrNoRows)
	// Save(null) must not issue any SQL

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)
	err = cc.Save(context.Background(), replication.LogPos(0)) // null → no-op
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckpointCacheSavePersists(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved with a Load (no row → null)
	mock.ExpectQuery("SELECT CACHE_VAL").WithArgs("k").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("UPSERT").
		WithArgs("k", uint64(99)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)
	err = cc.Save(context.Background(), replication.NewLogPos(99))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckpointCacheSaveError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// NewCheckpointCache pre-warms lastSaved with a Load (no row → null)
	mock.ExpectQuery("SELECT CACHE_VAL").WithArgs("k").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`UPSERT`).WillReturnError(errors.New("write failed"))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)
	err = cc.Save(context.Background(), replication.NewLogPos(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "saving checkpoint")
}

func TestNewCheckpointCacheCreateFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnError(errors.New("permission denied"))

	_, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating checkpoint infrastructure")
}

// ──────────────────────────────────────────────
// SchemaCache unit tests (mock DB)
// ──────────────────────────────────────────────

func TestSchemaCacheForTableReturnsCols(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(`SELECT.*COLUMN_NAME.*FROM SYS.TABLE_COLUMNS`).
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME", "DATA_TYPE_NAME", "POSITION", "IS_NULLABLE", "IS_PK",
		}).
			AddRow("ID", "INTEGER", 1, "FALSE", "TRUE").
			AddRow("NAME", "NVARCHAR", 2, "TRUE", "FALSE"))

	sc := saphana.NewSchemaCache(db)
	cols, pks, err := sc.ForTable(context.Background(), "HR", "EMPLOYEES")
	require.NoError(t, err)
	require.Len(t, cols, 2)
	assert.Equal(t, "ID", cols[0].Name)
	assert.Equal(t, "INTEGER", cols[0].TypeName)
	assert.True(t, cols[0].IsPK)
	assert.False(t, cols[0].Nullable)
	assert.Equal(t, "NAME", cols[1].Name)
	assert.True(t, cols[1].Nullable)
	assert.Equal(t, []string{"ID"}, pks)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaCacheForTableCachesResult(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	// Only ONE query expected — second call hits cache
	mock.ExpectQuery(`SELECT.*FROM SYS.TABLE_COLUMNS`).
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME", "DATA_TYPE_NAME", "POSITION", "IS_NULLABLE", "IS_PK",
		}).AddRow("ID", "INTEGER", 1, "FALSE", "TRUE"))

	sc := saphana.NewSchemaCache(db)
	_, _, err := sc.ForTable(context.Background(), "S", "T")
	require.NoError(t, err)
	_, _, err = sc.ForTable(context.Background(), "S", "T") // cached
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaCacheInvalidateClearsCache(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	// Two queries expected — second call after Invalidate re-fetches
	for range 2 {
		mock.ExpectQuery(`SELECT.*FROM SYS.TABLE_COLUMNS`).
			WillReturnRows(sqlmock.NewRows([]string{
				"COLUMN_NAME", "DATA_TYPE_NAME", "POSITION", "IS_NULLABLE", "IS_PK",
			}).AddRow("ID", "INTEGER", 1, "FALSE", "TRUE"))
	}

	sc := saphana.NewSchemaCache(db)
	_, _, _ = sc.ForTable(context.Background(), "S", "T")
	sc.Invalidate("S", "T")
	_, _, err := sc.ForTable(context.Background(), "S", "T")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaCacheForTableDBError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(`SELECT.*FROM SYS.TABLE_COLUMNS`).
		WillReturnError(errors.New("table not found"))

	sc := saphana.NewSchemaCache(db)
	_, _, err := sc.ForTable(context.Background(), "S", "T")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching columns")
}

// ──────────────────────────────────────────────
// SaveIfHigher in-memory behaviour (Bug 5 & 20)
// ──────────────────────────────────────────────

// TestSaveIfHigherSkipsDBWhenNotAdvancing verifies that SaveIfHigher issues
// NO database statement when pos does not advance the in-memory high-water mark.
func TestSaveIfHigherSkipsDBWhenNotAdvancing(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// Pre-warm Load: checkpoint at pos 50.
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("k").
		WillReturnRows(sqlmock.NewRows([]string{"CACHE_VAL"}).AddRow(uint64(50)))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)

	// SaveIfHigher with pos <= 50 must NOT issue any SQL (no more expectations).
	err = cc.SaveIfHigher(context.Background(), replication.NewLogPos(30))
	require.NoError(t, err)
	err = cc.SaveIfHigher(context.Background(), replication.NewLogPos(50))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSaveIfHigherPersistsWhenAdvancing verifies that SaveIfHigher issues a
// single UPSERT when pos > the in-memory high-water mark.
func TestSaveIfHigherPersistsWhenAdvancing(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// Pre-warm Load: checkpoint at pos 10.
	mock.ExpectQuery("SELECT CACHE_VAL").
		WithArgs("k").
		WillReturnRows(sqlmock.NewRows([]string{"CACHE_VAL"}).AddRow(uint64(10)))
	// Exactly one UPSERT for the advance.
	mock.ExpectExec("UPSERT").
		WithArgs("k", uint64(20)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)

	err = cc.SaveIfHigher(context.Background(), replication.NewLogPos(20))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// A second call with the same pos must NOT issue another UPSERT.
	err = cc.SaveIfHigher(context.Background(), replication.NewLogPos(20))
	require.NoError(t, err)
}

// TestSaveIfHigherPropagatesSaveError verifies that errors from the underlying
// Save() are returned to the caller (Bug 20).
func TestSaveIfHigherPropagatesSaveError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cfg, _ := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	// Pre-warm Load: no row (null checkpoint).
	mock.ExpectQuery("SELECT CACHE_VAL").WithArgs("k").WillReturnError(sql.ErrNoRows)
	// UPSERT fails.
	mock.ExpectExec("UPSERT").WillReturnError(errors.New("disk full"))

	cc, err := saphana.NewCheckpointCache(context.Background(), cfg, db, "k")
	require.NoError(t, err)

	err = cc.SaveIfHigher(context.Background(), replication.NewLogPos(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "saving checkpoint")
}
