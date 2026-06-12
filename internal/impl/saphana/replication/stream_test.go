package replication_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func streamCols() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"ID", "OP", "SCHEMA_NAME", "TABLE_NAME", "OP_TIME", "PK_JSON", "OLD_VALUES", "NEW_VALUES"})
}

func minStreamCfg() replication.StreamConfig {
	return replication.StreamConfig{PollBatchSize: 100, MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond}
}

func TestStreamInsert(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	nv, _ := json.Marshal(map[string]any{"ID": 1, "NAME": "Alice"})
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WithArgs(int64(0), 100).
		WillReturnRows(streamCols().AddRow(1, "I", "HR", "EMP", time.Now(), `{"ID":1}`, "", string(nv)))
	s := replication.NewStream(db, minStreamCfg())
	events, err := s.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, replication.OpTypeInsert, events[0].Operation)
	assert.Equal(t, "Alice", events[0].Data["NAME"])
	assert.Nil(t, events[0].BeforeData)
	assert.Equal(t, replication.NewLogPos(1), events[0].LogPos)
	assert.Equal(t, "HR", events[0].Schema)
	assert.Equal(t, "EMP", events[0].Table)
}

func TestStreamUpdate(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	ov, _ := json.Marshal(map[string]any{"ID": 2, "NAME": "Bob"})
	nv, _ := json.Marshal(map[string]any{"ID": 2, "NAME": "Robert"})
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WillReturnRows(streamCols().AddRow(2, "U", "HR", "EMP", time.Now(), `{"ID":2}`, string(ov), string(nv)))
	s := replication.NewStream(db, minStreamCfg())
	events, err := s.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, replication.OpTypeUpdate, events[0].Operation)
	assert.Equal(t, "Bob", events[0].BeforeData["NAME"])
	assert.Equal(t, "Robert", events[0].Data["NAME"])
}

func TestStreamDelete(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	ov, _ := json.Marshal(map[string]any{"ID": 3, "NAME": "Eve"})
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WillReturnRows(streamCols().AddRow(3, "D", "HR", "EMP", time.Now(), `{"ID":3}`, string(ov), ""))
	s := replication.NewStream(db, minStreamCfg())
	events, err := s.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, replication.OpTypeDelete, events[0].Operation)
	assert.Equal(t, "Eve", events[0].Data["NAME"])
	assert.Nil(t, events[0].BeforeData)
}

func TestStreamEmpty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).WillReturnRows(streamCols())
	s := replication.NewStream(db, minStreamCfg())
	events, err := s.Poll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, events)
	assert.True(t, s.LastPos().IsNull(), "empty poll must not advance LastPos")
}

func TestStreamAdvancesPos(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WillReturnRows(streamCols().AddRow(5, "D", "S", "T", time.Now(), `{}`, `{"ID":5}`, ""))
	s := replication.NewStream(db, minStreamCfg())
	_, _ = s.Poll(context.Background())
	assert.Equal(t, replication.NewLogPos(5), s.LastPos())
}

func TestStreamStartFrom(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WithArgs(int64(100), 100).WillReturnRows(streamCols())
	s := replication.NewStream(db, minStreamCfg())
	s.StartFrom(replication.NewLogPos(100))
	_, err := s.Poll(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamMultipleRows(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	rows := streamCols()
	for i := 1; i <= 5; i++ {
		nv, _ := json.Marshal(map[string]any{"ID": i})
		rows.AddRow(i, "I", "S", "T", time.Now(), fmt.Sprintf(`{"ID":%d}`, i), "", string(nv))
	}
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).WillReturnRows(rows)
	s := replication.NewStream(db, minStreamCfg())
	events, err := s.Poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, events, 5)
	assert.Equal(t, replication.NewLogPos(5), s.LastPos())
}

func TestStreamDefaultConfig(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	// Zero config → defaults applied; batch size 2048
	mock.ExpectQuery(`SELECT .* FROM _RPCN_CDC\.CHANGES`).
		WithArgs(int64(0), 2048).WillReturnRows(streamCols())
	s := replication.NewStream(db, replication.StreamConfig{})
	_, err := s.Poll(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
