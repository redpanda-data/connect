package saphana_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana"
	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func TestEventToMessageInsert(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema: "HR", Table: "EMPLOYEES",
		Operation: replication.OpTypeInsert,
		LogPos:    replication.NewLogPos(42),
		Timestamp: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
		Data:      map[string]any{"ID": float64(1), "NAME": "Alice"},
	}
	msg, err := saphana.EventToMessage(ev, "test-connector")
	require.NoError(t, err)
	raw, err := msg.AsStructuredMut()
	require.NoError(t, err)
	env := raw.(map[string]any)
	assert.Nil(t, env["before"])
	assert.Equal(t, map[string]any{"ID": float64(1), "NAME": "Alice"}, env["after"])
	assert.Equal(t, "c", env["op"])
	assert.NotNil(t, env["ts_ms"])
	src := env["source"].(map[string]any)
	assert.Equal(t, "redpanda.saphana", src["connector"])
	assert.Equal(t, "HR", src["schema"])
	assert.Equal(t, "EMPLOYEES", src["table"])
	assert.Equal(t, "42", src["lsn"])
	assert.Equal(t, "false", src["snapshot"])
	lsn, ok := msg.MetaGet("saphana_lsn")
	require.True(t, ok)
	assert.Equal(t, "42", lsn)
	op, ok := msg.MetaGet("saphana_op")
	require.True(t, ok)
	assert.Equal(t, "c", op)
}

func TestEventToMessageUpdate(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema: "HR", Table: "EMPLOYEES",
		Operation:  replication.OpTypeUpdate,
		LogPos:     replication.NewLogPos(99),
		Timestamp:  time.Now(),
		Data:       map[string]any{"ID": float64(1), "NAME": "Robert"},
		BeforeData: map[string]any{"ID": float64(1), "NAME": "Bob"},
	}
	msg, err := saphana.EventToMessage(ev, "test")
	require.NoError(t, err)
	raw, _ := msg.AsStructuredMut()
	env := raw.(map[string]any)
	assert.Equal(t, "u", env["op"])
	assert.Equal(t, map[string]any{"ID": float64(1), "NAME": "Bob"}, env["before"])
	assert.Equal(t, map[string]any{"ID": float64(1), "NAME": "Robert"}, env["after"])
}

func TestEventToMessageDelete(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema: "HR", Table: "EMPLOYEES",
		Operation: replication.OpTypeDelete,
		LogPos:    replication.NewLogPos(77),
		Timestamp: time.Now(),
		Data:      map[string]any{"ID": float64(5), "NAME": "Charlie"},
	}
	msg, err := saphana.EventToMessage(ev, "test")
	require.NoError(t, err)
	raw, _ := msg.AsStructuredMut()
	env := raw.(map[string]any)
	assert.Equal(t, "d", env["op"])
	assert.Equal(t, map[string]any{"ID": float64(5), "NAME": "Charlie"}, env["before"])
	assert.Nil(t, env["after"])
}

func TestEventToMessageSnapshot(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema: "HR", Table: "EMPLOYEES",
		Operation: replication.OpTypeRead,
		LogPos:    replication.LogPos(0),
		Timestamp: time.Now(),
		Data:      map[string]any{"ID": float64(1)},
	}
	msg, err := saphana.EventToMessage(ev, "test")
	require.NoError(t, err)
	raw, _ := msg.AsStructuredMut()
	env := raw.(map[string]any)
	assert.Equal(t, "r", env["op"])
	src := env["source"].(map[string]any)
	assert.Equal(t, "true", src["snapshot"])
	assert.Empty(t, src["lsn"])
}

func TestEventToMessageHeartbeat(t *testing.T) {
	ev := replication.ChangeEvent{
		Operation: replication.OpTypeHeartbeat,
		Timestamp: time.Now(),
	}
	msg, err := saphana.EventToMessage(ev, "test")
	require.NoError(t, err)
	raw, _ := msg.AsStructuredMut()
	env := raw.(map[string]any)
	assert.Equal(t, "hb", env["op"])
	_, hasAfter := env["after"]
	assert.False(t, hasAfter)
	op, ok := msg.MetaGet("saphana_op")
	require.True(t, ok)
	assert.Equal(t, "hb", op)
}

func TestSplitQualTable(t *testing.T) {
	cases := []struct {
		input      string
		wantSchema string
		wantTable  string
		wantErr    bool
	}{
		{"HR.EMPLOYEES", "HR", "EMPLOYEES", false},
		{"SALES.ORDERS", "SALES", "ORDERS", false},
		{"NO_DOT", "", "", true},
		{".MISSING_SCHEMA", "", "", true},
		{"MISSING_TABLE.", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			s, tbl, err := saphana.SplitQualTable(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantSchema, s)
				assert.Equal(t, tc.wantTable, tbl)
			}
		})
	}
}

func TestColumnNames(t *testing.T) {
	cols := []saphana.ColumnInfo{
		{Name: "ID"},
		{Name: "NAME"},
		{Name: "DEPT"},
	}
	assert.Equal(t, []string{"ID", "NAME", "DEPT"}, saphana.ColumnNames(cols))
}
