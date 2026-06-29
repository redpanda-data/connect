package replication_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func TestLogPosZero(t *testing.T) {
	var p replication.LogPos
	assert.True(t, p.IsNull())
	assert.Empty(t, p.String())
}

func TestLogPosNonZero(t *testing.T) {
	p := replication.NewLogPos(12345)
	assert.False(t, p.IsNull())
	assert.Equal(t, "12345", p.String())
}

func TestLogPosCompare(t *testing.T) {
	a := replication.NewLogPos(100)
	b := replication.NewLogPos(200)
	assert.Equal(t, -1, a.Compare(b))
	assert.Equal(t, 0, a.Compare(a))
	assert.Equal(t, 1, b.Compare(a))
}

func TestLogPosLargeValue(t *testing.T) {
	// Confirm uint64 handles values beyond int32/int64 max cleanly.
	p := replication.NewLogPos(^uint64(0)) // max uint64
	assert.False(t, p.IsNull())
	assert.Equal(t, "18446744073709551615", p.String())
}

func TestOpTypeDebeziumCodes(t *testing.T) {
	cases := map[replication.OpType]string{
		replication.OpTypeRead:      "r",
		replication.OpTypeInsert:    "c",
		replication.OpTypeUpdate:    "u",
		replication.OpTypeDelete:    "d",
		replication.OpTypeHeartbeat: "hb",
	}
	for op, want := range cases {
		t.Run(string(op), func(t *testing.T) {
			assert.Equal(t, want, op.DebeziumCode())
		})
	}
}

func TestOpTypeUnknownPassthrough(t *testing.T) {
	custom := replication.OpType("custom_op")
	assert.Equal(t, "custom_op", custom.DebeziumCode())
}

func TestChangeEventInsert(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema:    "HR",
		Table:     "EMPLOYEES",
		Operation: replication.OpTypeInsert,
		LogPos:    replication.NewLogPos(42),
		Timestamp: time.Now(),
		Data:      map[string]any{"ID": 1, "NAME": "Alice"},
	}
	assert.Nil(t, ev.BeforeData)
	assert.Equal(t, "Alice", ev.Data["NAME"])
	assert.Equal(t, "HR.EMPLOYEES", ev.QualKey())
}

func TestChangeEventUpdate(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema:     "HR",
		Table:      "EMPLOYEES",
		Operation:  replication.OpTypeUpdate,
		LogPos:     replication.NewLogPos(99),
		Timestamp:  time.Now(),
		Data:       map[string]any{"ID": 1, "NAME": "Robert"},
		BeforeData: map[string]any{"ID": 1, "NAME": "Bob"},
	}
	assert.Equal(t, "HR.EMPLOYEES", ev.QualKey())
	assert.Equal(t, replication.OpTypeUpdate, ev.Operation)
	assert.Equal(t, "99", ev.LogPos.String())
	assert.False(t, ev.Timestamp.IsZero())
	require.NotNil(t, ev.BeforeData)
	assert.Equal(t, "Bob", ev.BeforeData["NAME"])
	assert.Equal(t, "Robert", ev.Data["NAME"])
}

func TestChangeEventDelete(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema:    "HR",
		Table:     "EMPLOYEES",
		Operation: replication.OpTypeDelete,
		LogPos:    replication.NewLogPos(77),
		Timestamp: time.Now(),
		Data:      map[string]any{"ID": 5, "NAME": "Charlie"},
	}
	assert.Equal(t, "HR.EMPLOYEES", ev.QualKey())
	assert.Equal(t, replication.OpTypeDelete, ev.Operation)
	assert.Equal(t, "77", ev.LogPos.String())
	assert.False(t, ev.Timestamp.IsZero())
	assert.Equal(t, "Charlie", ev.Data["NAME"])
	assert.Nil(t, ev.BeforeData)
}

func TestChangeEventSnapshot(t *testing.T) {
	ev := replication.ChangeEvent{
		Schema:    "HR",
		Table:     "EMPLOYEES",
		Operation: replication.OpTypeRead,
		LogPos:    replication.LogPos(0),
		Timestamp: time.Now(),
		Data:      map[string]any{"ID": 1},
		PKColumns: []string{"ID"},
	}
	assert.Equal(t, "HR.EMPLOYEES", ev.QualKey())
	assert.Equal(t, replication.OpTypeRead, ev.Operation)
	assert.True(t, ev.LogPos.IsNull(), "snapshot events have null LogPos")
	assert.Empty(t, ev.LogPos.String())
	assert.False(t, ev.Timestamp.IsZero())
	assert.Equal(t, 1, ev.Data["ID"])
	assert.Equal(t, []string{"ID"}, ev.PKColumns)
}

func TestChangeEventQualKey(t *testing.T) {
	cases := []struct {
		schema, table, want string
	}{
		{"HR", "EMPLOYEES", "HR.EMPLOYEES"},
		{"SALES", "ORDERS", "SALES.ORDERS"},
		{"", "", "."},
	}
	for _, tc := range cases {
		ev := replication.ChangeEvent{Schema: tc.schema, Table: tc.table}
		assert.Equal(t, tc.want, ev.QualKey())
	}
}

func TestColumnMeta(t *testing.T) {
	cm := replication.ColumnMeta{
		Name:     "ID",
		TypeName: "INTEGER",
		Position: 1,
		Nullable: false,
	}
	assert.Equal(t, "ID", cm.Name)
	assert.Equal(t, "INTEGER", cm.TypeName)
	assert.Equal(t, 1, cm.Position)
	assert.False(t, cm.Nullable)
}
