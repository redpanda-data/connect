package replication_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func TestTriggerNameFormat(t *testing.T) {
	assert.Equal(t, "_RPCN_HR_EMPLOYEES_ins", replication.TriggerName("HR", "EMPLOYEES", "ins"))
}

func TestTriggerNameUppercase(t *testing.T) {
	assert.Equal(t, "_RPCN_HR_EMPLOYEES_upd", replication.TriggerName("hr", "employees", "upd"))
}

func TestTriggerNameMaxLength(t *testing.T) {
	n := replication.TriggerName("S", strings.Repeat("A", 120), "del")
	assert.LessOrEqual(t, len(n), 127)
}

func TestBuildInsertTriggerSQL(t *testing.T) {
	s := replication.BuildInsertTriggerSQL("HR", "EMPLOYEES", []string{"ID", "NAME", "DEPT"}, "ID")
	assert.Contains(t, s, `AFTER INSERT ON "HR"."EMPLOYEES"`)
	assert.Contains(t, s, "FOR EACH ROW")
	assert.Contains(t, s, "_RPCN_CDC.CHANGES")
	assert.Contains(t, s, "'I'")
	// Column refs are now double-quoted to prevent SQL injection via column names.
	assert.Contains(t, s, `:new."ID"`)
	assert.Contains(t, s, `:new."NAME"`)
	assert.Contains(t, s, `'ID' VALUE :new."ID"`)
}

func TestBuildUpdateTriggerSQL(t *testing.T) {
	s := replication.BuildUpdateTriggerSQL("HR", "EMPLOYEES", []string{"ID", "NAME"}, "ID")
	assert.Contains(t, s, `AFTER UPDATE ON "HR"."EMPLOYEES"`)
	assert.Contains(t, s, "'U'")
	assert.Contains(t, s, `:old."ID"`)
	assert.Contains(t, s, `:new."ID"`)
	assert.Contains(t, s, "OLD_VALUES")
	assert.Contains(t, s, "NEW_VALUES")
}

func TestBuildDeleteTriggerSQL(t *testing.T) {
	s := replication.BuildDeleteTriggerSQL("HR", "EMPLOYEES", []string{"ID", "NAME"}, "ID")
	assert.Contains(t, s, `AFTER DELETE ON "HR"."EMPLOYEES"`)
	assert.Contains(t, s, "'D'")
	assert.Contains(t, s, `:old."ID"`)
	assert.Contains(t, s, "OLD_VALUES")
	assert.NotContains(t, s, "NEW_VALUES")
}

func TestBuildInsertTriggerNoPK(t *testing.T) {
	s := replication.BuildInsertTriggerSQL("HR", "NO_PK_TABLE", []string{"A", "B"})
	assert.Contains(t, s, "NULL")
}

func TestValidateTargetTableValid(t *testing.T) {
	require.NoError(t, replication.ValidateTargetTable("HR", "EMPLOYEES"))
	require.NoError(t, replication.ValidateTargetTable("_RPCN_CDC", "CHANGES"))
}

func TestValidateTargetTableInvalid(t *testing.T) {
	require.Error(t, replication.ValidateTargetTable("", "EMPLOYEES"))
	require.Error(t, replication.ValidateTargetTable("HR", ""))
	require.Error(t, replication.ValidateTargetTable("HR; DROP", "T"))
}
