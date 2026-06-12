// Package replication implements the SAP HANA CDC replication layer.
// Change events are captured via trigger-based change tables (_RPCN_CDC.CHANGES)
// following the same pattern as the DB2 CDC connector (internal/impl/db2/replication).
package replication

import (
	"fmt"
	"strconv"
	"time"
)

// LogPos is the change table sequence ID (_RPCN_CDC.CHANGES.ID, BIGINT GENERATED ALWAYS AS IDENTITY).
// Zero value means "no position" (snapshot rows, uninitialized state).
// Analogous to DB2's CSN and Oracle's SCN.
type LogPos uint64

// NewLogPos creates a non-null LogPos from a positive ID.
func NewLogPos(id uint64) LogPos { return LogPos(id) }

// IsNull returns true for the zero value (uninitialized / snapshot context).
func (p LogPos) IsNull() bool { return p == 0 }

// String returns the decimal string representation, or "" for null.
func (p LogPos) String() string {
	if p.IsNull() {
		return ""
	}
	return strconv.FormatUint(uint64(p), 10)
}

// Compare returns -1, 0, or 1.
func (p LogPos) Compare(other LogPos) int {
	switch {
	case uint64(p) < uint64(other):
		return -1
	case uint64(p) > uint64(other):
		return 1
	default:
		return 0
	}
}

// OpType represents the CDC operation category.
type OpType string

const (
	// OpTypeRead represents an initial snapshot row.
	OpTypeRead OpType = "read"
	// OpTypeInsert represents an INSERT statement.
	OpTypeInsert OpType = "insert"
	// OpTypeUpdate represents an UPDATE statement.
	OpTypeUpdate OpType = "update"
	// OpTypeDelete represents a DELETE statement.
	OpTypeDelete OpType = "delete"
	// OpTypeHeartbeat represents a liveness signal when idle.
	OpTypeHeartbeat OpType = "heartbeat"
)

// DebeziumCode returns the single-character Debezium op code.
func (o OpType) DebeziumCode() string {
	switch o {
	case OpTypeRead:
		return "r"
	case OpTypeInsert:
		return "c"
	case OpTypeUpdate:
		return "u"
	case OpTypeDelete:
		return "d"
	case OpTypeHeartbeat:
		return "hb"
	default:
		return string(o)
	}
}

// ColumnMeta holds lightweight type metadata for schema construction.
type ColumnMeta struct {
	Name     string
	TypeName string // HANA DATA_TYPE_NAME from SYS.TABLE_COLUMNS
	Position int    // POSITION (1-based)
	Nullable bool   // IS_NULLABLE = 'TRUE'
}

// ChangeEvent is a single captured change from a HANA table.
// The Data field holds the after-image for INSERT/UPDATE and the
// before-image for DELETE. BeforeData is only populated for UPDATE.
type ChangeEvent struct {
	Schema     string
	Table      string
	Operation  OpType
	LogPos     LogPos         // ID from _RPCN_CDC.CHANGES; IsNull() for snapshot
	Timestamp  time.Time      // OP_TIME from change table
	Data       map[string]any // after-image (INSERT/UPDATE) or row (DELETE/READ)
	BeforeData map[string]any // before-image (UPDATE only; nil otherwise)
	PKColumns  []string       // non-empty for snapshot rows (used in idempotency key)
}

// QualKey returns "SCHEMA.TABLE" for use as a map key.
func (e ChangeEvent) QualKey() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}
