package sqlredo

import (
	"database/sql"
	"time"
)

// Operation represents a LogMiner operation type
type Operation int

const (
	// OpUnknown represents an unknown or unsupported operation
	OpUnknown Operation = iota
	// OpInsert represents an INSERT operation
	OpInsert
	// OpDelete represents a DELETE operation
	OpDelete
	// OpUpdate represents an UPDATE operation
	OpUpdate
	// OpStart represents a transaction START operation
	OpStart
	// OpCommit represents a transaction COMMIT operation
	OpCommit
	// OpRollback represents a transaction ROLLBACK operation
	OpRollback
)

func OperationFromCode(code int) Operation {
	switch code {
	case 1:
		return OpInsert
	case 2:
		return OpDelete
	case 3:
		return OpUpdate
	case 6:
		return OpStart
	case 7:
		return OpCommit
	case 36:
		return OpRollback
	default:
		return OpUnknown
	}
}

// DMLEvent represents a parsed DML operation
type DMLEvent struct {
	Operation Operation
	Schema    string
	Table     string
	SQLRedo   string
	Data      map[string]any
	Timestamp time.Time
}

// RedoEvent represents a redo log row from V$LOGMNR_CONTENTS
type RedoEvent struct {
	SCN     int64
	SQLRedo sql.NullString
	Data    map[string]any
	// TODO: Do we need both Operation and OperationCode?
	Operation     Operation
	OperationCode int
	TableName     sql.NullString
	SchemaName    sql.NullString
	Timestamp     time.Time
	TransactionID string
}
