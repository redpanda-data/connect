// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// Operation represents a LogMiner operation type
type Operation int64

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

// operationFromCode converts an operation code integer into an Operation type
func operationFromCode(code int) Operation {
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

// Scan implements the DB Scanner interface.
func (op *Operation) Scan(src any) error {
	if src == nil { // db returned nil, CDC record may not exist yet
		op = nil
		return nil
	}
	switch v := src.(type) {
	case int:
		*op = operationFromCode(v)
	case string:
		if val, err := strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("parsing operation code: %w", err)
		} else {
			*op = operationFromCode(int(val))
		}
	default:
		return fmt.Errorf("cannot scan %T to operation code", src)
	}
	return nil
}

// DMLEvent represents a parsed DML (Data Manipulation Language) operation
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
	SCN           uint64
	SQLRedo       sql.NullString
	Data          map[string]any
	Operation     Operation
	TableName     sql.NullString
	SchemaName    sql.NullString
	Timestamp     time.Time
	TransactionID string
}
