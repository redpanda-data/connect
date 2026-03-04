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
	"errors"
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
	// OpSelectLobLocator represents a SEL_LOB_LOCATOR operation (code 9),
	// which precedes LOB_WRITE events and identifies which column is being written.
	OpSelectLobLocator Operation = 9
	// OpLobWrite represents a LOB_WRITE operation (code 10) that carries a
	// chunk of LOB data to be assembled into the final column value.
	OpLobWrite Operation = 10
	// OpLobTrim represents a LOB_TRIM operation (code 11).
	OpLobTrim Operation = 11
	// OpLobErase represents a LOB_ERASE operation (code 29).
	OpLobErase Operation = 29
)

// String converts the operation type to a string equivalent.
func (op Operation) String() string {
	switch op {
	case OpInsert:
		return "insert"
	case OpDelete:
		return "delete"
	case OpUpdate:
		return "update"
	case OpStart:
		return "start"
	case OpCommit:
		return "commit"
	case OpRollback:
		return "rollback"
	default:
		return fmt.Sprintf("unknown operation (%d)", int64(op))
	}
}

// Scan implements the DB Scanner interface.
func (op *Operation) Scan(src any) error {
	if src == nil {
		return errors.New("no operation found when parsing operation code")
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
	case 9:
		return OpSelectLobLocator
	case 10:
		return OpLobWrite
	case 11:
		return OpLobTrim
	case 29:
		return OpLobErase
	case 36:
		return OpRollback
	default:
		return OpUnknown
	}
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
