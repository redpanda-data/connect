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
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// TransactionID uniquely identifies an Oracle transaction. It is derived from
// Oracle's native XID (RAW(8)) and formatted as "USN.SLOT.SEQ" — the three
// little-endian components of the XID: undo segment number (uint16), slot
// (uint16), and sequence number (uint32).
type TransactionID string

// Scan implements sql.Scanner, decoding Oracle's RAW(8) XID bytes directly
// into the "USN.SLOT.SEQ" string representation.
func (txID *TransactionID) Scan(src any) error {
	if src == nil {
		return errors.New("cannot scan nil into TransactionID")
	}

	switch v := src.(type) {
	case []byte:
		// V$LOGMNR_CONTENTS.XID is RAW(8) https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html
		if len(v) != 8 {
			return fmt.Errorf("V$LOGMNR_CONTENTS.XID expected to be 8 bytes but was %d", len(v))
		}

		usn := binary.LittleEndian.Uint16(v[0:2])
		slot := binary.LittleEndian.Uint16(v[2:4])
		seq := binary.LittleEndian.Uint32(v[4:8])
		*txID = TransactionID(fmt.Sprintf("%d.%d.%d", usn, slot, seq))
	default:
		return fmt.Errorf("cannot scan %T to transaction id", src)
	}
	return nil
}

// String returns the TransactionID in its "USN.SLOT.SEQ" string form.
func (txID TransactionID) String() string {
	return string(txID)
}

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
	// OpSelectLobLocator represents a SELECT_LOB_LOCATOR operation (op 9)
	OpSelectLobLocator Operation = 9
	// OpLobWrite represents a LOB_WRITE operation (op 10)
	OpLobWrite Operation = 10
	// OpLobTrim represents a LOB_TRIM operation (op 11)
	OpLobTrim Operation = 11
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
	case OpSelectLobLocator:
		return "select_lob_locator"
	case OpLobWrite:
		return "lob_write"
	case OpLobTrim:
		return "lob_trim"
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
	case int64:
		*op = operationFromCode(v)
	case string:
		if val, err := strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("parsing operation code: %w", err)
		} else {
			*op = operationFromCode(val)
		}
	default:
		return fmt.Errorf("cannot scan %T to operation code", src)
	}
	return nil
}

// operationFromCode converts an operation code integer into an Operation type
func operationFromCode(code int64) Operation {
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
	case 9:
		return OpSelectLobLocator
	case 10:
		return OpLobWrite
	case 11:
		return OpLobTrim
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
	// OldValues holds the WHERE-clause column values for UPDATE and DELETE events.
	// For LOB-init UPDATE events these are used to identify the source row for PK matching.
	OldValues     map[string]any
	Timestamp     time.Time
	TransactionID TransactionID
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
	TransactionID TransactionID
}
