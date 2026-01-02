// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"encoding/hex"
	"fmt"
)

// SCN represents an Oracle System Change Number
type SCN []byte

// Scan implements the Scanner interface.
func (scn *SCN) Scan(src any) error {
	if src == nil { // db returned nil, CDC record may not exist yet
		*scn = nil
		return nil
	}

	switch v := src.(type) {
	case []byte:
		if len(v) == 0 {
			*scn = nil
		} else {
			// copy to avoid driver buffer reuse
			*scn = append((*scn)[:0], v...)
		}
		return nil
	case string:
		// Oracle might return SCN as string
		if v == "" {
			*scn = nil
		} else {
			*scn = []byte(v)
		}
		return nil
	case int64:
		// Oracle might return SCN as number
		*scn = []byte(fmt.Sprintf("%d", v))
		return nil
	default:
		*scn = nil
		return fmt.Errorf("cannot scan %T to SCN", src)
	}
}

// String formats the SCN to the hexadecimal equivalent.
func (scn SCN) String() string {
	if len(scn) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(scn)
}

// OpType is the type of operation from the database.
type OpType int

const (
	// MessageOperationRead represents a snapshot read operation
	MessageOperationRead OpType = 0
	// MessageOperationDelete represents a delete operation from Oracle's CDC table
	MessageOperationDelete OpType = 1
	// MessageOperationInsert represents an insert operation from Oracle's CDC table
	MessageOperationInsert OpType = 2
	// MessageOperationUpdateBefore represents an update (before) operation from Oracle's CDC table
	MessageOperationUpdateBefore OpType = 3
	// MessageOperationUpdateAfter represents an update (after) operation from Oracle's CDC table
	MessageOperationUpdateAfter OpType = 4
)

// String converts the operation type to a string equivalent.
func (op OpType) String() string {
	switch op {
	case MessageOperationRead:
		return "read"
	case MessageOperationDelete:
		return "delete"
	case MessageOperationInsert:
		return "insert"
	case MessageOperationUpdateBefore:
		return "update_before"
	case MessageOperationUpdateAfter:
		return "update_after"
	default:
		return fmt.Sprintf("unknown(%d)", int(op))
	}
}

// MessageEvent represents a single change from Table's change table in the database.
type MessageEvent struct {
	SCN       SCN    `json:"start_scn"`
	Operation string `json:"operation"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Data      any    `json:"data"`
}
