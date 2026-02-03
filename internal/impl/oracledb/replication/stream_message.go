// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"time"
)

// SCN represents an Oracle System Change Number (SCN).
type SCN uint64

// InvalidSCN represents an SCN value that's unset or invalid.
const InvalidSCN SCN = 0

// String formats the SCN to a string for logging.
func (scn SCN) String() string {
	return strconv.FormatUint(uint64(scn), 10)
}

// Bytes converts a uint64 value SCN into a byte slice.
func (scn SCN) Bytes() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(scn))
	return b
}

// IsValid verifies that the SCN is considered a valid SCN.
func (scn SCN) IsValid() bool {
	return scn > 0
}

// ParseSCN parses a string into an SCN value.
func ParseSCN(s string) (SCN, error) {
	if s == "" {
		return InvalidSCN, nil
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return InvalidSCN, fmt.Errorf("failed to parse SCN from string %q: %w", s, err)
	}
	return SCN(val), nil
}

// SCNFromBytes converts a byte slice to an SCN value
func SCNFromBytes(b []byte) (SCN, error) {
	if len(b) == 0 {
		return InvalidSCN, nil
	}
	if len(b) != 8 {
		return InvalidSCN, fmt.Errorf("expected 8 bytes for SCN, got %d", len(b))
	}
	return SCN(binary.LittleEndian.Uint64(b)), nil
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
	SCN       SCN       `json:"start_scn"`
	Operation string    `json:"operation"`
	Schema    string    `json:"schema"`
	Table     string    `json:"table"`
	Data      any       `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}
