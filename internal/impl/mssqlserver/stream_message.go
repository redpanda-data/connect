// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"encoding/hex"
	"fmt"
)

// LSN represents a Microsoft SQL Server Log Sequence Number
type LSN []byte

// Scan implements the Scanner interface.
func (lsn *LSN) Scan(src any) error {
	if src == nil { // db returned nil, CDC record may not exist yet
		*lsn = nil
		return nil
	}

	switch v := src.(type) {
	case []byte:
		// *lsn = append((*lsn)[:0], v...) // reuse underlying array?
		*lsn = LSN(v)
	default:
		*lsn = nil
		return fmt.Errorf("cannot scan %T to LSN", src)
	}

	return nil
}

// String formats the LSN to the hexidecimal equivalent
func (lsn LSN) String() string {
	if len(lsn) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(lsn)
}

// StreamMode represents the mode of the stream at the time of the message
type StreamMode string

const (
	// StreamModeStreaming indicates that the stream is in streaming mode
	StreamModeStreaming StreamMode = "streaming"
	// StreamModeSnapshot indicates that the stream is in snapshot mode
	StreamModeSnapshot StreamMode = "snapshot"
)

// OpType is the type of operation from the database
type OpType int

const (
	// MessageOperationRead represents a snapshot read operation
	MessageOperationRead OpType = 0
	// MessageOperationDelete represents a delete operation from MS SQL Server's CDC table
	MessageOperationDelete OpType = 1
	// MessageOperationInsert represents a insert operation from MS SQL Server's CDC table
	MessageOperationInsert OpType = 2
	// MessageOperationUpdateBefore represents a update (before) operation from MS SQL Server's CDC table
	MessageOperationUpdateBefore OpType = 3
	// MessageOperationUpdateAfter represents a update (after) operation from MS SQL Server's CDC table
	MessageOperationUpdateAfter OpType = 4
	// MessageOperationMerge represents a merge operation from MS SQL Server's CDC table
	MessageOperationMerge OpType = 5
)

// MessageEvent represents a single change from Table's change table in the database
type MessageEvent struct {
	LSN       LSN    `json:"start_lsn"`
	Operation int    `json:"operation"`
	Table     string `json:"table"`
	Data      any    `json:"data"`
}
