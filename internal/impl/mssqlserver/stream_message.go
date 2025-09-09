// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

// LSN represents a Microsoft SQL Server Log Sequence Number
type LSN []byte

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
	Table         string         `json:"table"`
	StartLSN      LSN            `json:"start_lsn"`
	EndLSN        LSN            `json:"end_lsn"`
	SequenceValue []byte         `json:"sequence_value"`
	Operation     OpType         `json:"operation"`
	UpdateMask    []byte         `json:"update_mask"`
	Data          map[string]any `json:"data"`
	CommandID     int            `json:"command_id"`
}
