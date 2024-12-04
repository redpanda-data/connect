// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

// StreamMode represents the mode of the stream at the time of the message
type StreamMode string

const (
	// StreamModeStreaming indicates that the stream is in streaming mode
	StreamModeStreaming StreamMode = "streaming"
	// StreamModeSnapshot indicates that the stream is in snapshot mode
	StreamModeSnapshot StreamMode = "snapshot"
)

// OpType is the type of operation from the database
type OpType string

const (
	// InsertOpType is a database insert
	InsertOpType OpType = "insert"
	// UpdateOpType is a database update
	UpdateOpType OpType = "update"
	// DeleteOpType is a database delete
	DeleteOpType OpType = "delete"
	// BeginOpType is a database transaction begin
	BeginOpType OpType = "begin"
	// CommitOpType is a database transaction commit
	CommitOpType OpType = "commit"
)

// StreamMessage represents a single change from the database
type StreamMessage struct {
	LSN       *string    `json:"lsn"`
	Operation OpType     `json:"operation"`
	Schema    string     `json:"schema"`
	Table     string     `json:"table"`
	Mode      StreamMode `json:"mode"`
	// For deleted messages - there will be old changes if replica identity set to full or empty changes
	Data any `json:"data"`
}
