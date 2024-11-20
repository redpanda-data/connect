// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import "github.com/go-mysql-org/go-mysql/mysql"

// MessageOperation is a string type specifying message opration
type MessageOperation string

const (
	// MessageOperationInsert represents insert statement in mysql binlog
	MessageOperationInsert MessageOperation = "insert"
	// MessageOperationUpdate represents update statement in mysql binlog
	MessageOperationUpdate MessageOperation = "update"
	// MessageOperationDelete represents delete statement in mysql binlog
	MessageOperationDelete MessageOperation = "delete"
)

// MessageType is a base string type defining a type of the message
type MessageType string

const (
	// MessageTypeSnapshot occures when plugin is processing existing snapshot data
	MessageTypeSnapshot MessageType = "snapshot"
	// MessageTypeStreaming occures when plugin is processing data from the binlog
	MessageTypeStreaming MessageType = "streaming"
)

// MessageEvent represents a message from mysql cdc plugin
type MessageEvent struct {
	Row       map[string]any   `json:"row"`
	Table     string           `json:"table"`
	Operation MessageOperation `json:"operation"`
	Type      MessageType      `json:"type"`
	Position  *mysql.Position  `json:"position"`
}
