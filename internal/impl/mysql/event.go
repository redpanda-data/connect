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
	// MessageOperationInsert represents read from snapshot
	MessageOperationRead MessageOperation = "read"
	// MessageOperationInsert represents insert statement in mysql binlog
	MessageOperationInsert MessageOperation = "insert"
	// MessageOperationUpdate represents update statement in mysql binlog
	MessageOperationUpdate MessageOperation = "update"
	// MessageOperationDelete represents delete statement in mysql binlog
	MessageOperationDelete MessageOperation = "delete"
)

// MessageEvent represents a message from mysql cdc plugin
type MessageEvent struct {
	Row       map[string]any   `json:"row"`
	Table     string           `json:"table"`
	Operation MessageOperation `json:"operation"`
	Position  *mysql.Position  `json:"position"`
}
