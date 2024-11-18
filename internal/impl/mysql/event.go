// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import "github.com/go-mysql-org/go-mysql/mysql"

type ProcessEventParams struct {
	initValue, incrementValue int
}

type MessageOperation string

const (
	MessageOperationInsert MessageOperation = "insert"
	MessageOperationUpdate MessageOperation = "update"
	MessageOperationDelete MessageOperation = "delete"
)

type MessageType string

const (
	MessageTypeSnapshot  MessageType = "snapshot"
	MessageTypeStreaming MessageType = "streaming"
)

type MessageEvent struct {
	Row       map[string]any   `json:"row"`
	Table     string           `json:"table"`
	Operation MessageOperation `json:"operation"`
	Type      MessageType      `json:"type"`
	Position  *mysql.Position  `json:"position"`
}
