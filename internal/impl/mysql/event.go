// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type position = mysql.Position

// MessageOperation is a string type specifying message operation
type MessageOperation string

const (
	// MessageOperationRead represents read from snapshot
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
	Position  *position        `json:"position"`
}

func binlogPositionToString(pos position) string {
	// Pad the position so this string is lexicographically ordered.
	return fmt.Sprintf("%s@%08X", pos.Name, pos.Pos)
}

func parseBinlogPosition(str string) (pos position, err error) {
	idx := strings.LastIndexByte(str, '@')
	if idx == -1 {
		err = fmt.Errorf("invalid binlog string: %s", str)
		return
	}
	pos.Name = str[:idx]
	var offset uint64
	offset, err = strconv.ParseUint(str[idx+1:], 16, 32)
	pos.Pos = uint32(offset)
	if err != nil {
		err = fmt.Errorf("invalid binlog string offset: %w", err)
	}
	return
}
