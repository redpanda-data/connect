// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"errors"
	"regexp"
	"unicode/utf8"
)

var (
	ErrEmptyTableName        = errors.New("empty table name")
	ErrInvalidTableLength    = errors.New("invalid table length")
	ErrInvalidTableStartChar = errors.New("invalid start char in mysql table name")
	ErrInvalidTableName      = errors.New("invalid table name")
)

func validateTableName(tableName string) error {
	// Check if empty
	if tableName == "" {
		return ErrEmptyTableName
	}

	// Check length
	if utf8.RuneCountInString(tableName) > 64 {
		return ErrInvalidTableLength
	}

	// Check if starts with a valid character
	if matched, _ := regexp.MatchString(`^[a-zA-Z_]`, tableName); !matched {
		return ErrInvalidTableStartChar
	}

	// Check if contains only valid characters
	if matched, _ := regexp.MatchString(`^[a-zA-Z0-9_$]+$`, tableName); !matched {
		return ErrInvalidTableName
	}

	return nil
}
