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
	errEmptyTableName        = errors.New("empty table name")
	errInvalidTableLength    = errors.New("invalid table length")
	errInvalidTableStartChar = errors.New("invalid start char in mysql table name")
	errInvalidTableName      = errors.New("invalid table name")
)

func validateTableName(tableName string) error {
	// Check if empty
	if tableName == "" {
		return errEmptyTableName
	}

	// Check length
	if utf8.RuneCountInString(tableName) > 64 {
		return errInvalidTableLength
	}

	// Check if starts with a valid character
	if matched, _ := regexp.MatchString(`^[a-zA-Z_]`, tableName); !matched {
		return errInvalidTableStartChar
	}

	// Check if contains only valid characters
	if matched, _ := regexp.MatchString(`^[a-zA-Z0-9_$]+$`, tableName); !matched {
		return errInvalidTableName
	}

	return nil
}
