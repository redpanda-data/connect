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
	"unicode/utf8"
)

var (
	errEmptyTableName        = errors.New("empty table name")
	errInvalidTableLength    = errors.New("invalid table length")
	errInvalidTableStartChar = errors.New("invalid start char in mysql table name")
	errInvalidTableName      = errors.New("invalid table name")
)

// isExtendedIdentifierRune reports whether r falls in the extended range MySQL
// permits in unquoted identifiers, U+0080 to U+FFFF. Supplementary characters
// (U+10000 and above) are not permitted.
//
// See https://dev.mysql.com/doc/refman/8.4/en/identifiers.html
func isExtendedIdentifierRune(r rune) bool {
	return r >= 0x80 && r <= 0xFFFF
}

// isIdentifierStartRune reports whether r may begin an unquoted table name.
func isIdentifierStartRune(r rune) bool {
	switch {
	case r == '_':
		return true
	case 'a' <= r && r <= 'z':
		return true
	case 'A' <= r && r <= 'Z':
		return true
	default:
		return isExtendedIdentifierRune(r)
	}
}

// isIdentifierRune reports whether r may appear after the first character of an
// unquoted table name.
func isIdentifierRune(r rune) bool {
	switch {
	case '0' <= r && r <= '9':
		return true
	case r == '$':
		return true
	default:
		return isIdentifierStartRune(r)
	}
}

func validateTableName(tableName string) error {
	// Check if empty
	if tableName == "" {
		return errEmptyTableName
	}

	// Reject malformed input up front. Ranging over a string yields
	// utf8.RuneError for an invalid byte, which sits inside the extended range
	// and would otherwise be accepted.
	if !utf8.ValidString(tableName) {
		return errInvalidTableName
	}

	// Check length
	if utf8.RuneCountInString(tableName) > 64 {
		return errInvalidTableLength
	}

	for i, r := range tableName {
		// i is a byte offset, so it is only zero for the first rune.
		if i == 0 {
			// Check if starts with a valid character
			if !isIdentifierStartRune(r) {
				return errInvalidTableStartChar
			}
			continue
		}

		// Check if contains only valid characters
		if !isIdentifierRune(r) {
			return errInvalidTableName
		}
	}

	return nil
}
