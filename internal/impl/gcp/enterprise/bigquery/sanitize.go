// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"strings"
	"unicode"
)

const maxFieldNameLength = 300

// sanitizeFieldName converts an arbitrary string into a valid BigQuery column
// identifier. BigQuery column names must start with a letter or underscore,
// contain only letters, digits, and underscores, and be at most 300 characters.
func sanitizeFieldName(name string) string {
	if name == "" {
		return ""
	}

	var sb strings.Builder
	sb.Grow(len(name))
	for _, r := range name {
		switch {
		case r == '-' || r == '.' || r == '/' || unicode.IsSpace(r):
			sb.WriteByte('_')
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_':
			sb.WriteRune(r)
		}
	}

	result := sb.String()

	// Collapse runs of consecutive underscores.
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}

	// Trim trailing underscores.
	result = strings.TrimRight(result, "_")

	if result == "" {
		return ""
	}

	// Prepend underscore if the name starts with a digit.
	if result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}

	if len(result) > maxFieldNameLength {
		result = result[:maxFieldNameLength]
	}

	return result
}

// sanitizeTableName converts an arbitrary string (e.g. a Kafka topic name)
// into a valid BigQuery table identifier using the same rules as
// sanitizeFieldName.
func sanitizeTableName(name string) string {
	return sanitizeFieldName(name)
}
