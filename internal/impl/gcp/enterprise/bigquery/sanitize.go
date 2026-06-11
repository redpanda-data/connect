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

// maxTableNameLength is BigQuery's hard limit for table identifiers.
const maxTableNameLength = 1024

// sanitizeTableName rewrites an arbitrary string (e.g. a Kafka topic name)
// into a valid BigQuery table identifier: starts with letter or underscore,
// contains only ASCII letters, digits, and underscores, max 1024 characters.
// Non-ASCII letters are dropped rather than transliterated — they don't
// round-trip cleanly through the proto-descriptor step that follows. Returns
// "" if the input has no usable characters.
func sanitizeTableName(name string) string {
	if name == "" {
		return ""
	}

	var sb strings.Builder
	sb.Grow(len(name))
	prevUnderscore := false
	for _, r := range name {
		switch {
		case r == '-' || r == '.' || r == '/' || unicode.IsSpace(r) || r == '_':
			// Coalesce separators / explicit underscores into a single
			// underscore in one pass — avoids the O(n^2) re-scan that the
			// previous strings.Contains/ReplaceAll loop required.
			if !prevUnderscore {
				sb.WriteByte('_')
				prevUnderscore = true
			}
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			sb.WriteRune(r)
			prevUnderscore = false
		}
	}

	result := strings.TrimRight(sb.String(), "_")
	if result == "" {
		return ""
	}

	// Prepend underscore if the name starts with a digit.
	if result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}

	if len(result) > maxTableNameLength {
		result = result[:maxTableNameLength]
	}

	return result
}
