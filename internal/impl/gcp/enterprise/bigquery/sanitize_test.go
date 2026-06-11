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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"already valid", "my_table", "my_table"},
		{"topic with dots", "events.user.created", "events_user_created"},
		{"topic with hyphens", "my-topic-v2", "my_topic_v2"},
		{"slash to underscore", "events/v2/created", "events_v2_created"},
		{"whitespace to underscore", "my table", "my_table"},
		{"leading digit", "42_table", "_42_table"},
		{"strip invalid chars", "table@#$name", "tablename"},
		{"collapse consecutive separators", "a---b...c", "a_b_c"},
		{"trim trailing underscores", "table___", "table"},
		{"mixed rules", "1st.user-name!!", "_1st_user_name"},
		{"empty string", "", ""},
		{"all invalid", "@#$%", ""},
		{"unicode stripped", "table_\u00e9", "table"},
		{"max table length", strings.Repeat("a", 1100), strings.Repeat("a", 1024)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, sanitizeTableName(tc.input))
		})
	}
}

func TestSanitizeTableNameNoConsecutiveUnderscoreRescan(t *testing.T) {
	// Pathological input the previous strings.Contains/ReplaceAll loop would
	// rescan O(n) times. The single-pass implementation should handle it
	// linearly and return "" (only separators, nothing usable).
	assert.Empty(t, sanitizeTableName(strings.Repeat("_", 10000)))
}
