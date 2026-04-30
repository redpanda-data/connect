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

func TestSanitizeFieldName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"already valid", "user_name", "user_name"},
		{"hyphen to underscore", "user-name", "user_name"},
		{"dot to underscore", "user.name", "user_name"},
		{"slash to underscore", "path/to/field", "path_to_field"},
		{"whitespace to underscore", "user name", "user_name"},
		{"leading digit", "123field", "_123field"},
		{"strip invalid chars", "field@#$name", "fieldname"},
		{"collapse consecutive underscores", "a---b...c", "a_b_c"},
		{"trim trailing underscores", "field___", "field"},
		{"mixed rules", "1st.user-name!!", "_1st_user_name"},
		{"empty string", "", ""},
		{"all invalid", "@#$%", ""},
		{"unicode stripped", "field_\u00e9", "field"},
		{"max length", strings.Repeat("a", 350), strings.Repeat("a", 300)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, sanitizeFieldName(tc.input))
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"already valid", "my_table", "my_table"},
		{"topic with dots", "events.user.created", "events_user_created"},
		{"topic with hyphens", "my-topic-v2", "my_topic_v2"},
		{"leading digit", "42_table", "_42_table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, sanitizeTableName(tc.input))
		})
	}
}
