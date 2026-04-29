// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFindCaseOnlyDuplicate covers the create-time guard that prevents an
// inbound record with two keys differing only in case from being committed
// as two separate iceberg columns under case-insensitive matching, which
// would either be rejected by the catalog or corrupt the schema.
func TestFindCaseOnlyDuplicate(t *testing.T) {
	t.Run("no duplicates returns false", func(t *testing.T) {
		record := map[string]any{"id": 1, "name": "alice", "email": "x@y.z"}
		_, _, ok := findCaseOnlyDuplicate(record)
		assert.False(t, ok)
	})

	t.Run("uppercase and lowercase duplicate", func(t *testing.T) {
		record := map[string]any{"id": 1, "ID": 2}
		a, b, ok := findCaseOnlyDuplicate(record)
		assert.True(t, ok)
		// Order is non-deterministic over map iteration; assert the pair.
		assert.ElementsMatch(t, []string{"id", "ID"}, []string{a, b})
	})

	t.Run("mixed-case duplicate", func(t *testing.T) {
		record := map[string]any{"User_Id": 1, "user_id": 2}
		a, b, ok := findCaseOnlyDuplicate(record)
		assert.True(t, ok)
		pair := []string{strings.ToLower(a), strings.ToLower(b)}
		assert.Equal(t, []string{"user_id", "user_id"}, pair)
	})

	t.Run("empty record returns false", func(t *testing.T) {
		_, _, ok := findCaseOnlyDuplicate(map[string]any{})
		assert.False(t, ok)
	})
}
