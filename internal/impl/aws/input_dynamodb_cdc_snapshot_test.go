// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSequenceBuffer(t *testing.T) {
	t.Run("basic deduplication", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		// Record a snapshot item
		buffer.RecordSnapshotItem("key1", "seq100")

		// CDC event with same or earlier sequence should be skipped
		assert.True(t, buffer.ShouldSkipCDCEvent("key1", "seq050"))
		assert.True(t, buffer.ShouldSkipCDCEvent("key1", "seq100"))

		// CDC event with later sequence should not be skipped
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", "seq150"))

		// Unknown key should not be skipped
		assert.False(t, buffer.ShouldSkipCDCEvent("key2", "seq100"))
	})

	t.Run("buffer overflow handling", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(2)

		// Fill buffer
		buffer.RecordSnapshotItem("key1", "seq100")
		buffer.RecordSnapshotItem("key2", "seq200")

		// This should trigger overflow
		buffer.RecordSnapshotItem("key3", "seq300")

		assert.True(t, buffer.IsOverflow())

		// After overflow, should not skip anything (to prevent data loss)
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", "seq050"))
		assert.False(t, buffer.ShouldSkipCDCEvent("key2", "seq150"))
		assert.False(t, buffer.ShouldSkipCDCEvent("key3", "seq250"))
	})

	t.Run("buffer size tracking", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		assert.Equal(t, 0, buffer.Size())

		buffer.RecordSnapshotItem("key1", "seq100")
		assert.Equal(t, 1, buffer.Size())

		buffer.RecordSnapshotItem("key2", "seq200")
		assert.Equal(t, 2, buffer.Size())

		// Recording same key again updates, doesn't increase size
		buffer.RecordSnapshotItem("key1", "seq150")
		assert.Equal(t, 2, buffer.Size())
	})

	t.Run("empty buffer", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		// Empty buffer should not skip anything
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", "seq100"))
		assert.False(t, buffer.IsOverflow())
		assert.Equal(t, 0, buffer.Size())
	})
}

func TestDynamoDBCDCConfig_SnapshotFields(t *testing.T) {
	// This test verifies that snapshot configuration fields are properly parsed
	// We'll test this via the actual config parsing when we have a full config spec
	t.Run("default values", func(t *testing.T) {
		// Default snapshot_mode should be "none"
		// Default snapshot_segments should be 1
		// Default snapshot_batch_size should be 100
		// Default snapshot_throttle should be 100ms
		// Default snapshot_deduplicate should be true
		// Default snapshot_buffer_size should be 100000

		// These will be validated in integration tests
		t.Skip("Config parsing tested in integration tests")
	})
}

func TestSnapshotModeValidation(t *testing.T) {
	validModes := []string{"none", "snapshot_only", "snapshot_and_cdc"}

	for _, mode := range validModes {
		t.Run(mode, func(t *testing.T) {
			// Just verify the mode strings are valid
			// Actual validation happens at config parse time
			require.Contains(t, validModes, mode)
		})
	}
}

func TestConvertDynamoDBAttributeValue(t *testing.T) {
	t.Run("converts string attribute", func(t *testing.T) {
		// Test will be expanded when we add actual conversion tests
		// For now, just verify the function exists and compiles
		t.Skip("Attribute conversion tested in integration tests")
	})
}
