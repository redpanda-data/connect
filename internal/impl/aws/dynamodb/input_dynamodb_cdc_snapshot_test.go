// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestSnapshotSequenceBufferMixedTimeZones(t *testing.T) {
	// The snapshot start comes from time.Now() (host zone). The CDC time comes from the SDK (UTC).
	snapshotStart := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC).In(time.FixedZone("UTC+1", 3600))
	cdcTime := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)

	buffer := newSnapshotSequenceBuffer(100)
	buffer.RecordSnapshotItem("key1", snapshotStart.Format(time.RFC3339Nano))

	assert.False(t, buffer.ShouldSkipCDCEvent("key1", cdcTime.Format(time.RFC3339Nano)))
}
