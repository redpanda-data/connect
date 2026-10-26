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
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	t.Run("basic deduplication", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		// Record a snapshot item
		buffer.RecordSnapshotItem("key1", at(100))

		// CDC event with same or earlier time should be skipped
		assert.True(t, buffer.ShouldSkipCDCEvent("key1", at(50)))
		assert.True(t, buffer.ShouldSkipCDCEvent("key1", at(100)))

		// CDC event with later time should not be skipped
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", at(150)))

		// Unknown key should not be skipped
		assert.False(t, buffer.ShouldSkipCDCEvent("key2", at(100)))
	})

	t.Run("buffer overflow handling", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(2)

		// Fill buffer
		buffer.RecordSnapshotItem("key1", at(100))
		buffer.RecordSnapshotItem("key2", at(200))

		// This should trigger overflow
		buffer.RecordSnapshotItem("key3", at(300))

		assert.True(t, buffer.IsOverflow())

		// After overflow, should not skip anything (to prevent data loss)
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", at(50)))
		assert.False(t, buffer.ShouldSkipCDCEvent("key2", at(150)))
		assert.False(t, buffer.ShouldSkipCDCEvent("key3", at(250)))
	})

	t.Run("buffer size tracking", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		assert.Equal(t, 0, buffer.Size())

		buffer.RecordSnapshotItem("key1", at(100))
		assert.Equal(t, 1, buffer.Size())

		buffer.RecordSnapshotItem("key2", at(200))
		assert.Equal(t, 2, buffer.Size())

		// Recording same key again updates, doesn't increase size
		buffer.RecordSnapshotItem("key1", at(150))
		assert.Equal(t, 2, buffer.Size())
	})

	t.Run("empty buffer", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		// Empty buffer should not skip anything
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", at(100)))
		assert.False(t, buffer.IsOverflow())
		assert.Equal(t, 0, buffer.Size())
	})
}

func TestSnapshotSequenceBufferMixedTimeZones(t *testing.T) {
	// The snapshot start comes from time.Now() (host zone). The CDC time comes from the SDK (UTC).
	utcPlusOne := time.FixedZone("UTC+1", 3600)
	utcMinusFive := time.FixedZone("UTC-5", -5*3600)
	snapshotStart := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		snapshotStart time.Time
		cdcTime       time.Time
		wantSkip      bool
	}{
		{
			name:          "east of UTC, CDC after start",
			snapshotStart: snapshotStart.In(utcPlusOne),
			cdcTime:       time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
			wantSkip:      false,
		},
		{
			name:          "east of UTC, CDC before start",
			snapshotStart: snapshotStart.In(utcPlusOne),
			cdcTime:       time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC),
			wantSkip:      true,
		},
		{
			name:          "west of UTC, CDC before start",
			snapshotStart: snapshotStart.In(utcMinusFive),
			cdcTime:       time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC),
			wantSkip:      true,
		},
		{
			name:          "west of UTC, CDC after start",
			snapshotStart: snapshotStart.In(utcMinusFive),
			cdcTime:       time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
			wantSkip:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := newSnapshotSequenceBuffer(100)
			buffer.RecordSnapshotItem("key1", tt.snapshotStart)

			assert.Equal(t, tt.wantSkip, buffer.ShouldSkipCDCEvent("key1", tt.cdcTime))
		})
	}
}

func TestSnapshotSequenceBufferSameSecondAsStart(t *testing.T) {
	// ApproximateCreationDateTime is rounded down to the second. The snapshot start keeps nanoseconds.
	snapshotStart := time.Date(2026, 1, 1, 10, 0, 0, 300_000_000, time.UTC)

	tests := []struct {
		name     string
		cdcTime  time.Time
		wantSkip bool
	}{
		{
			name:     "second before the start second",
			cdcTime:  time.Date(2026, 1, 1, 9, 59, 59, 0, time.UTC),
			wantSkip: true,
		},
		{
			name:     "same second as the start, can be after the start",
			cdcTime:  time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip: false,
		},
		{
			name:     "second after the start second",
			cdcTime:  time.Date(2026, 1, 1, 10, 0, 1, 0, time.UTC),
			wantSkip: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := newSnapshotSequenceBuffer(100)
			buffer.RecordSnapshotItem("key1", snapshotStart)

			assert.Equal(t, tt.wantSkip, buffer.ShouldSkipCDCEvent("key1", tt.cdcTime))
		})
	}
}
