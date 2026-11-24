// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSnapshotSequenceBuffer(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	t.Run("basic deduplication", func(t *testing.T) {
		buffer := newSnapshotSequenceBuffer(100)

		// Record a snapshot item
		buffer.RecordSnapshotItem("key1", at(100))

		// CDC event before the snapshot start should be skipped
		assert.True(t, buffer.ShouldSkipCDCEvent("key1", at(50)))

		// CDC event in the start second or later should not be skipped
		assert.False(t, buffer.ShouldSkipCDCEvent("key1", at(100)))
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

// The snapshot start comes from the host clock and stream stamps from the AWS
// clock. On a host clock ahead of AWS, a write after the true start can get a
// stamp before the recorded start. Within the clock skew margin, the dedupe
// must keep that write.
func TestSnapshotSequenceBufferHostClockAhead(t *testing.T) {
	tests := []struct {
		name            string
		clockSkewMargin time.Duration
		snapshotStart   time.Time
		cdcTime         time.Time
		wantSkip        bool
	}{
		{
			name:            "host 300 ms ahead, write 100 ms after the true start",
			clockSkewMargin: 2 * time.Second,
			snapshotStart:   time.Date(2026, 1, 1, 10, 0, 1, 100_000_000, time.UTC),
			cdcTime:         time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip:        false,
		},
		{
			name:            "host 2 s ahead, write 100 ms after the true start",
			clockSkewMargin: 2 * time.Second,
			snapshotStart:   time.Date(2026, 1, 1, 10, 0, 2, 100_000_000, time.UTC),
			cdcTime:         time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip:        false,
		},
		{
			name:            "write 3 s before the recorded start",
			clockSkewMargin: 2 * time.Second,
			snapshotStart:   time.Date(2026, 1, 1, 10, 0, 3, 100_000_000, time.UTC),
			cdcTime:         time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip:        true,
		},
		{
			name:            "host 10 s ahead with an 11 s margin, write 100 ms after the true start",
			clockSkewMargin: 11 * time.Second,
			snapshotStart:   time.Date(2026, 1, 1, 10, 0, 10, 100_000_000, time.UTC),
			cdcTime:         time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip:        false,
		},
		{
			name:            "write 12 s before the recorded start with an 11 s margin",
			clockSkewMargin: 11 * time.Second,
			snapshotStart:   time.Date(2026, 1, 1, 10, 0, 12, 100_000_000, time.UTC),
			cdcTime:         time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			wantSkip:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := newSnapshotSequenceBuffer(100)
			// connectWithSnapshot records the snapshot start minus the clock skew margin.
			buffer.RecordSnapshotItem("key1", tt.snapshotStart.Add(-tt.clockSkewMargin))

			assert.Equal(t, tt.wantSkip, buffer.ShouldSkipCDCEvent("key1", tt.cdcTime))
		})
	}
}

// The scan must record each item with the dedupe start, not the host start
// time, so that the clock skew margin applies to the items it emitted.
func TestHandleSnapshotBatchRecordsDedupeStart(t *testing.T) {
	keySchema := []dynamodbtypes.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
	}
	d := &dynamoDBCDCInput{
		snapshot: &snapshotState{
			startTime:   time.Date(2026, 1, 1, 10, 0, 2, 100_000_000, time.UTC),
			dedupeStart: time.Date(2026, 1, 1, 10, 0, 0, 100_000_000, time.UTC),
			seqBuffer:   newSnapshotSequenceBuffer(100),
		},
		keySchema: keySchema,
		msgChan:   make(chan asyncMessage, 1),
		log:       service.MockResources().Logger(),
		metrics:   newDynamoDBCDCMetrics(service.MockResources().Metrics()),
	}
	item := map[string]dynamodbtypes.AttributeValue{
		"pk": &dynamodbtypes.AttributeValueMemberS{Value: "item-1"},
	}
	tracker, _ := newTestSnapshotAckTracker(1)
	var ackGate sync.WaitGroup

	err := d.handleSnapshotBatch(t.Context(), []map[string]dynamodbtypes.AttributeValue{item}, 0, "t", item, tracker, &ackGate)
	require.NoError(t, err)

	key := buildItemKeyString(item, keySchema)
	assert.False(t, d.snapshot.seqBuffer.ShouldSkipCDCEvent(key, time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)))
	assert.True(t, d.snapshot.seqBuffer.ShouldSkipCDCEvent(key, time.Date(2026, 1, 1, 9, 59, 59, 0, time.UTC)))
}
