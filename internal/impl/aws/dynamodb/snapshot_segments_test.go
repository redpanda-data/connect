// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// segmentRowsAPI is a fakeCheckpointAPI that stores the rows it is given and
// returns them from the snapshot segment query; the completion marker is
// never present.
func segmentRowsAPI() *fakeCheckpointAPI {
	var mu sync.Mutex
	var rows []map[string]dynamodbtypes.AttributeValue
	return &fakeCheckpointAPI{
		putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			rows = append(rows, in.Item)
			return &dynamodb.PutItemOutput{}, nil
		},
		getItem: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{}, nil
		},
		query: func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			return &dynamodb.QueryOutput{Items: rows}, nil
		},
	}
}

// TestSnapshotSegmentCountSurvivesResume: a Scan segment's key range depends
// on the total segment count, so a resume after snapshot_segments changed
// must keep the count the stored positions were read under. Resuming them
// under the new count would skip or re-read keys.
func TestSnapshotSegmentCountSurvivesResume(t *testing.T) {
	cp := &Checkpointer{
		tableName:       "ckpt",
		sourceTable:     "t",
		streamArn:       testStreamArn,
		checkpointLimit: 10,
		svc:             segmentRowsAPI(),
		log:             service.MockResources().Logger(),
	}
	tracker := newSnapshotAckTracker(cp, 1, 4, service.MockResources().Logger())

	resolve := tracker.TrackBatch(1, map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k"}}, 3)
	require.NoError(t, tracker.Ack(t.Context(), 1, 3, resolve))

	progress, err := cp.SnapshotProgress(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 4, progress.TotalSegments, "the segment count is stored with the position")

	segments, resumed := progress.ScanSegments(8)
	assert.Equal(t, 4, segments, "a resume keeps the stored segment count")
	assert.True(t, resumed)

	segments, resumed = progress.ScanSegments(4)
	assert.Equal(t, 4, segments)
	assert.False(t, resumed)
}

func TestSnapshotCheckpointScanSegments(t *testing.T) {
	withProgress := func(total int, complete bool) *SnapshotCheckpoint {
		c := NewSnapshotCheckpoint()
		c.SegmentProgress[0] = &SegmentState{}
		c.TotalSegments = total
		c.Complete = complete
		return c
	}
	tests := []struct {
		name     string
		resume   *SnapshotCheckpoint
		want     int
		wantFlag bool
	}{
		{name: "nil", resume: nil, want: 8},
		{name: "fresh", resume: NewSnapshotCheckpoint(), want: 8},
		{name: "legacy rows without a count", resume: withProgress(0, false), want: 8},
		{name: "same count", resume: withProgress(8, false), want: 8},
		{name: "changed count", resume: withProgress(4, false), want: 4, wantFlag: true},
		{name: "complete snapshot", resume: withProgress(4, true), want: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, resumed := tt.resume.ScanSegments(8)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantFlag, resumed)
		})
	}
}
