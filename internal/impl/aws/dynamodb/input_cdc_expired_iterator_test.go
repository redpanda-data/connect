// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
)

func TestIsExpiredIteratorError(t *testing.T) {
	assert.False(t, isExpiredIteratorError(nil))
	assert.False(t, isExpiredIteratorError(errors.New("boom")))
	assert.False(t, isExpiredIteratorError(&types.TrimmedDataAccessException{}))
	assert.True(t, isExpiredIteratorError(&types.ExpiredIteratorException{}))
}

func TestResolveResumeIterator(t *testing.T) {
	tests := []struct {
		name       string
		lastSeq    string
		checkpoint string
		wantType   types.ShardIteratorType
		wantSeq    *string
	}{
		{
			name:       "prefers last read sequence over checkpoint",
			lastSeq:    "100",
			checkpoint: "50",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("100"),
		},
		{
			name:       "falls back to checkpoint when nothing read",
			lastSeq:    "",
			checkpoint: "50",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("50"),
		},
		{
			name:     "falls back to trim horizon when no sequence available",
			lastSeq:  "",
			wantType: types.ShardIteratorTypeTrimHorizon,
			wantSeq:  nil,
		},
		{
			// LATEST must never be re-acquired: the shard was already
			// positioned when the expired iterator was obtained, so LATEST
			// would silently skip everything published since.
			name:       "never re-acquires latest",
			lastSeq:    "",
			checkpoint: "",
			wantType:   types.ShardIteratorTypeTrimHorizon,
			wantSeq:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotSeq := resolveResumeIterator(tc.lastSeq, tc.checkpoint)
			assert.Equal(t, tc.wantType, gotType)
			if tc.wantSeq == nil {
				assert.Nil(t, gotSeq)
			} else {
				assert.Equal(t, *tc.wantSeq, *gotSeq)
			}
		})
	}
}

// TestInitialIteratorType locks in the start_from contract: latest applies
// only to the first discovery of a genuinely fresh pipeline; every other
// checkpoint-less shard (rotation children found on refresh cycles, or any
// shard after a restart with existing state) starts at TRIM_HORIZON so its
// backlog is never silently skipped.
func TestInitialIteratorType(t *testing.T) {
	cases := []struct {
		name      string
		startFrom string
		honor     bool
		want      types.ShardIteratorType
	}{
		{"fresh pipeline honors latest", "latest", true, types.ShardIteratorTypeLatest},
		{"fresh pipeline honors trim_horizon", "trim_horizon", true, types.ShardIteratorTypeTrimHorizon},
		{"rotation child ignores latest", "latest", false, types.ShardIteratorTypeTrimHorizon},
		{"restart with state ignores latest", "latest", false, types.ShardIteratorTypeTrimHorizon},
		{"trim_horizon unaffected by honor flag", "trim_horizon", false, types.ShardIteratorTypeTrimHorizon},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, initialIteratorType(tc.startFrom, tc.honor))
		})
	}
}
