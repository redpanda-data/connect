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
		startFrom  string
		wantType   types.ShardIteratorType
		wantSeq    *string
	}{
		{
			name:       "prefers last read sequence over checkpoint",
			lastSeq:    "100",
			checkpoint: "50",
			startFrom:  "latest",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("100"),
		},
		{
			name:       "falls back to checkpoint when nothing read",
			lastSeq:    "",
			checkpoint: "50",
			startFrom:  "latest",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("50"),
		},
		{
			name:      "falls back to latest when no sequence available",
			lastSeq:   "",
			startFrom: "latest",
			wantType:  types.ShardIteratorTypeLatest,
			wantSeq:   nil,
		},
		{
			name:      "falls back to trim horizon when no sequence available",
			lastSeq:   "",
			startFrom: "trim_horizon",
			wantType:  types.ShardIteratorTypeTrimHorizon,
			wantSeq:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotSeq := resolveResumeIterator(tc.lastSeq, tc.checkpoint, tc.startFrom)
			assert.Equal(t, tc.wantType, gotType)
			if tc.wantSeq == nil {
				assert.Nil(t, gotSeq)
			} else {
				assert.Equal(t, *tc.wantSeq, *gotSeq)
			}
		})
	}
}
