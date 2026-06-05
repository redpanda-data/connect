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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStreamPager is a configurable test double for describeStreamPager.
type fakeStreamPager struct {
	// pages is consumed FIFO; each call to DescribeStream returns one page.
	pages []*dynamodbstreams.DescribeStreamOutput
	// errs[i] is returned by the i-th call if non-nil. Nil means no error.
	errs []error
	// inputs records each DescribeStreamInput received by the fake.
	inputs []*dynamodbstreams.DescribeStreamInput
	calls  int
}

func (f *fakeStreamPager) DescribeStream(_ context.Context, in *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	f.inputs = append(f.inputs, in)
	idx := f.calls
	f.calls++
	if idx < len(f.errs) && f.errs[idx] != nil {
		return nil, f.errs[idx]
	}
	if idx >= len(f.pages) {
		return nil, errors.New("fakeStreamPager: ran out of pages")
	}
	return f.pages[idx], nil
}

func shardWithID(id string) types.Shard {
	return types.Shard{ShardId: &id}
}

func TestDescribeStreamAllShards(t *testing.T) {
	arn := "stream-arn"

	t.Run("empty stream returns empty slice", func(t *testing.T) {
		fake := &fakeStreamPager{
			pages: []*dynamodbstreams.DescribeStreamOutput{
				{StreamDescription: &types.StreamDescription{Shards: nil}},
			},
		}
		shards, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.NoError(t, err)
		assert.Empty(t, shards)
		assert.Equal(t, 1, fake.calls)
	})

	t.Run("single page returns all shards", func(t *testing.T) {
		fake := &fakeStreamPager{
			pages: []*dynamodbstreams.DescribeStreamOutput{
				{StreamDescription: &types.StreamDescription{
					Shards: []types.Shard{shardWithID("s1"), shardWithID("s2"), shardWithID("s3")},
				}},
			},
		}
		shards, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.NoError(t, err)
		require.Len(t, shards, 3)
		assert.Equal(t, "s1", *shards[0].ShardId)
		assert.Equal(t, "s2", *shards[1].ShardId)
		assert.Equal(t, "s3", *shards[2].ShardId)
		assert.Equal(t, 1, fake.calls)
	})

	t.Run("multiple pages accumulated", func(t *testing.T) {
		last1 := "s100"
		last2 := "s200"
		fake := &fakeStreamPager{
			pages: []*dynamodbstreams.DescribeStreamOutput{
				{StreamDescription: &types.StreamDescription{
					Shards:               []types.Shard{shardWithID("s1"), shardWithID("s2")},
					LastEvaluatedShardId: &last1,
				}},
				{StreamDescription: &types.StreamDescription{
					Shards:               []types.Shard{shardWithID("s3"), shardWithID("s4")},
					LastEvaluatedShardId: &last2,
				}},
				{StreamDescription: &types.StreamDescription{
					Shards: []types.Shard{shardWithID("s5")},
				}},
			},
		}
		shards, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.NoError(t, err)
		require.Len(t, shards, 5)
		for i, want := range []string{"s1", "s2", "s3", "s4", "s5"} {
			assert.Equal(t, want, *shards[i].ShardId, "index %d", i)
		}
		assert.Equal(t, 3, fake.calls)
	})

	t.Run("ExclusiveStartShardId chained from LastEvaluatedShardId", func(t *testing.T) {
		last := "from-page-1"
		fake := &fakeStreamPager{
			pages: []*dynamodbstreams.DescribeStreamOutput{
				{StreamDescription: &types.StreamDescription{
					Shards:               []types.Shard{shardWithID("a")},
					LastEvaluatedShardId: &last,
				}},
				{StreamDescription: &types.StreamDescription{
					Shards: []types.Shard{shardWithID("b")},
				}},
			},
		}
		_, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.NoError(t, err)
		require.Len(t, fake.inputs, 2)
		assert.Nil(t, fake.inputs[0].ExclusiveStartShardId, "first call must not set ExclusiveStartShardId")
		require.NotNil(t, fake.inputs[1].ExclusiveStartShardId, "second call must set ExclusiveStartShardId")
		assert.Equal(t, last, *fake.inputs[1].ExclusiveStartShardId)
		// StreamArn must be carried on every call.
		require.NotNil(t, fake.inputs[0].StreamArn)
		require.NotNil(t, fake.inputs[1].StreamArn)
		assert.Equal(t, arn, *fake.inputs[0].StreamArn)
		assert.Equal(t, arn, *fake.inputs[1].StreamArn)
	})

	t.Run("error mid-pagination returns error", func(t *testing.T) {
		last := "s100"
		fake := &fakeStreamPager{
			pages: []*dynamodbstreams.DescribeStreamOutput{
				{StreamDescription: &types.StreamDescription{
					Shards:               []types.Shard{shardWithID("s1")},
					LastEvaluatedShardId: &last,
				}},
			},
			errs: []error{nil, errors.New("network failure")},
		}
		_, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "network failure")
		assert.Equal(t, 2, fake.calls)
	})

	t.Run("error on first call returns error immediately", func(t *testing.T) {
		fake := &fakeStreamPager{
			errs: []error{errors.New("auth failure")},
		}
		_, err := describeStreamAllShards(context.Background(), fake, &arn)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "auth failure")
		assert.Equal(t, 1, fake.calls)
	})
}
