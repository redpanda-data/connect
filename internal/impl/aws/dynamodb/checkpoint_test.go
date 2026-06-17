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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// fakeCheckpointAPI is a programmable stand-in for the DynamoDB client used by
// the Checkpointer. Each field is a function the test sets to control behavior;
// unset functions panic to surface unexpected calls.
type fakeCheckpointAPI struct {
	describeTable func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	createTable   func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	updateTable   func(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	getItem       func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	putItem       func(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	query         func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func (f *fakeCheckpointAPI) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, o ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return f.describeTable(ctx, in, o...)
}

func (f *fakeCheckpointAPI) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, o ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return f.createTable(ctx, in, o...)
}

func (f *fakeCheckpointAPI) UpdateTable(ctx context.Context, in *dynamodb.UpdateTableInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	return f.updateTable(ctx, in, o...)
}

func (f *fakeCheckpointAPI) GetItem(ctx context.Context, in *dynamodb.GetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return f.getItem(ctx, in, o...)
}

func (f *fakeCheckpointAPI) PutItem(ctx context.Context, in *dynamodb.PutItemInput, o ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return f.putItem(ctx, in, o...)
}

func (f *fakeCheckpointAPI) Query(ctx context.Context, in *dynamodb.QueryInput, o ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return f.query(ctx, in, o...)
}

func checkpointTestLogger() *service.Logger { return service.MockResources().Logger() }

func TestNewCheckpointer_NonGlobalCreatesTable(t *testing.T) {
	created := false
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return nil, &types.ResourceNotFoundException{}
		},
		createTable: func(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			created = true
			require.Nil(t, in.StreamSpecification, "non-global table must not enable streams")
			require.Equal(t, "StreamArn", aws.ToString(in.KeySchema[0].AttributeName))
			return &dynamodb.CreateTableOutput{}, nil
		},
	}
	_, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName:       "cps",
		SourceTable:     "mytable",
		StreamArn:       "arn:stream:A",
		CheckpointLimit: 1000,
	}, checkpointTestLogger())
	require.NoError(t, err)
	require.True(t, created)
}

func TestCheckpointer_GlobalModeSetUsesPortableKey(t *testing.T) {
	var put *dynamodb.PutItemInput
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableStatus: types.TableStatusActive,
				Replicas: []types.ReplicaDescription{
					{RegionName: aws.String("us-east-1")},
					{RegionName: aws.String("us-west-2")},
				},
			}}, nil
		},
		putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			put = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "mytable", StreamArn: "arn:A",
		CheckpointLimit: 1, GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)

	require.NoError(t, c.Set(context.Background(), "shard-1", "seq-100", "2026-06-16T10:00:00Z"))

	require.Equal(t, "mytable", put.Item["TableId"].(*types.AttributeValueMemberS).Value)
	require.Equal(t, "shard-1", put.Item["ShardID"].(*types.AttributeValueMemberS).Value)
	require.Equal(t, "seq-100", put.Item["SequenceNumber"].(*types.AttributeValueMemberS).Value)
	require.Equal(t, "arn:A", put.Item["StreamArn"].(*types.AttributeValueMemberS).Value)
	require.Equal(t, "2026-06-16T10:00:00Z", put.Item["ApproximateCreationTime"].(*types.AttributeValueMemberS).Value)
	_, hasStreamArnHashKey := put.Item["TableId"]
	require.True(t, hasStreamArnHashKey)
}

func TestEnsureTable_GlobalCreatesWithStreamsAndAddsReplicas(t *testing.T) {
	var createIn *dynamodb.CreateTableInput
	var replicaRegions []string
	describeCalls := 0
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			describeCalls++
			if describeCalls == 1 {
				return nil, &types.ResourceNotFoundException{} // not exists -> create
			}
			reps := make([]types.ReplicaDescription, 0, len(replicaRegions))
			for _, r := range replicaRegions {
				reps = append(reps, types.ReplicaDescription{RegionName: aws.String(r), ReplicaStatus: types.ReplicaStatusActive})
			}
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableStatus: types.TableStatusActive,
				Replicas:    reps,
			}}, nil
		},
		createTable: func(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			createIn = in
			return &dynamodb.CreateTableOutput{}, nil
		},
		updateTable: func(_ context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
			for _, u := range in.ReplicaUpdates {
				if u.Create != nil {
					replicaRegions = append(replicaRegions, aws.ToString(u.Create.RegionName))
				}
			}
			return &dynamodb.UpdateTableOutput{}, nil
		},
	}
	_, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)
	require.NotNil(t, createIn.StreamSpecification)
	require.True(t, aws.ToBool(createIn.StreamSpecification.StreamEnabled))
	require.Equal(t, types.StreamViewTypeNewAndOldImages, createIn.StreamSpecification.StreamViewType)
	require.Equal(t, "TableId", aws.ToString(createIn.KeySchema[0].AttributeName))
	require.Contains(t, replicaRegions, "us-west-2")
	require.NotContains(t, replicaRegions, "us-east-1", "home region must not be added as a replica")
}

func TestEnsureTable_GlobalReconcilesMissingReplicaWhenTableExists(t *testing.T) {
	added := []string{}
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			reps := []types.ReplicaDescription{{RegionName: aws.String("us-east-1"), ReplicaStatus: types.ReplicaStatusActive}}
			for _, r := range added {
				reps = append(reps, types.ReplicaDescription{RegionName: aws.String(r), ReplicaStatus: types.ReplicaStatusActive})
			}
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive, Replicas: reps}}, nil
		},
		createTable: func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			return nil, errors.New("CreateTable must not be called when table exists")
		},
		updateTable: func(_ context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
			for _, u := range in.ReplicaUpdates {
				if u.Create != nil {
					added = append(added, aws.ToString(u.Create.RegionName))
				}
			}
			return &dynamodb.UpdateTableOutput{}, nil
		},
	}
	_, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)
	require.Equal(t, []string{"us-west-2"}, added)
}

func globalCheckpointerWithPartition(t *testing.T, currentStreamArn string, items []map[string]types.AttributeValue) *Checkpointer {
	t.Helper()
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableStatus: types.TableStatusActive,
				Replicas:    []types.ReplicaDescription{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("us-west-2")}},
			}}, nil
		},
		query: func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: items}, nil
		},
	}
	c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: currentStreamArn, CheckpointLimit: 1,
		GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)
	return c
}

func shardRow(streamArn, shardID, seq, ts string) map[string]types.AttributeValue {
	row := map[string]types.AttributeValue{
		"TableId":        &types.AttributeValueMemberS{Value: "t"},
		"ShardID":        &types.AttributeValueMemberS{Value: shardID},
		"StreamArn":      &types.AttributeValueMemberS{Value: streamArn},
		"SequenceNumber": &types.AttributeValueMemberS{Value: seq},
	}
	if ts != "" {
		row["ApproximateCreationTime"] = &types.AttributeValueMemberS{Value: ts}
	}
	return row
}

func TestResolveResume_ExactWhenCurrentStreamHasRows(t *testing.T) {
	c := globalCheckpointerWithPartition(t, "arn:A", []map[string]types.AttributeValue{
		shardRow("arn:A", "shard-1", "seq-9", "2026-06-16T10:00:00Z"),
	})
	d, err := c.ResolveResume(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, resumeExact, d.Mode)
	require.Equal(t, "seq-9", d.SequenceNumber)
}

func TestResolveResume_FailoverUsesMinTimestampOfForeignRows(t *testing.T) {
	c := globalCheckpointerWithPartition(t, "arn:B", []map[string]types.AttributeValue{
		shardRow("arn:A", "shard-x", "seq-1", "2026-06-16T10:05:00Z"),
		shardRow("arn:A", "shard-y", "seq-2", "2026-06-16T10:01:00Z"), // min
	})
	d, err := c.ResolveResume(context.Background(), "shard-new")
	require.NoError(t, err)
	require.Equal(t, resumeFailover, d.Mode)
	require.Equal(t, "2026-06-16T10:01:00Z", d.Cutoff.UTC().Format(time.RFC3339))
}

func TestResolveResume_DefaultWhenPartitionEmpty(t *testing.T) {
	c := globalCheckpointerWithPartition(t, "arn:A", nil)
	d, err := c.ResolveResume(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, resumeDefault, d.Mode)
}

func TestResolveResume_NonGlobalFallsBackToGet(t *testing.T) {
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		getItem: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
				"SequenceNumber": &types.AttributeValueMemberS{Value: "seq-42"},
			}}, nil
		},
	}
	c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
	}, checkpointTestLogger())
	require.NoError(t, err)
	d, err := c.ResolveResume(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, resumeExact, d.Mode)
	require.Equal(t, "seq-42", d.SequenceNumber)
}
