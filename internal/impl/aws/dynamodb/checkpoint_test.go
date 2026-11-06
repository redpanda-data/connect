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
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
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
	updateItem    func(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
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

func (f *fakeCheckpointAPI) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return f.updateItem(ctx, in, o...)
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
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema:   globalTableKeySchema(),
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
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema:   globalTableKeySchema(),
				Replicas:    reps,
			}}, nil
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

func TestEnsureTable_GlobalRejectsExistingNonGlobalTable(t *testing.T) {
	updateCalled := false
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			// A table created without global_table mode uses a StreamArn hash key.
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("StreamArn"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
				},
			}}, nil
		},
		createTable: func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			return nil, errors.New("CreateTable must not be called when table exists")
		},
		updateTable: func(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
			updateCalled = true
			return &dynamodb.UpdateTableOutput{}, nil
		},
	}
	_, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.Error(t, err)
	require.Contains(t, err.Error(), "StreamArn")
	require.Contains(t, err.Error(), "global_table")
	require.False(t, updateCalled, "must not reconcile replicas against a schema-mismatched table")
}

// globalTableKeySchema returns the key schema of a checkpoint table created in
// global mode (TableId hash + ShardID range), matching what ensureTableExists
// provisions and what validateGlobalTableSchema requires.
func globalTableKeySchema() []types.KeySchemaElement {
	return []types.KeySchemaElement{
		{AttributeName: aws.String("TableId"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
	}
}

func globalCheckpointerWithPartition(t *testing.T, currentStreamArn string, items []map[string]types.AttributeValue) *Checkpointer {
	t.Helper()
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema:   globalTableKeySchema(),
				Replicas:    []types.ReplicaDescription{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("us-west-2")}},
			}}, nil
		},
		query: func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: items}, nil
		},
		getItem: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			shardID := in.Key[checkpointRangeKey].(*types.AttributeValueMemberS).Value
			for _, item := range items {
				if item["ShardID"].(*types.AttributeValueMemberS).Value == shardID {
					return &dynamodb.GetItemOutput{Item: item}, nil
				}
			}
			return &dynamodb.GetItemOutput{}, nil
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

func TestHasAnyState(t *testing.T) {
	t.Run("false on an empty partition", func(t *testing.T) {
		c := globalCheckpointerWithPartition(t, "arn:A", nil)
		has, err := c.HasAnyState(context.Background())
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("true when any row exists", func(t *testing.T) {
		c := globalCheckpointerWithPartition(t, "arn:A", []map[string]types.AttributeValue{
			shardRow("arn:A", "shard-1", "seq-9", ""),
		})
		has, err := c.HasAnyState(context.Background())
		require.NoError(t, err)
		require.True(t, has)
	})

	t.Run("queries the namespaced hash key", func(t *testing.T) {
		var queryIn *dynamodb.QueryInput
		api := &fakeCheckpointAPI{
			describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
			},
			query: func(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
				queryIn = in
				return &dynamodb.QueryOutput{}, nil
			},
		}
		c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A",
			Namespace: "dev", CheckpointLimit: 1, Region: "us-east-1",
		}, checkpointTestLogger())
		require.NoError(t, err)

		_, err = c.HasAnyState(context.Background())
		require.NoError(t, err)
		require.NotNil(t, queryIn)
		hv, ok := queryIn.ExpressionAttributeValues[":hv"].(*types.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "dev#arn:A", hv.Value)
	})
}

func TestCDCCheckpointProbeNeeded(t *testing.T) {
	// Only an exact, same-region resume can be a stale checkpoint: its sequence
	// number belongs to this stream, so a failed iterator means trimmed data.
	require.True(t, cdcCheckpointProbeNeeded(resumeExact))
	// A failover resume reads from the trim horizon (another region's sequence
	// number must NOT be probed against this stream) — never stale. This is the
	// regression guard: probing here re-triggered a full snapshot after failover.
	require.False(t, cdcCheckpointProbeNeeded(resumeFailover))
	// No usable checkpoint: nothing to probe.
	require.False(t, cdcCheckpointProbeNeeded(resumeDefault))
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

// sharedStoreAPI returns a fakeCheckpointAPI backed by a shared in-memory item
// store keyed by "<hashValue>|<shardID>", so multiple Checkpointers can
// exercise cross-namespace visibility against one "table".
func sharedStoreAPI(store map[string]map[string]types.AttributeValue) *fakeCheckpointAPI {
	keyOf := func(m map[string]types.AttributeValue) string {
		attrStr := func(name string) string {
			if v, ok := m[name].(*types.AttributeValueMemberS); ok {
				return v.Value
			}
			return ""
		}
		// Global-mode items carry a non-key StreamArn attribute alongside the
		// TableId hash key, so prefer TableId when present.
		hash := attrStr(checkpointHashKeyGlobal)
		if hash == "" {
			hash = attrStr(checkpointHashKeyDefault)
		}
		return hash + "|" + attrStr(checkpointRangeKey)
	}
	return &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if err := checkAttributeNotExistsCondition(in, store[keyOf(in.Item)]); err != nil {
				return nil, err
			}
			store[keyOf(in.Item)] = in.Item
			return &dynamodb.PutItemOutput{}, nil
		},
		updateItem: func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			k := keyOf(in.Key)
			row := map[string]types.AttributeValue{}
			maps.Copy(row, in.Key)
			maps.Copy(row, store[k])
			maps.Copy(row, applySetExpression(in))
			store[k] = row
			return &dynamodb.UpdateItemOutput{}, nil
		},
		getItem: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: store[keyOf(in.Key)]}, nil
		},
	}
}

// applySetExpression resolves a "SET #a = :x, #b = :y" UpdateExpression into
// the attributes it assigns.
func applySetExpression(in *dynamodb.UpdateItemInput) map[string]types.AttributeValue {
	out := map[string]types.AttributeValue{}
	expr := strings.TrimPrefix(aws.ToString(in.UpdateExpression), "SET ")
	for clause := range strings.SplitSeq(expr, ", ") {
		name, value, _ := strings.Cut(clause, " = ")
		if resolved, ok := in.ExpressionAttributeNames[name]; ok {
			name = resolved
		}
		out[name] = in.ExpressionAttributeValues[value]
	}
	return out
}

// checkAttributeNotExistsCondition evaluates the only ConditionExpression the
// Checkpointer writes, "attribute_not_exists(<#alias>)", against the row a
// PutItem would replace, the way DynamoDB does: the write is rejected with
// ConditionalCheckFailedException when the existing row has the attribute.
func checkAttributeNotExistsCondition(in *dynamodb.PutItemInput, existing map[string]types.AttributeValue) error {
	cond := aws.ToString(in.ConditionExpression)
	if cond == "" {
		return nil
	}
	const prefix, suffix = "attribute_not_exists(", ")"
	if !strings.HasPrefix(cond, prefix) || !strings.HasSuffix(cond, suffix) {
		return fmt.Errorf("fake PutItem: unsupported ConditionExpression %q", cond)
	}
	attr := strings.TrimSuffix(strings.TrimPrefix(cond, prefix), suffix)
	if strings.HasPrefix(attr, "#") {
		resolved, ok := in.ExpressionAttributeNames[attr]
		if !ok {
			return fmt.Errorf("fake PutItem: ConditionExpression alias %q has no ExpressionAttributeNames entry", attr)
		}
		attr = resolved
	}
	if _, has := existing[attr]; has {
		return &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
	}
	return nil
}

func TestCheckpointer_NamespacePrefixesHashKeyValue(t *testing.T) {
	cases := []struct {
		name      string
		namespace string
		global    bool
		wantAttr  string
		wantValue string
	}{
		{"default mode no namespace", "", false, "StreamArn", "arn:A"},
		{"default mode namespaced", "dev-alice", false, "StreamArn", "dev-alice#arn:A"},
		{"global mode no namespace", "", true, "TableId", "t"},
		{"global mode namespaced", "dev-alice", true, "TableId", "dev-alice#t"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var put *dynamodb.PutItemInput
			api := &fakeCheckpointAPI{
				describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
					desc := &types.TableDescription{TableStatus: types.TableStatusActive}
					if tc.global {
						desc.KeySchema = globalTableKeySchema()
						desc.Replicas = []types.ReplicaDescription{
							{RegionName: aws.String("us-east-1")},
							{RegionName: aws.String("us-west-2")},
						}
					}
					return &dynamodb.DescribeTableOutput{Table: desc}, nil
				},
				putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
					put = in
					return &dynamodb.PutItemOutput{}, nil
				},
			}
			cfg := CheckpointerConfig{
				TableName: "cps", SourceTable: "t", StreamArn: "arn:A",
				Namespace: tc.namespace, CheckpointLimit: 1,
			}
			if tc.global {
				cfg.GlobalTable = true
				cfg.Region = "us-east-1"
				cfg.ReplicaRegions = []string{"us-west-2"}
			}
			c, err := NewCheckpointer(context.Background(), api, cfg, checkpointTestLogger())
			require.NoError(t, err)

			require.NoError(t, c.Set(context.Background(), "shard-1", "seq-1", ""))
			require.Equal(t, tc.wantValue, put.Item[tc.wantAttr].(*types.AttributeValueMemberS).Value)
			require.Equal(t, "shard-1", put.Item[checkpointRangeKey].(*types.AttributeValueMemberS).Value,
				"range key must never be namespaced")
		})
	}
}

func TestCheckpointer_NamespaceIsolation(t *testing.T) {
	store := map[string]map[string]types.AttributeValue{}
	api := sharedStoreAPI(store)

	newCP := func(ns string) *Checkpointer {
		c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A",
			Namespace: ns, CheckpointLimit: 1,
		}, checkpointTestLogger())
		require.NoError(t, err)
		return c
	}
	alice, bob, unscoped := newCP("dev-alice"), newCP("dev-bob"), newCP("")

	require.NoError(t, alice.Set(context.Background(), "shard-1", "seq-alice", ""))
	require.NoError(t, unscoped.Set(context.Background(), "shard-1", "seq-legacy", ""))

	got, err := alice.Get(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, "seq-alice", got, "a namespace must read back its own checkpoint")

	got, err = bob.Get(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Empty(t, got, "a namespace must not see another namespace's checkpoint")

	got, err = unscoped.Get(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, "seq-legacy", got, "un-namespaced reader must not see namespaced rows")
}

func TestSnapshotCheckpoints_UseNamespacedHashValue(t *testing.T) {
	var getIn *dynamodb.GetItemInput
	var queryIn *dynamodb.QueryInput
	var putIn *dynamodb.PutItemInput
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		getItem: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			getIn = in
			return &dynamodb.GetItemOutput{}, nil
		},
		query: func(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			queryIn = in
			return &dynamodb.QueryOutput{}, nil
		},
		putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			putIn = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A",
		Namespace: "dev-alice", CheckpointLimit: 1,
	}, checkpointTestLogger())
	require.NoError(t, err)

	_, err = c.SnapshotProgress(context.Background())
	require.NoError(t, err)
	require.Equal(t, "dev-alice#arn:A", getIn.Key["StreamArn"].(*types.AttributeValueMemberS).Value,
		"snapshot completion sentinel must use the namespaced hash value")
	require.Equal(t, "dev-alice#arn:A", queryIn.ExpressionAttributeValues[":hash"].(*types.AttributeValueMemberS).Value,
		"snapshot segment query must use the namespaced hash value")

	require.NoError(t, c.UpdateSnapshotProgress(context.Background(), 3, nil, 42))
	require.Equal(t, "dev-alice#arn:A", putIn.Item["StreamArn"].(*types.AttributeValueMemberS).Value,
		"snapshot segment writes must use the namespaced hash value")
	require.Equal(t, "snapshot#segment#3", putIn.Item[checkpointRangeKey].(*types.AttributeValueMemberS).Value)
}

func TestPrepareResume_UsesNamespacedHashValue(t *testing.T) {
	var queryIn *dynamodb.QueryInput
	api := &fakeCheckpointAPI{
		describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema:   globalTableKeySchema(),
				Replicas: []types.ReplicaDescription{
					{RegionName: aws.String("us-east-1")},
					{RegionName: aws.String("us-west-2")},
				},
			}}, nil
		},
		query: func(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			queryIn = in
			return &dynamodb.QueryOutput{}, nil
		},
		getItem: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{}, nil
		},
	}
	c, err := NewCheckpointer(context.Background(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		Namespace: "dev-alice", GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)

	_, err = c.ResolveResume(context.Background(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, "dev-alice#t", queryIn.ExpressionAttributeValues[":hash"].(*types.AttributeValueMemberS).Value,
		"global-mode resume partition query must use the namespaced hash value")
}

// TestCheckpointerShardCompletionMarkerRoundTrip (CON-599): a shard marked
// completed resolves to resumeCompleted after a restart, with or without a
// final sequence number, while ordinary rows keep resolving to an exact
// resume.
func TestCheckpointerShardCompletionMarkerRoundTrip(t *testing.T) {
	store := map[string]map[string]types.AttributeValue{}
	c, err := NewCheckpointer(t.Context(), sharedStoreAPI(store), CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
	}, checkpointTestLogger())
	require.NoError(t, err)

	require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-done", "seq-9", ""))
	require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-empty", "", ""))
	require.NoError(t, c.Set(t.Context(), "shard-open", "seq-5", ""))

	for _, shardID := range []string{"shard-done", "shard-empty"} {
		d, err := c.ResolveResume(t.Context(), shardID)
		require.NoError(t, err)
		assert.Equal(t, resumeCompleted, d.Mode, shardID)
	}
	seq, err := c.Get(t.Context(), "shard-done")
	require.NoError(t, err)
	assert.Equal(t, "seq-9", seq, "the completion row still carries the final checkpoint")

	d, err := c.ResolveResume(t.Context(), "shard-open")
	require.NoError(t, err)
	assert.Equal(t, resumeExact, d.Mode)
	assert.Equal(t, "seq-5", d.SequenceNumber)

	// A later plain checkpoint write must not clear the marker (CON-600).
	require.NoError(t, c.Set(t.Context(), "shard-done", "seq-10", ""))
	d, err = c.ResolveResume(t.Context(), "shard-done")
	require.NoError(t, err)
	assert.Equal(t, resumeCompleted, d.Mode)
}

// TestCheckpointerSetKeepsCompletionMarker (CON-600): a plain checkpoint
// write landing after the completion marker (a persister Set that timed
// out client-side but still reaches the server, or the shutdown flush) must
// leave the completed row intact, so a restart still skips the shard. The
// write is conditional on the marker being absent and a rejected write is not
// an error: the row already records the shard's final position.
func TestCheckpointerSetKeepsCompletionMarker(t *testing.T) {
	t.Run("default mode", func(t *testing.T) {
		store := map[string]map[string]types.AttributeValue{}
		c, err := NewCheckpointer(t.Context(), sharedStoreAPI(store), CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		}, checkpointTestLogger())
		require.NoError(t, err)

		require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-done", "seq-9", ""))
		require.NoError(t, c.Set(t.Context(), "shard-done", "seq-7", ""), "a late plain write is not an error")
		require.NoError(t, c.Set(t.Context(), "shard-done", "seq-8", ""), "the shutdown flush goes through the same guarded write")

		d, err := c.ResolveResume(t.Context(), "shard-done")
		require.NoError(t, err)
		assert.Equal(t, resumeCompleted, d.Mode)
		seq, err := c.Get(t.Context(), "shard-done")
		require.NoError(t, err)
		assert.Equal(t, "seq-9", seq, "the final checkpoint must not step backwards either")

		// Rows without the marker keep accepting plain writes.
		require.NoError(t, c.Set(t.Context(), "shard-open", "seq-1", ""))
		require.NoError(t, c.Set(t.Context(), "shard-open", "seq-2", ""))
		seq, err = c.Get(t.Context(), "shard-open")
		require.NoError(t, err)
		assert.Equal(t, "seq-2", seq)
	})

	t.Run("global mode", func(t *testing.T) {
		store := map[string]map[string]types.AttributeValue{}
		api := sharedStoreAPI(store)
		api.describeTable = func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName:   aws.String("cps"),
				TableStatus: types.TableStatusActive,
				KeySchema:   globalTableKeySchema(),
				Replicas:    []types.ReplicaDescription{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("us-west-2")}},
			}}, nil
		}
		c, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
			GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
		}, checkpointTestLogger())
		require.NoError(t, err)

		require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-done", "seq-9", "2026-06-16T10:09:00Z"))
		require.NoError(t, c.Set(t.Context(), "shard-done", "seq-7", "2026-06-16T10:07:00Z"))

		row := store["t|shard-done"]
		require.NotNil(t, row)
		assert.True(t, isShardCompletedRow(row))
		assert.Equal(t, "seq-9", row["SequenceNumber"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "2026-06-16T10:09:00Z", row["ApproximateCreationTime"].(*types.AttributeValueMemberS).Value,
			"the failover watermark must not step backwards")
	})

	t.Run("only plain writes are conditional", func(t *testing.T) {
		var puts []*dynamodb.PutItemInput
		var updates []*dynamodb.UpdateItemInput
		api := &fakeCheckpointAPI{
			describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
			},
			putItem: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				puts = append(puts, in)
				return &dynamodb.PutItemOutput{}, nil
			},
			updateItem: func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
				updates = append(updates, in)
				return &dynamodb.UpdateItemOutput{}, nil
			},
		}
		c, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		}, checkpointTestLogger())
		require.NoError(t, err)

		require.NoError(t, c.Set(t.Context(), "shard-1", "seq-1", ""))
		require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-1", "seq-2", ""))
		require.Len(t, puts, 1)
		require.Len(t, updates, 1)

		assert.Equal(t, "attribute_not_exists(#completed)", aws.ToString(puts[0].ConditionExpression))
		assert.Equal(t, checkpointShardCompletedAttr, puts[0].ExpressionAttributeNames["#completed"])
		assert.Nil(t, updates[0].ConditionExpression, "the completion write itself is unconditional")
	})

	t.Run("other write errors still surface", func(t *testing.T) {
		api := &fakeCheckpointAPI{
			describeTable: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
			},
			putItem: func(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				return nil, &types.ProvisionedThroughputExceededException{}
			},
		}
		c, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
			TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
		}, checkpointTestLogger())
		require.NoError(t, err)

		err = c.Set(t.Context(), "shard-1", "seq-1", "")
		require.Error(t, err)
		_, isThrottle := errors.AsType[*types.ProvisionedThroughputExceededException](err)
		assert.True(t, isThrottle, "the original error must stay unwrapped")
	})
}

// TestResolveResume_GlobalCompletedRows: in global mode completed rows of the
// current stream resolve to resumeCompleted and count as own state, so an
// unknown shard still starts by default instead of a failover replay.
func TestResolveResume_GlobalCompletedRows(t *testing.T) {
	completedRow := shardRow("arn:A", "shard-done", "seq-9", "2026-06-16T10:00:00Z")
	completedRow[checkpointShardCompletedAttr] = &types.AttributeValueMemberBOOL{Value: true}
	emptyCompletedRow := map[string]types.AttributeValue{
		"TableId":                    &types.AttributeValueMemberS{Value: "t"},
		"ShardID":                    &types.AttributeValueMemberS{Value: "shard-empty"},
		"StreamArn":                  &types.AttributeValueMemberS{Value: "arn:A"},
		checkpointShardCompletedAttr: &types.AttributeValueMemberBOOL{Value: true},
	}
	c := globalCheckpointerWithPartition(t, "arn:A", []map[string]types.AttributeValue{completedRow, emptyCompletedRow})

	for _, shardID := range []string{"shard-done", "shard-empty"} {
		d, err := c.ResolveResume(t.Context(), shardID)
		require.NoError(t, err)
		assert.Equal(t, resumeCompleted, d.Mode, shardID)
	}
	d, err := c.ResolveResume(t.Context(), "shard-new")
	require.NoError(t, err)
	assert.Equal(t, resumeDefault, d.Mode)
	assert.False(t, cdcCheckpointProbeNeeded(resumeCompleted), "a completed shard has nothing to probe")
}

// TestResolveResume_GlobalForeignCompletedRowWithoutTimeIgnored (CON-599): a
// completed row from another region's stream that carries no
// ApproximateCreationTime (a closed shard that delivered nothing) gives no
// failover position, so it must not turn a fresh region into a failover
// resume with a zero cutoff, which would replay every shard from TRIM_HORIZON.
func TestResolveResume_GlobalForeignCompletedRowWithoutTimeIgnored(t *testing.T) {
	emptyCompleted := func(shardID string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"TableId":                    &types.AttributeValueMemberS{Value: "t"},
			"ShardID":                    &types.AttributeValueMemberS{Value: shardID},
			"StreamArn":                  &types.AttributeValueMemberS{Value: "arn:A"},
			checkpointShardCompletedAttr: &types.AttributeValueMemberBOOL{Value: true},
		}
	}

	t.Run("only timeless completed rows", func(t *testing.T) {
		c := globalCheckpointerWithPartition(t, "arn:B", []map[string]types.AttributeValue{
			emptyCompleted("shard-x"), emptyCompleted("shard-y"),
		})
		d, err := c.ResolveResume(t.Context(), "shard-new")
		require.NoError(t, err)
		assert.Equal(t, resumeDefault, d.Mode)
	})

	t.Run("beside a timed row", func(t *testing.T) {
		timedCompleted := shardRow("arn:A", "shard-z", "seq-3", "2026-06-16T10:02:00Z")
		timedCompleted[checkpointShardCompletedAttr] = &types.AttributeValueMemberBOOL{Value: true}
		c := globalCheckpointerWithPartition(t, "arn:B", []map[string]types.AttributeValue{
			emptyCompleted("shard-x"),
			timedCompleted,
		})
		d, err := c.ResolveResume(t.Context(), "shard-new")
		require.NoError(t, err)
		assert.Equal(t, resumeFailover, d.Mode)
		assert.Equal(t, "2026-06-16T10:02:00Z", d.Cutoff.UTC().Format(time.RFC3339))
	})
}

// TestResolveResume_GlobalReadsCurrentRow (CON-599): in global mode a shard
// resolved during the run (its reader was released, or it left the listing
// and came back, and discovery prepares it again) must resume from the
// checkpoint written during this run, not from the partition cache read at
// startup, or the records acked since startup are delivered again.
func TestResolveResume_GlobalReadsCurrentRow(t *testing.T) {
	store := map[string]map[string]types.AttributeValue{}
	api := sharedStoreAPI(store)
	api.describeTable = func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
			TableStatus: types.TableStatusActive,
			KeySchema:   globalTableKeySchema(),
			Replicas:    []types.ReplicaDescription{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("us-west-2")}},
		}}, nil
	}
	api.query = func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
		out := &dynamodb.QueryOutput{}
		for _, r := range store {
			out.Items = append(out.Items, r)
		}
		return out, nil
	}
	// Another region's row makes a shard with no own checkpoint a failover.
	foreign := shardRow("arn:A", "shard-foreign", "seq-1", "2026-06-16T10:00:00Z")
	store["t|shard-foreign"] = foreign
	c, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "t", StreamArn: "arn:B", CheckpointLimit: 1,
		GlobalTable: true, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
	}, checkpointTestLogger())
	require.NoError(t, err)

	d, err := c.ResolveResume(t.Context(), "shard-1")
	require.NoError(t, err)
	require.Equal(t, resumeFailover, d.Mode)

	// Acks during the run checkpoint the shard; a re-resolve resumes there.
	require.NoError(t, c.Set(t.Context(), "shard-1", "seq-1000", "2026-06-16T11:00:00Z"))
	d, err = c.ResolveResume(t.Context(), "shard-1")
	require.NoError(t, err)
	assert.Equal(t, resumeExact, d.Mode)
	assert.Equal(t, "seq-1000", d.SequenceNumber)

	require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-1", "seq-2000", "2026-06-16T12:00:00Z"))
	d, err = c.ResolveResume(t.Context(), "shard-1")
	require.NoError(t, err)
	assert.Equal(t, resumeCompleted, d.Mode)

	// A shard completed during the run resolves as completed even on its first
	// resolve since startup.
	require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-3", "seq-3000", "2026-06-16T12:00:00Z"))
	d, err = c.ResolveResume(t.Context(), "shard-3")
	require.NoError(t, err)
	assert.Equal(t, resumeCompleted, d.Mode)

	// A shard with no row of its own still falls back to the cached decision.
	_, err = c.ResolveResume(t.Context(), "shard-2")
	require.NoError(t, err)
	d, err = c.ResolveResume(t.Context(), "shard-2")
	require.NoError(t, err)
	assert.Equal(t, resumeFailover, d.Mode)
}

// TestCheckpointerMarkShardCompletedKeepsStoredPosition (CON-599): marking a
// shard completed without a final position (nothing was delivered from it
// during this run) must keep the row's stored SequenceNumber and, in global
// mode, its ApproximateCreationTime. The other region derives its failover
// cutoff from those times, so losing one would skip the records between the
// shard's last checkpoint and the next cutoff.
func TestCheckpointerMarkShardCompletedKeepsStoredPosition(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "global"}[global], func(t *testing.T) {
			store := map[string]map[string]types.AttributeValue{}
			api := sharedStoreAPI(store)
			if global {
				api.describeTable = func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
					return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
						TableStatus: types.TableStatusActive,
						KeySchema:   globalTableKeySchema(),
						Replicas:    []types.ReplicaDescription{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("us-west-2")}},
					}}, nil
				}
				api.query = func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
					out := &dynamodb.QueryOutput{}
					for _, r := range store {
						out.Items = append(out.Items, r)
					}
					return out, nil
				}
			}
			c, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
				TableName: "cps", SourceTable: "t", StreamArn: "arn:A", CheckpointLimit: 1,
				GlobalTable: global, Region: "us-east-1", ReplicaRegions: []string{"us-west-2"},
			}, checkpointTestLogger())
			require.NoError(t, err)

			require.NoError(t, c.Set(t.Context(), "shard-1", "seq-7", "2026-06-16T10:00:00Z"))
			require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-1", "", ""))
			require.Len(t, store, 1)
			var row map[string]types.AttributeValue
			for _, r := range store {
				row = r
			}
			assert.True(t, isShardCompletedRow(row))
			assert.Equal(t, "seq-7", row["SequenceNumber"].(*types.AttributeValueMemberS).Value)
			if global {
				assert.Equal(t, "2026-06-16T10:00:00Z", row["ApproximateCreationTime"].(*types.AttributeValueMemberS).Value)
				assert.Equal(t, "arn:A", row["StreamArn"].(*types.AttributeValueMemberS).Value)
			} else {
				assert.NotContains(t, row, "ApproximateCreationTime")
			}

			d, err := c.ResolveResume(t.Context(), "shard-1")
			require.NoError(t, err)
			assert.Equal(t, resumeCompleted, d.Mode)

			// A final position, when known, still replaces the stored one.
			require.NoError(t, c.MarkShardCompleted(t.Context(), "shard-1", "seq-9", "2026-06-16T11:00:00Z"))
			for _, r := range store {
				row = r
			}
			assert.Equal(t, "seq-9", row["SequenceNumber"].(*types.AttributeValueMemberS).Value)
			if global {
				assert.Equal(t, "2026-06-16T11:00:00Z", row["ApproximateCreationTime"].(*types.AttributeValueMemberS).Value)
			}
		})
	}
}
