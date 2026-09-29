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
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// eventLog records test events from several fakes in one order.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(format string, args ...any) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf(format, args...))
}

// index returns the position of the first event equal to e, or -1.
func (l *eventLog) index(e string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Index(l.events, e)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type memRows = map[string]map[string]map[string]types.AttributeValue

// memCheckpointAPI is an in-memory checkpoint table keyed by hash value and
// ShardID. Once freezeStale is called, every read that is not strongly
// consistent sees the rows as they were at that moment, modelling an
// eventually consistent read that has not caught up with later writes and
// deletes. Query pages hold at most pageSize rows when pageSize is set.
type memCheckpointAPI struct {
	mu       sync.Mutex
	rows     memRows
	stale    memRows
	pageSize int
	log      *eventLog
}

func newMemCheckpointAPI(log *eventLog) *memCheckpointAPI {
	return &memCheckpointAPI{rows: memRows{}, log: log}
}

// put stores item under hash and its ShardID, bypassing the API.
func (m *memCheckpointAPI) put(hash string, item map[string]types.AttributeValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putLocked(hash, item)
}

func (m *memCheckpointAPI) putLocked(hash string, item map[string]types.AttributeValue) {
	rng := item[checkpointRangeKey].(*types.AttributeValueMemberS).Value
	if m.rows[hash] == nil {
		m.rows[hash] = map[string]map[string]types.AttributeValue{}
	}
	m.rows[hash][rng] = item
}

// freezeStale pins the view eventually consistent reads see from now on.
func (m *memCheckpointAPI) freezeStale() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stale = memRows{}
	for h, rs := range m.rows {
		m.stale[h] = maps.Clone(rs)
	}
}

// has reports whether the current (strongly consistent) view holds a row.
func (m *memCheckpointAPI) has(hash, rng string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.rows[hash][rng]
	return ok
}

func (m *memCheckpointAPI) view(consistent *bool) memRows {
	if m.stale != nil && !aws.ToBool(consistent) {
		return m.stale
	}
	return m.rows
}

// splitKey returns a checkpoint key's hash value and ShardID.
func splitKey(key map[string]types.AttributeValue) (hash, rng string) {
	for k, v := range key {
		s := v.(*types.AttributeValueMemberS).Value
		if k == checkpointRangeKey {
			rng = s
		} else {
			hash = s
		}
	}
	return hash, rng
}

func (*memCheckpointAPI) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{}}, nil
}

func (*memCheckpointAPI) CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, nil
}

func (*memCheckpointAPI) UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	return &dynamodb.UpdateTableOutput{}, nil
}

func (m *memCheckpointAPI) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hash, rng := splitKey(in.Key)
	m.log.add("get %s %s", hash, rng)
	return &dynamodb.GetItemOutput{Item: m.view(in.ConsistentRead)[hash][rng]}, nil
}

func (m *memCheckpointAPI) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hash, rng := splitKey(map[string]types.AttributeValue{
		checkpointHashKeyDefault: in.Item[checkpointHashKeyDefault],
		checkpointRangeKey:       in.Item[checkpointRangeKey],
	})
	m.log.add("put %s %s", hash, rng)
	m.putLocked(hash, in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *memCheckpointAPI) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hash, rng := splitKey(in.Key)
	m.log.add("delete %s %s", hash, rng)
	delete(m.rows[hash], rng)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *memCheckpointAPI) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var hash, prefix string
	for k, v := range in.ExpressionAttributeValues {
		s := v.(*types.AttributeValueMemberS).Value
		if k == ":snapshot_prefix" {
			prefix = s
		} else {
			hash = s
		}
	}
	rs := m.view(in.ConsistentRead)[hash]
	var after string
	if in.ExclusiveStartKey != nil {
		_, after = splitKey(in.ExclusiveStartKey)
	}
	limit := m.pageSize
	if in.Limit != nil && (limit == 0 || int(*in.Limit) < limit) {
		limit = int(*in.Limit)
	}
	out := &dynamodb.QueryOutput{}
	for _, rng := range slices.Sorted(maps.Keys(rs)) {
		if !strings.HasPrefix(rng, prefix) || (after != "" && rng <= after) {
			continue
		}
		if limit > 0 && len(out.Items) == limit {
			out.LastEvaluatedKey = map[string]types.AttributeValue{
				checkpointHashKeyDefault: &types.AttributeValueMemberS{Value: hash},
				checkpointRangeKey:       out.Items[len(out.Items)-1][checkpointRangeKey],
			}
			break
		}
		out.Items = append(out.Items, rs[rng])
	}
	return out, nil
}

// memCheckpointer builds a default-mode Checkpointer for streamArn on api.
func memCheckpointer(t *testing.T, api *memCheckpointAPI, table, streamArn string) *Checkpointer {
	t.Helper()
	return &Checkpointer{
		tableName:       "ckpt",
		sourceTable:     table,
		streamArn:       streamArn,
		checkpointLimit: 10,
		svc:             api,
		log:             service.MockResources().Logger(),
	}
}

// snapshotRow builds a snapshot checkpoint row for hash.
func snapshotRow(hash, shardID string, complete bool, lastKey map[string]types.AttributeValue) map[string]types.AttributeValue {
	row := map[string]types.AttributeValue{
		checkpointHashKeyDefault: &types.AttributeValueMemberS{Value: hash},
		checkpointRangeKey:       &types.AttributeValueMemberS{Value: shardID},
		"Complete":               &types.AttributeValueMemberBOOL{Value: complete},
	}
	if lastKey != nil {
		row["LastKey"] = &types.AttributeValueMemberM{Value: lastKey}
	}
	return row
}

// memShardRow builds a shard checkpoint row for hash.
func memShardRow(hash, shardID, seq string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		checkpointHashKeyDefault: &types.AttributeValueMemberS{Value: hash},
		checkpointRangeKey:       &types.AttributeValueMemberS{Value: shardID},
		"SequenceNumber":         &types.AttributeValueMemberS{Value: seq},
	}
}
