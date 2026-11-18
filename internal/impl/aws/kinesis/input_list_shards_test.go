// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kinesis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockListShardsAPI serves ListShards pages keyed by NextToken: the empty key
// is the first page.
type mockListShardsAPI struct {
	pages map[string]*kinesis.ListShardsOutput
	errOn string
	calls []*kinesis.ListShardsInput
}

func (m *mockListShardsAPI) ListShards(_ context.Context, in *kinesis.ListShardsInput, _ ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	m.calls = append(m.calls, in)
	token := aws.ToString(in.NextToken)
	if m.errOn != "" && token == m.errOn {
		return nil, errors.New("boom")
	}
	page, ok := m.pages[token]
	if !ok {
		return nil, fmt.Errorf("unknown token %q", token)
	}
	return page, nil
}

func shardIDs(shards []types.Shard) []string {
	ids := make([]string, 0, len(shards))
	for _, s := range shards {
		ids = append(ids, aws.ToString(s.ShardId))
	}
	return ids
}

func shardPage(nextToken string, ids ...string) *kinesis.ListShardsOutput {
	out := &kinesis.ListShardsOutput{}
	for _, id := range ids {
		out.Shards = append(out.Shards, types.Shard{ShardId: aws.String(id)})
	}
	if nextToken != "" {
		out.NextToken = aws.String(nextToken)
	}
	return out
}

func TestListAllShardsFollowsPagination(t *testing.T) {
	api := &mockListShardsAPI{pages: map[string]*kinesis.ListShardsOutput{
		"":   shardPage("t1", "shard-0", "shard-1"),
		"t1": shardPage("t2", "shard-2"),
		"t2": shardPage("", "shard-3", "shard-4"),
	}}

	shards, err := listAllShards(t.Context(), api, "arn:stream")
	require.NoError(t, err)
	assert.Equal(t, []string{"shard-0", "shard-1", "shard-2", "shard-3", "shard-4"}, shardIDs(shards))

	require.Len(t, api.calls, 3)
	// The first page identifies the stream by ARN, the following pages by
	// token alone: ListShards rejects a request carrying both.
	assert.Equal(t, "arn:stream", aws.ToString(api.calls[0].StreamARN))
	assert.Nil(t, api.calls[0].NextToken)
	for _, call := range api.calls[1:] {
		assert.Nil(t, call.StreamARN)
		assert.Nil(t, call.StreamName)
	}
	assert.Equal(t, "t1", aws.ToString(api.calls[1].NextToken))
	assert.Equal(t, "t2", aws.ToString(api.calls[2].NextToken))
}

func TestListAllShardsSinglePage(t *testing.T) {
	api := &mockListShardsAPI{pages: map[string]*kinesis.ListShardsOutput{
		"": shardPage("", "shard-0"),
	}}

	shards, err := listAllShards(t.Context(), api, "arn:stream")
	require.NoError(t, err)
	assert.Equal(t, []string{"shard-0"}, shardIDs(shards))
	require.Len(t, api.calls, 1)
}

func TestListAllShardsPageError(t *testing.T) {
	api := &mockListShardsAPI{
		pages: map[string]*kinesis.ListShardsOutput{
			"": shardPage("t1", "shard-0"),
		},
		errOn: "t1",
	}

	shards, err := listAllShards(t.Context(), api, "arn:stream")
	require.EqualError(t, err, "boom")
	assert.Nil(t, shards, "a failed page must not yield a partial shard list")
}
