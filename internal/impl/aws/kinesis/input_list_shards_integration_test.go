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
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/awstest"
)

// pagedListShardsAPI caps every ListShards call at one shard, so that a
// stream with a few shards is listed over several pages.
type pagedListShardsAPI struct {
	client *kinesis.Client
	calls  []*kinesis.ListShardsInput
}

func (p *pagedListShardsAPI) ListShards(ctx context.Context, in *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	in.MaxResults = aws.Int32(1)
	p.calls = append(p.calls, in)
	return p.client.ListShards(ctx, in, optFns...)
}

func TestIntegrationKinesisListAllShardsPaginates(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := awstest.GetLocalStack(t)
	ctx := t.Context()

	want, err := createKinesisShards(ctx, t, port, "list-shards-paging", 3)
	require.NoError(t, err)
	require.Len(t, want, 3)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = aws.String(fmt.Sprintf("http://localhost:%v", port))
	client := kinesis.NewFromConfig(conf)

	summary, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String("stream-list-shards-paging"),
	})
	require.NoError(t, err)
	arn := aws.ToString(summary.StreamDescriptionSummary.StreamARN)

	api := &pagedListShardsAPI{client: client}
	shards, err := listAllShards(ctx, api, arn)
	require.NoError(t, err)
	assert.ElementsMatch(t, want, shardIDs(shards))

	// One shard per page, and the token alone identifies the stream after the
	// first page.
	require.Len(t, api.calls, 3)
	assert.Equal(t, arn, aws.ToString(api.calls[0].StreamARN))
	for _, call := range api.calls[1:] {
		assert.NotNil(t, call.NextToken)
		assert.Nil(t, call.StreamARN)
	}
}
