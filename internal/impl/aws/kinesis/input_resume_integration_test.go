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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/awstest"
)

// A client that is killed before its first commit period leaves a claim on
// each of its shards with a client ID and a lease but no SequenceNumber. A
// fresh client must take those shards over and deliver the whole backlog from
// the configured starting position, instead of failing the takeover after the
// lease has already been transferred and leaving the shards unconsumed.
func TestIntegrationKinesisResumesClaimsWithoutCommittedSequence(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	lsPort := awstest.GetLocalStack(t)
	ctx := t.Context()

	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	streamName := "stream-" + id
	shards, err := createKinesisShards(ctx, t, lsPort, id, 2)
	require.NoError(t, err)
	require.Len(t, shards, 2)

	endpoint := fmt.Sprintf("http://localhost:%v", lsPort)
	awsConf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	awsConf.BaseEndpoint = &endpoint

	// The backlog: written before any consumer exists.
	const recordCount = 200
	entries := make([]types.PutRecordsRequestEntry, recordCount)
	for i := range entries {
		entries[i] = types.PutRecordsRequestEntry{
			Data:         []byte(strconv.Itoa(i)),
			PartitionKey: aws.String(strconv.Itoa(i)),
		}
	}
	putRes, err := kinesis.NewFromConfig(awsConf).PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: &streamName,
		Records:    entries,
	})
	require.NoError(t, err)
	require.Zero(t, aws.ToInt32(putRes.FailedRecordCount))

	// The dead client: it claimed both shards and was killed before it
	// committed a sequence for either. Its lease duration matches the
	// lease_period of the input below, since a takeover estimates the
	// victim's last commit from the lease it finds.
	const leasePeriod = 2 * time.Second
	dead, err := newAWSKinesisCheckpointer(ctx, awsConf, "dead-client", kiddbConfig{
		Table:       streamName,
		Create:      true,
		BillingMode: "PAY_PER_REQUEST",
	}, leasePeriod, 500*time.Millisecond, 500*time.Millisecond)
	require.NoError(t, err)
	for _, shard := range shards {
		seq, err := dead.Claim(ctx, streamName, shard, "")
		require.NoError(t, err)
		require.Empty(t, seq)
	}

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(fmt.Sprintf(`
aws_kinesis:
  endpoint: %v
  streams: [ %v ]
  checkpoint_limit: 10
  dynamodb:
    table: %v
  start_from_oldest: true
  lease_period: %v
  rebalance_period: 500ms
  commit_period: 500ms
  steal_grace_period: 500ms
  region: us-east-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
`, endpoint, streamName, streamName, leasePeriod)))

	var (
		receivedMu sync.Mutex
		received   = map[string]struct{}{}
	)
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}
		receivedMu.Lock()
		received[string(b)] = struct{}{}
		receivedMu.Unlock()
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	runErr := make(chan error, 1)
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { runErr <- stream.Run(runCtx) }()

	// Taking over the first shard is a steal (the dead client holds two
	// shards, this one none); the second becomes unclaimed once its lease is
	// two lease periods stale. Both complete within a few lease periods.
	require.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received) == recordCount
	}, 30*time.Second, 200*time.Millisecond, "every record of the backlog must arrive after taking over the dead client's shards")

	require.NoError(t, stream.StopWithin(10*time.Second))
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not exit after StopWithin returned")
	}
}
