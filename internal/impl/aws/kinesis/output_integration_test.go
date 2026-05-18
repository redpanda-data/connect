// Copyright 2024 Redpanda Data, Inc.
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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestKinesisIntegration(t *testing.T) {
	t.Skip("The docker image we're using here is old and deprecated")
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctr, err := testcontainers.Run(t.Context(), "vsouza/kinesis-local:latest",
		testcontainers.WithExposedPorts("4567/tcp"),
		testcontainers.WithCmd("--createStreamMs=5"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4567/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4567/tcp")
	require.NoError(t, err)

	endpoint := fmt.Sprintf("http://localhost:%s", mp.Port())

	pConf, err := koOutputSpec().ParseYAML(fmt.Sprintf(`
stream: foo
partition_key: ${! json("id") }
region: us-east-1
endpoint: "%v"
credentials:
  id: xxxxxx
  secret: xxxxxx
  token: xxxxxx
`, endpoint), nil)
	require.NoError(t, err)

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = &endpoint

	// bootstrap kinesis
	client := kinesis.NewFromConfig(conf)
	require.Eventually(t, func() bool {
		_, err := client.CreateStream(t.Context(), &kinesis.CreateStreamInput{
			ShardCount: aws.Int32(1),
			StreamName: aws.String("foo"),
		})
		return err == nil
	}, 30*time.Second, time.Second)

	koConf, err := koConfigFromParsed(pConf)
	require.NoError(t, err)

	t.Run("testKinesisConnect", func(t *testing.T) {
		testKinesisConnect(t, koConf, client)
	})

	t.Run("testKinesisConnectWithInvalidStream", func(t *testing.T) {
		koConf.Stream = "invalid-foo"
		testKinesisConnectWithInvalidStream(t, koConf)
	})
}

func testKinesisConnect(t *testing.T, c koConfig, client *kinesis.Client) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		require.NoError(t, r.Close(t.Context()))
	}()

	records := [][]byte{
		[]byte(`{"foo":"bar","id":123}`),
		[]byte(`{"foo":"baz","id":456}`),
		[]byte(`{"foo":"qux","id":789}`),
	}

	var msg service.MessageBatch
	for _, record := range records {
		msg = append(msg, service.NewMessage(record))
	}

	if err := r.WriteBatch(t.Context(), msg); err != nil {
		t.Fatal(err)
	}

	iterator, err := client.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		StreamName:        aws.String(c.Stream),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetRecords(t.Context(), &kinesis.GetRecordsInput{
		Limit:         aws.Int32(10),
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		t.Error(err)
	}
	if act, exp := len(out.Records), len(records); act != exp {
		t.Fatalf("Expected GetRecords response to have records with length of %d, got %d", exp, act)
	}
	for i, record := range records {
		if !bytes.Equal(out.Records[i].Data, record) {
			t.Errorf("Expected record %d to equal %v, got %v", i, record, out.Records[i])
		}
	}
}

func testKinesisConnectWithInvalidStream(t *testing.T, c koConfig) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	retries := 3
	for range retries {
		err := r.Connect(t.Context())
		assert.Error(t, err)
	}
}
