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

package aws

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestKinesisIntegration(t *testing.T) {
	t.Skip("The docker image we're using here is old and deprecated")
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	// start mysql container with binlog enabled
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "vsouza/kinesis-local",
		Cmd: []string{
			"--createStreamMs=5",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	port, err := strconv.ParseInt(resource.GetPort("4567/tcp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("http://localhost:%d", port)

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
	if err := pool.Retry(func() error {
		_, err := client.CreateStream(t.Context(), &kinesis.CreateStreamInput{
			ShardCount: aws.Int32(1),
			StreamName: aws.String("foo"),
		})
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

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
	for i := 0; i < retries; i++ {
		err := r.Connect(t.Context())
		assert.Error(t, err)
	}
}
