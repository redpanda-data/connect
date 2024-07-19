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

package gcp

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationGCPPubSub(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	dummyProject := "benthos"
	dummyTopic := "blobfish"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "thekevjames/gcloud-pubsub-emulator",
		Tag:          "latest",
		ExposedPorts: []string{"8681/tcp"},
		Env: []string{
			fmt.Sprintf("PUBSUB_PROJECT1=%s,%s", dummyProject, dummyTopic),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, os.Setenv("PUBSUB_EMULATOR_HOST", fmt.Sprintf("localhost:%v", resource.GetPort("8681/tcp"))))
	require.NotEqual(t, "localhost:", os.Getenv("PUBSUB_EMULATOR_HOST"))

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := pubsub.NewClient(ctx, dummyProject)
		if err != nil {
			return err
		}
		defer client.Close()

		ok, err := client.Topic(dummyTopic).Exists(ctx)
		if err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("failed to find topic: %s", dummyTopic)
		}

		return err
	}))

	template := `
output:
  gcp_pubsub:
    project: $PROJECT
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  gcp_pubsub:
    project: $PROJECT
    subscription: sub-$ID
    create_subscription:
      enabled: true
      topic: topic-$ID
`
	suiteOpts := []integration.StreamTestOptFunc{
		integration.StreamTestOptSleepAfterInput(100 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100 * time.Millisecond),
		integration.StreamTestOptTimeout(time.Minute * 5),
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			client, err := pubsub.NewClient(ctx, dummyProject)
			require.NoError(t, err)

			_, err = client.CreateTopic(ctx, fmt.Sprintf("topic-%v", vars.ID))
			require.NoError(t, err)

			client.Close()
		}),
		integration.StreamTestOptVarSet("PROJECT", dummyProject),
	}
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestMetadataFilter(),
		integration.StreamTestSendBatches(10, 1000, 10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		// integration.StreamTestAtLeastOnceDelivery(),
	)
	suite.Run(t, template, suiteOpts...)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			append([]integration.StreamTestOptFunc{integration.StreamTestOptMaxInFlight(10)}, suiteOpts...)...,
		)
	})

	t.Run("utf8 attribute values", func(t *testing.T) {
		tests := []struct {
			name        string
			key         string
			value       string
			expectedErr string
		}{
			{
				name:  "valid",
				key:   "foo",
				value: "bar",
			},
			{
				name:  "empty key",
				key:   "",
				value: "bar",
			},
			{
				name:  "empty value",
				key:   "foo",
				value: "",
			},
			{
				name:  "empty key and value",
				key:   "",
				value: "",
			},
			{
				name:        "invalid key",
				key:         "\xc0\x80",
				value:       "bar",
				expectedErr: "failed to build message attributes: metadata field \xc0\x80 contains non-UTF-8 characters",
			},
			{
				name:        "invalid control",
				key:         "foo",
				value:       "\xc0\x80",
				expectedErr: "failed to build message attributes: metadata field foo contains non-UTF-8 data: \xc0\x80",
			},
			{
				name:        "invalid high",
				key:         "foo",
				value:       "\xed\xa0\x80",
				expectedErr: "failed to build message attributes: metadata field foo contains non-UTF-8 data: \xed\xa0\x80",
			},
			{
				name:        "invalid low",
				key:         "foo",
				value:       "\xed\xbf\xbf",
				expectedErr: "failed to build message attributes: metadata field foo contains non-UTF-8 data: \xed\xbf\xbf",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				outputConf := fmt.Sprintf(`gcp_pubsub:
  project: %s
  topic: %s
`, dummyProject, dummyTopic)

				streamBuilder := service.NewStreamBuilder()
				require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))
				require.NoError(t, streamBuilder.AddOutputYAML(outputConf))

				pushFn, err := streamBuilder.AddBatchProducerFunc()
				require.NoError(t, err)

				stream, err := streamBuilder.Build()
				require.NoError(t, err)

				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					defer wg.Done()

					ctx, done := context.WithTimeout(context.Background(), 1*time.Second)
					defer done()

					msg := service.NewMessage([]byte("hello world!"))
					msg.MetaSet(test.key, test.value)
					err := pushFn(ctx, service.MessageBatch{
						msg,
					})

					if test.expectedErr != "" {
						assert.EqualError(t, err, test.expectedErr)
					} else {
						assert.NoError(t, err)
					}

					assert.NoError(t, stream.StopWithin(1*time.Second))
				}()

				require.NoError(t, stream.Run(context.Background()))

				wg.Wait()
			})

		}
	})
}
