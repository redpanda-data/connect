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

package amqp1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationAMQP1(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("apache/activemq-classic",
		"latest",
		[]string{
			"ACTIVEMQ_CONNECTION_USER=guest",
			"ACTIVEMQ_CONNECTION_PASSWORD=guest",
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		client, err := amqp.Dial(ctx, fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp")), nil)
		if err == nil {
			client.Close()
		}
		return err
	}))

	templateWithFieldURL := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithFieldURLS := `
output:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    source_address: "queue:/$ID"
`

	templateWithContentTypeString := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    content_type: "string"
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	testcases := []struct {
		label    string
		template string
	}{
		{
			label:    "should handle old field url",
			template: templateWithFieldURL,
		},
		{
			label:    "should handle new field urls",
			template: templateWithFieldURLS,
		},
		{
			label:    "should handle content type string",
			template: templateWithContentTypeString,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			suite := integration.StreamTests(
				integration.StreamTestOpenClose(),
				integration.StreamTestSendBatch(10),
				integration.StreamTestStreamSequential(1000),
				integration.StreamTestStreamParallel(1000),
				integration.StreamTestMetadata(),
				integration.StreamTestMetadataFilter(),
			)
			suite.Run(
				t, tc.template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
			)

			t.Run("with max in flight", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, tc.template,
					integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
					integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
					integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
					integration.StreamTestOptMaxInFlight(10),
				)
			})
		})
	}
}
