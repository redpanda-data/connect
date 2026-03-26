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
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationAMQP1(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "apache/activemq-classic:latest",
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ACTIVEMQ_CONNECTION_USER":     "guest",
			"ACTIVEMQ_CONNECTION_PASSWORD": "guest",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5672/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "5672/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		client, err := amqp.Dial(t.Context(), fmt.Sprintf("amqp://guest:guest@localhost:%v/", mp.Port()), nil)
		if err == nil {
			client.Close()
			return true
		}
		return false
	}, time.Minute, time.Second)

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

	templateWithAnonymousTerminus := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: ""
    message_properties_to: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithAnonymousTerminusBloblang := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: ""
    message_properties_to: '${! meta("target_queue").or("queue:/$ID") }'
    max_in_flight: $MAX_IN_FLIGHT
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
		{
			label:    "should handle Anonymous Terminus pattern",
			template: templateWithAnonymousTerminus,
		},
		{
			label:    "should handle Anonymous Terminus with Bloblang interpolation",
			template: templateWithAnonymousTerminusBloblang,
		},
	}

	for _, tc := range testcases {
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
				integration.StreamTestOptPort(mp.Port()),
			)

			t.Run("with max in flight", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, tc.template,
					integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
					integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
					integration.StreamTestOptPort(mp.Port()),
					integration.StreamTestOptMaxInFlight(10),
				)
			})
		})
	}
}
