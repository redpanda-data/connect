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

package amqp09

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func doSetupAndAssertions(setQueueDeclareAutoDelete bool, t *testing.T) {
	assertQueueStateFromRabbitMQManagementAPI := func(resource *dockertest.Resource) {
		require.NotNil(t, resource)

		type Queue struct {
			AutoDelete bool `json:"auto_delete"`
		}

		client := &http.Client{
			Timeout: time.Second * 5,
		}

		url := fmt.Sprintf("http://localhost:%v/api/queues", resource.GetPort("15672/tcp"))

		req, err := http.NewRequest("GET", url, http.NoBody)
		require.NoError(t, err)

		req.SetBasicAuth("guest", "guest")
		resp, err := client.Do(req)
		require.NoError(t, err)

		queues := make([]Queue, 0)
		err = json.NewDecoder(resp.Body).Decode(&queues)
		require.NoError(t, err)

		if !setQueueDeclareAutoDelete {
			// declared queues should remain when auto-delete is not set
			assert.Contains(t, queues, Queue{AutoDelete: false})
		} else {
			// declared queues should be cleaned up when auto-delete is not set
			assert.NotContains(t, queues, Queue{AutoDelete: true})
		}
	}

	getTemplate := func() string {
		// by completely omitting this item we can exercise the default setting
		queueDeclareAutoDeleteFragment := ""
		if setQueueDeclareAutoDelete {
			queueDeclareAutoDeleteFragment = "\n      auto_delete: true"
		}

		return fmt.Sprintf(
			`
output:
  amqp_0_9:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    max_in_flight: $MAX_IN_FLIGHT
    exchange: exchange-$ID
    key: benthos-key
    exchange_declare:
      enabled: true
      type: direct
      durable: true
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_0_9:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    auto_ack: $VAR1
    queue: queue-$ID
    queue_declare:
      durable: true
      enabled: true%s
    bindings_declare:
      - exchange: exchange-$ID
        key: benthos-key
`,
			queueDeclareAutoDeleteFragment,
		)
	}

	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("rabbitmq", "management", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		client, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp")))
		if err == nil {
			_ = client.Close()
		}
		return err
	}))

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestMetadataFilter(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
	)

	// we can't run these tests when auto-delete is not set because the disconnect / reconnect cycle cleans up the queues under test
	if !setQueueDeclareAutoDelete {
		suite = append(
			suite,
			integration.StreamTests(
				integration.StreamTestStreamParallelLossy(1000),
				integration.StreamTestStreamParallelLossyThroughReconnect(1000),
			)...,
		)
	}

	streamTestOptFuncs := []integration.StreamTestOptFunc{
		integration.StreamTestOptSleepAfterInput(500 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500 * time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
		integration.StreamTestOptVarSet("VAR1", "false"),
	}

	suite.Run(
		t,
		getTemplate(),
		streamTestOptFuncs...,
	)

	t.Cleanup(func() {
		assertQueueStateFromRabbitMQManagementAPI(resource)
	})
}

func TestIntegrationAMQP09WithoutQueueDeclareAutoDelete(t *testing.T) {
	doSetupAndAssertions(false, t)
}

func TestIntegrationAMQP09WithQueueDeclareAutoDelete(t *testing.T) {
	doSetupAndAssertions(true, t)
}
