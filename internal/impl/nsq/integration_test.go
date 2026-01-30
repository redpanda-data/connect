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

package nsq

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNSQ(t *testing.T) {
	t.Parallel()

	{
		timeout := time.Second
		conn, err := net.DialTimeout("tcp", "localhost:4150", timeout)
		if err != nil {
			t.Skip("Skipping NSQ tests as services are not running")
		}
		conn.Close()
	}

	template := `
output:
  nsq:
    nsqd_tcp_address: localhost:4150
    topic: topic-$ID
    # user_agent: ""
    max_in_flight: $MAX_IN_FLIGHT

input:
  nsq:
    nsqd_tcp_addresses: [ localhost:4150 ]
    lookupd_http_addresses: [ localhost:4160 ^]
    topic: topic-$ID
    channel: channel-$ID
    # user_agent: ""
    max_in_flight: 100
    max_attempts: 5
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
	)
	suite.Run(t, template)

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(t, template, integration.StreamTestOptMaxInFlight(10))
	})
}

func TestNSQConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("nsqio/nsq", "latest", []string{"/nsqd"})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		timeout := time.Second
		conn, err := net.DialTimeout("tcp", "localhost:"+resource.GetPort("4150/tcp"), timeout)
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	}))

	port := resource.GetPort("4150/tcp")

	t.Run("input_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: test_input
nsq:
  nsqd_tcp_addresses: [ localhost:%v ]
  lookupd_http_addresses: []
  topic: test-topic
  channel: test-channel
`, port)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessInput(t.Context(), "test_input", func(i *service.ResourceInput) {
			connResults := i.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("input_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(`
label: test_input
nsq:
  nsqd_tcp_addresses: [ localhost:11111 ]
  lookupd_http_addresses: []
  topic: test-topic
  channel: test-channel
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessInput(t.Context(), "test_input", func(i *service.ResourceInput) {
			connResults := i.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})

	t.Run("output_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: test_output
nsq:
  nsqd_tcp_address: localhost:%v
  topic: test-topic
`, port)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("output_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(`
label: test_output
nsq:
  nsqd_tcp_address: localhost:11111
  topic: test-topic
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})
}
