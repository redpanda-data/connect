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

package nats

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNats(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "nats:latest",
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4222/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		natsConn, err := nats.Connect(fmt.Sprintf("tcp://localhost:%v", mp.Port()))
		if err != nil {
			return false
		}
		natsConn.Close()
		return true
	}, 30*time.Second, time.Second)

	template := `
output:
  nats:
    urls: [ tcp://localhost:$PORT ]
    subject: subject-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  nats:
    urls: [ tcp://localhost:$PORT ]
    queue: queue-$ID
    subject: subject-$ID
    prefetch_count: 1048
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(500),
		integration.StreamTestStreamParallelLossy(500),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(mp.Port()),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(mp.Port()),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
}

func TestNATSConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "nats:latest",
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4222/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		natsConn, err := nats.Connect(fmt.Sprintf("tcp://localhost:%v", mp.Port()))
		if err != nil {
			return false
		}
		natsConn.Close()
		return true
	}, 30*time.Second, time.Second)

	port := mp.Port()

	t.Run("input_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: test_input
nats:
  urls: [ tcp://localhost:%v ]
  subject: test-subject
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
nats:
  urls: [ tcp://localhost:11111 ]
  subject: test-subject
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
nats:
  urls: [ tcp://localhost:%v ]
  subject: test-subject
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
nats:
  urls: [ tcp://localhost:11111 ]
  subject: test-subject
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
