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

package mqtt

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func startMQTT(t *testing.T) string {
	t.Helper()

	ctr, err := testcontainers.Run(t.Context(), "ncarlier/mqtt:latest",
		testcontainers.WithExposedPorts("1883/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("1883/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "1883/tcp")
	require.NoError(t, err)
	return mappedPort.Port()
}

func TestIntegrationMQTT(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startMQTT(t)

	// Verify connectivity
	inConf := mqtt.NewClientOptions().SetClientID("UNIT_TEST")
	inConf = inConf.AddBroker(fmt.Sprintf("tcp://localhost:%v", port))
	mIn := mqtt.NewClient(inConf)
	tok := mIn.Connect()
	tok.Wait()
	require.NoError(t, tok.Error())
	mIn.Disconnect(0)

	template := `
output:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID
    dynamic_client_id_suffix: "$VAR1"
    max_in_flight: $MAX_IN_FLIGHT

input:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    client_id: client-input-$ID
    dynamic_client_id_suffix: "$VAR1"
    clean_session: false
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		// integration.StreamTestStreamParallelLossy(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(port),
		integration.StreamTestOptVarSet("VAR1", ""),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(port),
			integration.StreamTestOptMaxInFlight(10),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})
	t.Run("with generated suffix", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(port),
			integration.StreamTestOptMaxInFlight(10),
			integration.StreamTestOptVarSet("VAR1", "nanoid"),
		)
	})
}

func TestMQTTConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startMQTT(t)

	t.Run("input_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: test_input
mqtt:
  urls: [ tcp://localhost:%v ]
  topics: [ test-topic ]
  client_id: test-client
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
mqtt:
  urls: [ tcp://localhost:11111 ]
  topics: [ test-topic ]
  client_id: test-client
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
mqtt:
  urls: [ tcp://localhost:%v ]
  topic: test-topic
  client_id: test-client
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
mqtt:
  urls: [ tcp://localhost:11111 ]
  topic: test-topic
  client_id: test-client
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
