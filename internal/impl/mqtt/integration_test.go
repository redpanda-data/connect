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
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationMQTT(t *testing.T) {
	// integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("ncarlier/mqtt", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		inConf := mqtt.NewClientOptions().SetClientID("UNIT_TEST")
		inConf = inConf.AddBroker(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("1883/tcp")))

		mIn := mqtt.NewClient(inConf)
		tok := mIn.Connect()
		tok.Wait()
		if cErr := tok.Error(); cErr != nil {
			return cErr
		}
		mIn.Disconnect(0)
		return nil
	}))

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
		integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
	t.Run("with generated suffix", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
			integration.StreamTestOptMaxInFlight(10),
			integration.StreamTestOptVarSet("VAR1", "nanoid"),
		)
	})
}
