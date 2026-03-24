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

package kafka_test

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/stretchr/testify/require"
)

func TestRedpandaConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, _ := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testtopic", 1) == nil
	}, time.Minute, time.Second)

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: binput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  tls:
    enabled: true
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: cinput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  unordered_processing:
    enabled: true
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: dinput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  tls:
    enabled: true
  unordered_processing:
    enabled: true
`, brokerAddr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
`, brokerAddr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: boutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
  tls:
    enabled: true
`, brokerAddr)))

	resources, _, err := resBuilder.BuildSuspended()
	require.NoError(t, err)

	require.NoError(t, resources.AccessInput(t.Context(), "ainput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "binput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "cinput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "dinput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessOutput(t.Context(), "aoutput", func(o *service.ResourceOutput) {
		connResults := o.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessOutput(t.Context(), "boutput", func(o *service.ResourceOutput) {
		connResults := o.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))
}

func TestRedpandaConnectionTestSaslIntegration(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, _ := startRedpandaWithSASL(t)

	require.Eventually(t, func() bool {
		return createKafkaTopicSasl(brokerAddr, "testtopic", 1) == nil
	}, time.Minute, time.Second)

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: binput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: cinput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
  unordered_processing:
    enabled: true
`, brokerAddr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: dinput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: nope
  unordered_processing:
    enabled: true
`, brokerAddr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
`, brokerAddr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: boutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
`, brokerAddr)))

	resources, _, err := resBuilder.BuildSuspended()
	require.NoError(t, err)

	require.NoError(t, resources.AccessInput(t.Context(), "ainput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "binput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "cinput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessInput(t.Context(), "dinput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessOutput(t.Context(), "aoutput", func(o *service.ResourceOutput) {
		connResults := o.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessOutput(t.Context(), "boutput", func(o *service.ResourceOutput) {
		connResults := o.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.Error(t, connResults[0].Err)
	}))
}

func TestRedpandaConnectionTestPrematureConnectIntegration(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, _ := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testtopic", 1) == nil
	}, time.Minute, time.Second)

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
`, brokerAddr)))

	resources, closeFn, err := resBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, resources.AccessOutput(t.Context(), "aoutput", func(o *service.ResourceOutput) {
		require.NoError(t, o.WriteBatch(t.Context(), service.MessageBatch{
			service.NewMessage([]byte("1")),
			service.NewMessage([]byte("2")),
			service.NewMessage([]byte("3")),
			service.NewMessage([]byte("4")),
			service.NewMessage([]byte("5")),
		}))
	}))

	require.NoError(t, closeFn(t.Context()))

	resBuilder = service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: testingstuff
`, brokerAddr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ %v ]
  topic: testtopic
`, brokerAddr)))

	resources, _, err = resBuilder.BuildSuspended()
	require.NoError(t, err)

	require.NoError(t, resources.AccessInput(t.Context(), "ainput", func(i *service.ResourceInput) {
		connResults := i.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	require.NoError(t, resources.AccessOutput(t.Context(), "aoutput", func(o *service.ResourceOutput) {
		connResults := o.ConnectionTest(t.Context())
		require.Len(t, connResults, 1)
		require.NoError(t, connResults[0].Err)
	}))

	resBuilder = service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ %v ]
  topics: [ testtopic ]
  consumer_group: testingstuff
`, brokerAddr)))

	resources, closeFn, err = resBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, resources.AccessInput(t.Context(), "ainput", func(i *service.ResourceInput) {
		b, aFn, err := i.ReadBatch(t.Context())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(b), 1)

		mBytes, err := b[0].AsBytes()
		require.NoError(t, err)
		require.Equal(t, "1", string(mBytes))

		require.NoError(t, aFn(t.Context(), nil))
	}))

	require.NoError(t, closeFn(t.Context()))
}
