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
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedpandaConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testtopic", 1)
	}))

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: binput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  tls:
    enabled: true
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: cinput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  unordered_processing:
    enabled: true
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: dinput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  tls:
    enabled: true
  unordered_processing:
    enabled: true
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: boutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
  tls:
    enabled: true
`, kafkaPortStr)))

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

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			"--set redpanda.enable_sasl=true",
			`--set redpanda.superusers=["admin"]`,
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	adminCreated := false

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if !adminCreated {
			var stdErr bytes.Buffer
			_, aerr := resource.Exec([]string{
				"rpk", "acl", "user", "create", "admin",
				"--password", "foobar",
				"--api-urls", "localhost:9644",
			}, dockertest.ExecOptions{
				StdErr: &stdErr,
			})
			if aerr != nil {
				return aerr
			}
			if stdErr.String() != "" {
				return errors.New(stdErr.String())
			}
			adminCreated = true
		}
		return createKafkaTopicSasl("localhost:"+kafkaPortStr, "testtopic", 1)
	}))

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: ainput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: binput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: cinput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
  unordered_processing:
    enabled: true
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: dinput
redpanda:
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: nope
  unordered_processing:
    enabled: true
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
  sasl:
    - mechanism: SCRAM-SHA-256
      username: admin
      password: foobar
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: boutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
`, kafkaPortStr)))

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

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testtopic", 1)
	}))

	resBuilder := service.NewResourceBuilder()

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
`, kafkaPortStr)))

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
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: testingstuff
`, kafkaPortStr)))

	require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: aoutput
redpanda:
  seed_brokers: [ localhost:%v ]
  topic: testtopic
`, kafkaPortStr)))

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
  seed_brokers: [ localhost:%v ]
  topics: [ testtopic ]
  consumer_group: testingstuff
`, kafkaPortStr)))

	resources, closeFn, err = resBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, resources.AccessInput(t.Context(), "ainput", func(i *service.ResourceInput) {
		b, aFn, err := i.ReadBatch(t.Context())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(b), 1)

		mBytes, err := b[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "1", string(mBytes))

		require.NoError(t, aFn(t.Context(), nil))
	}))

	require.NoError(t, closeFn(t.Context()))
}
