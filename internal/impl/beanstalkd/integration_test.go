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

package beanstalkd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const template string = `
output:
  beanstalkd:
    address: 127.0.0.1:$PORT
    max_in_flight: $MAX_IN_FLIGHT

input:
  beanstalkd:
    address: 127.0.0.1:$PORT
`

func startBeanstalkd(t testing.TB) string {
	t.Helper()

	ctr, err := testcontainers.Run(t.Context(), "websmurf/beanstalkd:1.12-alpine-3.14",
		testcontainers.WithExposedPorts("11300/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("11300/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "11300/tcp")
	require.NoError(t, err)
	return mappedPort.Port()
}

func TestIntegrationBeanstalkdOpenClose(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startBeanstalkd(t)

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(port),
	)
}

func TestIntegrationBeanstalkdSendBatch(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startBeanstalkd(t)

	suite := integration.StreamTests(
		integration.StreamTestSendBatch(10),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(port),
	)
}

func TestIntegrationBeanstalkdStreamSequential(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startBeanstalkd(t)

	suite := integration.StreamTests(
		integration.StreamTestStreamSequential(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(port),
	)
}

func TestIntegrationBeanstalkdStreamParallel(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := startBeanstalkd(t)

	suite := integration.StreamTests(
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(port),
	)
}
