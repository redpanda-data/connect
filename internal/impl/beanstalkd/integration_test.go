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
	"net"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestIntegrationBeanstalkdOpenClose(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("websmurf/beanstalkd", "1.12-alpine-3.14", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+resource.GetPort("11300/tcp"), time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdSendBatch(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("websmurf/beanstalkd", "1.12-alpine-3.14", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+resource.GetPort("11300/tcp"), time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))

	suite := integration.StreamTests(
		integration.StreamTestSendBatch(10),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdStreamSequential(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("websmurf/beanstalkd", "1.12-alpine-3.14", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+resource.GetPort("11300/tcp"), time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))

	suite := integration.StreamTests(
		integration.StreamTestStreamSequential(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdStreamParallel(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("websmurf/beanstalkd", "1.12-alpine-3.14", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+resource.GetPort("11300/tcp"), time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))

	suite := integration.StreamTests(
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}
