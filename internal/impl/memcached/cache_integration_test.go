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

package memcached

import (
	"fmt"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationMemcachedCache(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "memcached:latest",
		testcontainers.WithExposedPorts("11211/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("11211/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "11211/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	// Verify connectivity
	client := memcache.New(fmt.Sprintf("localhost:%s", port))
	require.NoError(t, client.Set(&memcache.Item{
		Key:        "testkey",
		Value:      []byte("testvalue"),
		Expiration: 30,
	}))

	template := `
cache_resources:
  - label: testcache
    memcached:
      addresses: [ localhost:$PORT ]
      prefix: $ID
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(port),
	)
}
