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

package gcp_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func createGCPCloudStorageBucket(var1, id string) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Bucket(var1+"-"+id).Create(ctx, "", nil)
}

func TestIntegrationGCP(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "fsouza/fake-gcs-server",
		Tag:          "latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http", "-public-host", "localhost"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	t.Setenv("STORAGE_EMULATOR_HOST", "localhost:"+resource.GetPort("4443/tcp")) //nolint: tenv // this test runs in parallel

	// Wait for fake-gcs-server to properly start up
	err = pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancelFunc()

		client, eerr := storage.NewClient(ctx)

		if eerr != nil {
			return eerr
		}
		defer client.Close()
		buckets := client.Buckets(ctx, "")
		_, eerr = buckets.Next()
		if eerr != iterator.Done {
			return eerr
		}

		return nil
	})
	require.NoError(t, err, "Failed to start fake-gcs-server")

	dummyBucketPrefix := "jotunheim"
	dummyPathPrefix := "kvenn"

	t.Run("gcs_overwrite", func(t *testing.T) {
		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    path: $VAR2/${!counter()}.txt
    max_in_flight: 1
    collision_mode: overwrite

input:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    prefix: $VAR2
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.General["VAR1"], vars.ID))
			}),
			integration.StreamTestOptVarSet("VAR1", dummyBucketPrefix),
			integration.StreamTestOptVarSet("VAR2", dummyPathPrefix),
		)
	})

	t.Run("gcs_append", func(t *testing.T) {
		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    path: $VAR2/test.txt
    max_in_flight: 1
    collision_mode: append
input:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    prefix: $VAR2/test.txt
    scanner:
      chunker:
        size: 14
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.General["VAR1"], vars.ID))
			}),
			integration.StreamTestOptVarSet("VAR1", dummyBucketPrefix),
			integration.StreamTestOptVarSet("VAR2", dummyPathPrefix),
		)
	})

	t.Run("gcs_append_old_codec", func(t *testing.T) {
		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    path: $VAR2/test.txt
    max_in_flight: 1
    collision_mode: append
input:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    prefix: $VAR2/test.txt
    codec: chunker:14
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.General["VAR1"], vars.ID))
			}),
			integration.StreamTestOptVarSet("VAR1", dummyBucketPrefix),
			integration.StreamTestOptVarSet("VAR2", dummyPathPrefix),
		)
	})
}
