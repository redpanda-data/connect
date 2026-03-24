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
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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

	ctr, err := testcontainers.Run(t.Context(), "fsouza/fake-gcs-server:latest",
		testcontainers.WithExposedPorts("4443/tcp"),
		testcontainers.WithCmd("-scheme", "http", "-public-host", "localhost"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4443/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4443/tcp")
	require.NoError(t, err)

	t.Setenv("STORAGE_EMULATOR_HOST", "localhost:"+mp.Port()) //nolint: tenv // this test runs in parallel

	// Wait for fake-gcs-server to properly start up
	require.Eventually(t, func() bool {
		ctx, cancelFunc := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancelFunc()

		client, eerr := storage.NewClient(ctx)
		if eerr != nil {
			return false
		}
		defer client.Close()
		buckets := client.Buckets(ctx, "")
		_, eerr = buckets.Next()
		return eerr == iterator.Done
	}, time.Minute, time.Second, "Failed to start fake-gcs-server")

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
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
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
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
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
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.General["VAR1"], vars.ID))
			}),
			integration.StreamTestOptVarSet("VAR1", dummyBucketPrefix),
			integration.StreamTestOptVarSet("VAR2", dummyPathPrefix),
		)
	})
}
