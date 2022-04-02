package gcp

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/benthosdev/benthos/v4/internal/integration"
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
	t.Parallel()

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

	os.Setenv("STORAGE_EMULATOR_HOST", "localhost:"+resource.GetPort("4443/tcp"))
	t.Cleanup(func() {
		defer os.Unsetenv("STORAGE_EMULATOR_HOST")
	})

	// Wait for fake-gcs-server to properly start up
	err = pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
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
    path: $VAR2/${!count("$ID")}.txt
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
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.Var1, testID))
			}),
			integration.StreamTestOptVarOne(dummyBucketPrefix),
			integration.StreamTestOptVarTwo(dummyPathPrefix),
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
    codec: chunker:14
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createGCPCloudStorageBucket(vars.Var1, testID))
			}),
			integration.StreamTestOptVarOne(dummyBucketPrefix),
			integration.StreamTestOptVarTwo(dummyPathPrefix),
		)
	})
}
