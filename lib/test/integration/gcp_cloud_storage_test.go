package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/impl/gcp"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func createGCPCloudStorageBucket(var1, id string) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	client, err := gcp.NewStorageClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Bucket(var1+"-"+id).Create(ctx, "", nil)
}

var _ = registerIntegrationTest("gcp_cloud_storage", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "fsouza/fake-gcs-server",
		Tag:          "1.21.2",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)

	os.Setenv("GCP_CLOUD_STORAGE_EMULATOR_URL", "http://localhost:"+resource.GetPort("4443/tcp")+"/storage/v1/")
	t.Cleanup(func() {
		defer os.Unsetenv("GCP_CLOUD_STORAGE_EMULATOR_URL")
	})

	// Wait for fake-gcs-server to properly start up
	err = pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()

		client, eerr := gcp.NewStorageClient(ctx)

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
	t.Run("gcs", func(t *testing.T) {
		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    path: $VAR2/${!count("$ID")}.txt
    max_in_flight: 1

input:
  gcp_cloud_storage:
    bucket: $VAR1-$ID
    prefix: $VAR2
`
		integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(10),
		).Run(
			t, template,
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				require.NoError(t, createGCPCloudStorageBucket(env.configVars.var1, env.configVars.id))
			}),
			testOptVarOne(dummyBucketPrefix),
			testOptVarTwo(dummyPathPrefix),
		)
	})
})
