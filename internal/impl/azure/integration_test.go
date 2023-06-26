package azure

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestIntegrationAzure(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		// Expose blob, queue and table service ports
		ExposedPorts: []string{"10000/tcp", "10001/tcp", "10002/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	connString := getEmulatorConnectionString(resource.GetPort("10000/tcp"), resource.GetPort("10001/tcp"), resource.GetPort("10002/tcp"))

	// Wait for Azurite to start up
	err = pool.Retry(func() error {
		client, err := azblob.NewClientFromConnectionString(connString, nil)
		if err != nil {
			return err

		}

		ctx, done := context.WithTimeout(context.Background(), 1*time.Second)
		defer done()

		if _, err = client.NewListContainersPager(nil).NextPage(ctx); err != nil {
			return err
		}
		return nil

	})
	require.NoError(t, err, "Failed to start Azurite")

	dummyContainer := "jotunheim"
	dummyPrefix := "kvenn"
	t.Run("blob_storage", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: $VAR3
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyContainer),
			integration.StreamTestOptVarTwo(dummyPrefix),
			integration.StreamTestOptVarThree(connString),
		)
	})

	t.Run("blob_storage_append", func(t *testing.T) {
		template := `
output:
  broker:
    pattern: fan_out_sequential
    outputs:
      - azure_blob_storage:
          blob_type: APPEND
          container: $VAR1-$ID
          max_in_flight: 1
          path: $VAR2/data.txt
          public_access_level: PRIVATE
          storage_connection_string: $VAR3
      - azure_blob_storage:
          blob_type: APPEND
          container: $VAR1-$ID
          max_in_flight: 1
          path: $VAR2/data.txt
          public_access_level: PRIVATE
          storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2/data.txt
    storage_connection_string: $VAR3
  processors:
    - mapping: |
        root = if content() == "hello worldhello world" { "hello world" } else { "" }
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyContainer),
			integration.StreamTestOptVarTwo(dummyPrefix),
			integration.StreamTestOptVarThree(connString),
		)
	})

	os.Setenv("AZURITE_QUEUE_ENDPOINT_PORT", resource.GetPort("10001/tcp"))
	dummyQueue := "foo"
	t.Run("queue_storage", func(t *testing.T) {
		template := `
output:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: $VAR2

input:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: $VAR2
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyQueue),
			integration.StreamTestOptVarTwo("UseDevelopmentStorage=true;"),
		)
	})
}
