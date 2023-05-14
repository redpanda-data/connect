package azure

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
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
		Tag:        "3.23.0",
		// Expose Azurite ports in the random port range, so we don't clash with
		// other apps.
		ExposedPorts: []string{"10000/tcp", "10001/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	blobServicePort := resource.GetPort("10000/tcp")
	connStr := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:%s/devstoreaccount1;", blobServicePort)
	dummyContainer := "jotunheim"
	dummyPrefix := "kvenn"
	// Wait for Azurite to properly start up
	// Copied from https://github.com/mfamador/data-webhooks/blob/2dca9b0fa36bcbadf38884fb1a2e8a3614e6135e/lib/docker_containers.go#L225-L236
	err = pool.Retry(func() error {
		client, eerr := azblob.NewClientFromConnectionString(connStr, nil)
		if eerr != nil {
			return eerr
		}
		if _, err := client.CreateContainer(context.Background(), dummyContainer, nil); err != nil {
			if containerAlreadyExists(err) {
				return nil
			}
			return err
		}
		return nil
	})
	require.NoError(t, err, "Failed to start Azurite")

	t.Run("blob_storage", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:$PORT/devstoreaccount1;"

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:$PORT/devstoreaccount1;"
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyContainer),
			integration.StreamTestOptVarTwo(dummyPrefix),
			integration.StreamTestOptPort(blobServicePort),
		)
	})

	t.Run("blob_storage_append", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: APPEND
    container: $VAR1
    max_in_flight: 1
    path: $VAR2/data.txt
    public_access_level: PRIVATE
    storage_connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:$PORT/devstoreaccount1;"

input:
  azure_blob_storage:
    container: $VAR1
    prefix: $VAR2/data.txt
    storage_connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:$PORT/devstoreaccount1;"
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyContainer),
			integration.StreamTestOptVarTwo(dummyPrefix),
			integration.StreamTestOptPort(blobServicePort),
		)
	})

	os.Setenv("AZURITE_QUEUE_ENDPOINT_PORT", resource.GetPort("10001/tcp"))
	dummyQueue := "foo"
	t.Run("queue_storage", func(t *testing.T) {
		template := `
output:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: "UseDevelopmentStorage=true;"

input:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: "UseDevelopmentStorage=true;"
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyQueue),
		)
	})
}

func containerAlreadyExists(err error) bool {
	if serr, ok := err.(*azcore.ResponseError); ok {
		return serr.ErrorCode == "ContainerAlreadyExists"
	}
	return false
}
