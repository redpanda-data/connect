package integration

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type AzuriteTransport struct {
	Host string
}

func (t AzuriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Intercept all requests made to 127.0.0.1:10000 and substitute the port
	// with the actual one that dockertest allocates for the Azurite container.
	// azure-sdk-for-go doesn't let us change this port when adding
	// `UseDevelopmentStorage=true;` to the connection string and using the
	// default credentials. If we use custom credentials (see the
	// `AZURITE_ACCOUNTS` env var) and don't pass `UseDevelopmentStorage=true;`
	// in the connection string, then azure-sdk-for-go will try to reach a
	// custom domain instead of localhost, so we'd have to use a similar hack
	// to point the request to localhost instead.
	if req.URL.Host == "127.0.0.1:10000" {
		req.URL.Host = t.Host
	}

	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	reqURL := req.URL.String()

	// Ugly hack: Detect API calls to storage.Container.ListBlobs and delete the
	// empty `<Snapshot/>` node from the XML response because azure-sdk-for-go
	// fails to deserialise an empty string to a valid timestamp.
	// Details here: https://github.com/Azure/Azurite/issues/663
	if strings.Contains(reqURL, "comp=list") &&
		strings.Contains(reqURL, "restype=container") {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return resp, fmt.Errorf("failed to read response body: %w", err)
		}
		newBody := strings.ReplaceAll(string(bodyBytes), "<Snapshot/>", "")
		resp.Body = ioutil.NopCloser(strings.NewReader(newBody))
		resp.ContentLength = int64(len(newBody))
	}

	return resp, err
}

var _ = registerIntegrationTest("azure", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "3.9.0",
		// Expose Azurite ports in the random port range, so we don't clash with
		// other apps.
		// Add "10001/tcp" if the queue service is also needed.
		ExposedPorts: []string{"10000/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)

	blobServicePort := resource.GetPort("10000/tcp")
	origDefaultClientTransport := http.DefaultClient.Transport
	http.DefaultClient.Transport = AzuriteTransport{Host: "localhost:" + blobServicePort}
	t.Cleanup(func() {
		http.DefaultClient.Transport = origDefaultClientTransport
	})

	// Wait for Azurite to properly start up
	// Copied from https://github.com/mfamador/data-webhooks/blob/2dca9b0fa36bcbadf38884fb1a2e8a3614e6135e/lib/docker_containers.go#L225-L236
	err = pool.Retry(func() error {
		client, eerr := storage.NewEmulatorClient()
		if eerr != nil {
			return eerr
		}
		s := client.GetBlobService()
		c := s.GetContainerReference("cont")
		if _, err = c.Exists(); err != nil {
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
  blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: "UseDevelopmentStorage=true;"
    timeout: 5s

input:
  blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: "UseDevelopmentStorage=true;"
    timeout: 5s
`
		integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(10),
		).Run(
			t, template,
			testOptVarOne(dummyContainer),
			testOptVarTwo(dummyPrefix),
		)
	})

})
