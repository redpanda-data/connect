package azure

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
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
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, fmt.Errorf("failed to read response body: %w", err)
		}
		newBody := strings.ReplaceAll(string(bodyBytes), "<Snapshot/>", "")
		resp.Body = io.NopCloser(strings.NewReader(newBody))
		resp.ContentLength = int64(len(newBody))
	}

	return resp, err
}

func TestIntegrationAzure(t *testing.T) {
	integration.CheckSkip(t)
	// Don't run this test by default, because it messes around with the
	// http.DefaultClient
	t.Skip()

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
		ExposedPorts: []string{"10000/tcp", "10001/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

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
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: "UseDevelopmentStorage=true;"

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: "UseDevelopmentStorage=true;"
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarOne(dummyContainer),
			integration.StreamTestOptVarTwo(dummyPrefix),
		)
	})

	// TODO: Re-enable this after https://github.com/Azure/Azurite/issues/682 is fixed
	// 	t.Run("blob_storage_append", func(t *testing.T) {
	// 		template := `
	// output:
	//   azure_blob_storage:
	//     blob_type: APPEND
	//     container: $VAR1
	//     max_in_flight: 1
	//     path: $VAR2/data.txt
	//     public_access_level: PRIVATE
	//     storage_connection_string: "UseDevelopmentStorage=true;"

	// input:
	//   azure_blob_storage:
	//     container: $VAR1
	//     prefix: $VAR2/data.txt
	//     storage_connection_string: "UseDevelopmentStorage=true;"
	// `
	// 		integration.StreamTests(
	// 			integration.StreamTestOpenCloseIsolated(),
	// 		).Run(
	// 			t, template,
	// 			integration.StreamTestOptVarOne(dummyContainer),
	// 			integration.StreamTestOptVarTwo(dummyPrefix),
	// 		)
	// 	})

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

func TestIntegrationAzureCosmosDB(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		Env: []string{
			// Controls how many database, container and partition combinations are supported.
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=2",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false",
		},
		ExposedPorts: []string{"8081/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// Start a HTTP -> HTTPS proxy server on a background goroutine to work around the self-signed certificate that the
	// CosmosDB container provides, because unfortunately, it doesn't expose a plain HTTP endpoint.
	// This listener will be owned and closed automatically by the HTTP server
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		url, err := url.Parse("https://localhost:" + resource.GetPort("8081/tcp"))
		require.NoError(t, err)

		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		p := httputil.NewSingleHostReverseProxy(url)
		p.Transport = customTransport
		// Ignore proxy errors
		p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {}

		p.ServeHTTP(res, req)
	})}
	go func() {
		require.ErrorIs(t, srv.Serve(listener), http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		assert.NoError(t, srv.Close())
	})

	_, servicePort, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	err = pool.Retry(func() error {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		client := &http.Client{Transport: customTransport}

		resp, err := client.Get("http://localhost:" + servicePort + "/_explorer/emulator.pem")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get emulator.pem, got status: %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return errors.New("failed to get emulator.pem")
		}

		return nil
	})
	require.NoError(t, err, "Failed to start CosmosDB emulator")

	emulatorKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	dummyDatabase := "Asgard"
	dummyContainer := "Valhalla"
	dummyPartitionKey := "PartOne"
	dummyPartitionKeyPath := "/" + dummyPartitionKey
	dummyPartitionKeyValue := "Mimir"
	t.Run("cosmosdb", func(t *testing.T) {
		template := `
output:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database_id: $VAR2-$ID
    container_id: $VAR3
    partition_key: ${! json("PartOne") }
  processors:
    - mapping: |
        root.id = uuid_v4()
        # Assign a fixed value to the partition key so we can use the same value in input.azure_cosmosdb.partition_key
        root.PartOne = "$VAR4"
        # Stash the actual message in the content field
        root.content = content().string()

input:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database_id: $VAR2-$ID
    container_id: $VAR3
    partition_key: $VAR4
    query: |
      SELECT * FROM $VAR3
  processors:
    - mapping: |
        root = this.content
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptVarOne(emulatorKey),
			integration.StreamTestOptVarTwo(dummyDatabase),
			integration.StreamTestOptVarThree(dummyContainer),
			integration.StreamTestOptVarFour(dummyPartitionKeyValue),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				cred, err := azcosmos.NewKeyCredential(vars.Var1)
				require.NoError(t, err)

				client, err := azcosmos.NewClientWithKey("http://localhost:"+servicePort, cred, nil)
				require.NoError(t, err)

				_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{
					ID: vars.Var2 + "-" + vars.ID,
				}, nil)
				require.NoError(t, err)

				db, err := client.NewDatabase(vars.Var2 + "-" + vars.ID)
				require.NoError(t, err)

				_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
					ID: vars.Var3,
					PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
						Paths: []string{dummyPartitionKeyPath},
					},
				}, nil)
				require.NoError(t, err)
			}),
		)
	})
}
