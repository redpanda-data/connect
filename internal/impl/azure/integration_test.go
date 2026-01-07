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

package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"testing"

	btls "github.com/redpanda-data/benthos/v4/internal/tls"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gofrs/uuid/v5"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/benthos/v4/public/service/securetls"
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

		ctx, done := context.WithTimeout(t.Context(), 1*time.Second)
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
    path: $VAR2/${!counter()}.txt
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
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	t.Run("blob_storage_streamed", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!counter()}.txt
    public_access_level: PRIVATE
    storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: $VAR3
    targets_input:
      azure_blob_storage:
        container: $VAR1-$ID
        prefix: $VAR2
        storage_connection_string: $VAR3
      processors:
        - mapping: 'root.name = @blob_storage_key'
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	t.Run("blob_storage_streamed_delete_file", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1
    max_in_flight: 1
    path: $VAR2/$VAR4
    public_access_level: PRIVATE
    storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1
    prefix: $VAR2
    storage_connection_string: $VAR3
    delete_objects: true
    targets_input:
      azure_blob_storage:
        container: $VAR1
        prefix: $VAR2
        storage_connection_string: $VAR3
      processors:
        - mapping: 'root.name = @blob_storage_key'
`

		u4, err := uuid.NewV4()
		require.NoError(t, err)
		dummyContainer := u4.String()
		dummyFile := "ginnungagap.txt"

		// This is a bit gross, but by pushing `integration.StreamTests()` into a subtest we force them to run before
		// asserting the that the container is empty below. This is necessary because `integration.StreamTests()` calls
		// `t.Parallel()`.
		t.Run("exec_stream_tests", func(t *testing.T) {
			integration.StreamTests(
				integration.StreamTestOpenCloseIsolated(),
			).Run(
				t, template,
				integration.StreamTestOptVarSet("VAR1", dummyContainer),
				integration.StreamTestOptVarSet("VAR2", dummyPrefix),
				integration.StreamTestOptVarSet("VAR3", connString),
				integration.StreamTestOptVarSet("VAR4", dummyFile),
			)
		})

		client, err := azblob.NewClientFromConnectionString(connString, nil)
		require.NoError(t, err)

		ctx, done := context.WithTimeout(t.Context(), 1*time.Second)
		defer done()

		file := path.Join(dummyPrefix, dummyFile)
		pager := client.NewListBlobsFlatPager(dummyContainer, &azblob.ListBlobsFlatOptions{Prefix: &file})
		require.True(t, pager.More())
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		require.Empty(t, page.Segment.BlobItems)
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
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	t.Run("queue_storage", func(t *testing.T) {
		dummyQueue := "foo"

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
			integration.StreamTestOptVarSet("VAR1", dummyQueue),
			integration.StreamTestOptVarSet("VAR2", connString),
		)
	})
}

func TestIntegrationCosmosDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

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
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=4",
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
		customTransport.TLSClientConfig = securetls.WithInsecureSkipVerify(securetls.SecurityLevelLax)

		p := httputil.NewSingleHostReverseProxy(url)
		p.Transport = customTransport
		// Don't log proxy errors, but return an error downstream
		p.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, _ error) {
			rw.WriteHeader(http.StatusBadGateway)
		}

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
		resp, err := http.Get("http://localhost:" + servicePort + "/_explorer/emulator.pem")
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
	dummyPartitionKeyField := "Ifing"
	dummyPartitionKeyValue := "Jotunheim"

	dbSetup := func(t testing.TB, ctx context.Context, databaseID string) {
		t.Helper()

		cred, err := azcosmos.NewKeyCredential(emulatorKey)
		require.NoError(t, err)

		client, err := azcosmos.NewClientWithKey("http://localhost:"+servicePort, cred, nil)
		require.NoError(t, err)

		_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{
			ID: databaseID,
		}, nil)
		require.NoError(t, err)

		db, err := client.NewDatabase(databaseID)
		require.NoError(t, err)

		_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
			ID: dummyContainer,
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths: []string{"/" + dummyPartitionKeyField},
			},
		}, nil)
		require.NoError(t, err)
	}

	t.Run("cosmosdb output -> input roundtrip", func(t *testing.T) {
		template := `
output:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database: $VAR2-$ID
    container: $VAR3
    partition_keys_map: root = "$VAR5"
    auto_id: true
    operation: Create
  processors:
    - mapping: |
        root.$VAR4 = "$VAR5"
        root.content = content().string()
        root.foo = "bar"

input:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database: $VAR2-$ID
    container: $VAR3
    partition_keys_map: root = "$VAR5"
    query: |
      select * from $VAR3 as c where c.foo = @foo
    args_mapping: |
      root = [
        { "Name": "@foo", "Value": "bar" },
      ]
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
			integration.StreamTestOptVarSet("VAR1", emulatorKey),
			integration.StreamTestOptVarSet("VAR2", dummyDatabase),
			integration.StreamTestOptVarSet("VAR3", dummyContainer),
			integration.StreamTestOptVarSet("VAR4", dummyPartitionKeyField),
			integration.StreamTestOptVarSet("VAR5", dummyPartitionKeyValue),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				dbSetup(t, ctx, fmt.Sprintf("%s-%s", dummyDatabase, vars.ID))
			}),
		)
	})

	t.Run("cosmosdb processor", func(t *testing.T) {
		dummyUUID, err := uuid.NewV4()
		require.NoError(t, err)

		ctx, done := context.WithTimeout(t.Context(), 30*time.Second)
		t.Cleanup(done)

		database := fmt.Sprintf("%s-%s", dummyDatabase, dummyUUID)
		dbSetup(t, ctx, database)

		env := service.NewEnvironment()

		createConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
auto_id: false
operation: Create
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		readConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
item_id: ${! json("id") }
operation: Read
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		patchConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
operation: Patch
patch_condition: from c where not is_defined(c.blobfish)
patch_operations:
  - operation: Add
    path: /blobfish
    value_map: root = json("blobfish")
item_id: ${! json("id") }
enable_content_response_on_write: true
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		createProc, err := newCosmosDBProcessorFromParsed(createConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { createProc.Close(ctx) })

		readProc, err := newCosmosDBProcessorFromParsed(readConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { readProc.Close(ctx) })

		patchProc, err := newCosmosDBProcessorFromParsed(patchConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { patchProc.Close(ctx) })

		var insertBatch service.MessageBatch
		for i := range 10 {
			insertBatch = append(insertBatch, service.NewMessage(
				fmt.Appendf(nil, `{
  "%s": "%s",
  "id": "%d",
  "foo": %d
}`, dummyPartitionKeyField, dummyPartitionKeyValue, i, i)),
			)
		}

		resBatches, err := createProc.ProcessBatch(ctx, insertBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(insertBatch))
		for _, m := range resBatches[0] {
			require.NoError(t, m.GetError())
		}

		var readBatch service.MessageBatch
		for i := range 10 {
			readBatch = append(readBatch, service.NewMessage(
				fmt.Appendf(nil, `{"id": "%d"}`, i)),
			)
		}
		resBatches, err = readProc.ProcessBatch(ctx, readBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(readBatch))

		blobl, err := bloblang.GlobalEnvironment().Parse(fmt.Sprintf(`root = this.with("%s", "id", "foo")`, dummyPartitionKeyField))
		require.NoError(t, err)
		for idx, m := range resBatches[0] {
			m, err := m.BloblangMutate(blobl)
			require.NoError(t, err)
			require.NoError(t, m.GetError())

			data, err := m.AsBytes()
			require.NoError(t, err)

			// Check if partition key, string and int fields are returned correctly
			expected, err := json.Marshal(map[string]any{dummyPartitionKeyField: dummyPartitionKeyValue, "id": strconv.Itoa(idx), "foo": idx})
			require.NoError(t, err)
			assert.JSONEq(t, string(expected), string(data))

			// Ensure metadata fields are set
			activityID, ok := m.MetaGetMut("activity_id")
			assert.True(t, ok)
			assert.NotEmpty(t, activityID)
			requestCharge, ok := m.MetaGetMut("request_charge")
			assert.True(t, ok)
			assert.EqualValues(t, 1.0, requestCharge)
		}

		var patchBatch service.MessageBatch
		for i := range 10 {
			patchBatch = append(patchBatch, service.NewMessage(
				fmt.Appendf(nil, `{"id": "%d", "blobfish": "are cool"}`, i)),
			)
		}
		resBatches, err = patchProc.ProcessBatch(ctx, patchBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(patchBatch))
		for _, m := range resBatches[0] {
			require.NoError(t, m.GetError())
			data, err := m.AsStructured()
			require.NoError(t, err)
			assert.Contains(t, data, "blobfish")
		}
	})
}
