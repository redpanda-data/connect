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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/benthos/v4/public/service/securetls"
)

func TestIntegrationAzure(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := azurite.Run(t.Context(), "mcr.microsoft.com/azure-storage/azurite:latest",
		testcontainers.WithCmdArgs("--skipApiVersionCheck"),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	blobPortM, err := ctr.MappedPort(t.Context(), azurite.BlobPort)
	require.NoError(t, err)
	queuePortM, err := ctr.MappedPort(t.Context(), azurite.QueuePort)
	require.NoError(t, err)
	tablePortM, err := ctr.MappedPort(t.Context(), azurite.TablePort)
	require.NoError(t, err)

	connString := getEmulatorConnectionString(blobPortM.Port(), queuePortM.Port(), tablePortM.Port())

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

		// Pre-create the container so it exists before stream tests and verification.
		client, err := azblob.NewClientFromConnectionString(connString, nil)
		require.NoError(t, err)
		_, err = client.CreateContainer(t.Context(), dummyContainer, nil)
		require.NoError(t, err)

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

	// The proxy below sends each request to the emulator container in cosmosTarget. Each start attempt sets it.
	var cosmosTarget atomic.Pointer[url.URL]

	// Start a HTTP -> HTTPS proxy server on a background goroutine to work around the self-signed certificate that the
	// CosmosDB container provides, because unfortunately, it doesn't expose a plain HTTP endpoint.
	// The proxy must listen on port 8081 because the CosmosDB SDK discovers the emulator's endpoint from
	// service responses. With AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1, the emulator advertises
	// 127.0.0.1:8081, so the proxy must be on that port for subsequent SDK requests to be routed through it.
	// This listener will be owned and closed automatically by the HTTP server
	listener, err := net.Listen("tcp", ":8081")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = securetls.WithInsecureSkipVerify(securetls.SecurityLevelNormal)

		p := httputil.NewSingleHostReverseProxy(cosmosTarget.Load())
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

	// getCert returns nil when the emulator serves its certificate through the proxy.
	getCert := func() error {
		resp, err := http.Get("http://127.0.0.1:" + servicePort + "/_explorer/emulator.pem")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %s", resp.Status)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return errors.New("empty certificate")
		}
		return nil
	}

	// errEmulatorExited marks a start attempt that failed because the emulator container stopped.
	errEmulatorExited := errors.New("emulator container is not running")

	// startEmulator starts an emulator container and waits until it serves its certificate. In CI, this is 6-15s
	// after the port wait. The wait stops before the package timeout, so that the test can log why the emulator is not
	// ready. The wait stops early when the container stops.
	startEmulator := func() (testcontainers.Container, error) {
		cosmosStartupTimeout := 30 * time.Second
		if deadline, ok := t.Deadline(); ok {
			cosmosStartupTimeout = time.Until(deadline) - 100*time.Millisecond
		}

		ctr, err := testcontainers.Run(t.Context(), "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest",
			testcontainers.WithImagePlatform("linux/amd64"),
			testcontainers.WithExposedPorts("8081/tcp"),
			testcontainers.WithEnv(map[string]string{
				// The bigger the value, the longer it takes for the container to start up.
				"AZURE_COSMOS_EMULATOR_PARTITION_COUNT":         "4",
				"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE": "false",
				// Override the emulator's advertised IP so the SDK doesn't try to connect to
				// the container's internal Docker bridge IP (e.g. 172.17.0.x).
				"AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE": "127.0.0.1",
			}),
			testcontainers.WithWaitStrategy(
				wait.ForListeningPort("8081/tcp").WithStartupTimeout(cosmosStartupTimeout),
			),
		)
		testcontainers.CleanupContainer(t, ctr)
		if err != nil {
			return ctr, fmt.Errorf("%w: %w", errEmulatorExited, err)
		}

		host, err := ctr.Host(t.Context())
		if err != nil {
			return ctr, err
		}
		port, err := ctr.MappedPort(t.Context(), "8081/tcp")
		if err != nil {
			return ctr, err
		}
		cosmosTarget.Store(&url.URL{Scheme: "https", Host: net.JoinHostPort(host, port.Port())})

		deadline := time.Now().Add(min(cosmosStartupTimeout, 2*time.Minute))
		for {
			err := getCert()
			if err == nil {
				return ctr, nil
			}
			if state, stateErr := ctr.State(t.Context()); stateErr == nil && !state.Running {
				return ctr, fmt.Errorf("%w (status=%s exit_code=%d): %w", errEmulatorExited, state.Status, state.ExitCode, err)
			}
			if time.Now().After(deadline) {
				return ctr, err
			}
			select {
			case <-t.Context().Done():
				return ctr, err
			case <-time.After(time.Second):
			}
		}
	}

	// When other test packages load the Docker host, the emulator can crash a few seconds after the start
	// ("ASSERT: Expression=(!currentWorker->IsPreemptiveOnEntry())") and the container exits with code 1. Then the
	// test starts a new container. An emulator that runs but is not ready is not started again.
	const maxAttempts = 3
	var cosmosCtr testcontainers.Container
	for attempt := 1; ; attempt++ {
		cosmosCtr, err = startEmulator()
		if !errors.Is(err, errEmulatorExited) || attempt == maxAttempts {
			break
		}
		t.Logf("Cosmos DB emulator start attempt %d/%d failed: %v", attempt, maxAttempts, err)
	}
	if err != nil {
		if cosmosCtr == nil {
			require.FailNow(t, "Failed to start CosmosDB emulator", "error: %v", err)
		}
		if state, err := cosmosCtr.State(t.Context()); err != nil {
			t.Logf("Cannot get the Cosmos DB emulator container state: %v", err)
		} else {
			t.Logf("Cosmos DB emulator container state: status=%s exit_code=%d oom_killed=%v error=%q",
				state.Status, state.ExitCode, state.OOMKilled, state.Error)
		}
		if logs, err := cosmosCtr.Logs(t.Context()); err != nil {
			t.Logf("Cannot get the Cosmos DB emulator container logs: %v", err)
		} else {
			b, _ := io.ReadAll(logs)
			logs.Close()
			t.Logf("Cosmos DB emulator container logs:\n%s", b)
		}
		require.FailNow(t, "Failed to start CosmosDB emulator", "last error: %v", err)
	}

	emulatorKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	dummyDatabase := "Asgard"
	dummyContainer := "Valhalla"
	dummyPartitionKeyField := "Ifing"
	dummyPartitionKeyValue := "Jotunheim"

	dbSetup := func(t testing.TB, ctx context.Context, databaseID string) {
		t.Helper()

		cred, err := azcosmos.NewKeyCredential(emulatorKey)
		require.NoError(t, err)

		client, err := azcosmos.NewClientWithKey("http://127.0.0.1:"+servicePort, cred, nil)
		require.NoError(t, err)

		db, err := client.NewDatabase(databaseID)
		require.NoError(t, err)

		// The certificate is available before the emulator can create containers. Under load, CreateContainer can
		// fail with 408 Request Timeout after the SDK retries. Try again until ctx stops. A try can fail after the
		// emulator created the resource, so 409 Conflict means that the resource exists.
		isConflict := func(err error) bool {
			rErr, ok := errors.AsType[*azcore.ResponseError](err)
			return ok && rErr.StatusCode == http.StatusConflict
		}
		create := func() error {
			_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: databaseID}, nil)
			if err != nil && !isConflict(err) {
				return fmt.Errorf("create database: %w", err)
			}
			_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
				ID: dummyContainer,
				PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
					Paths: []string{"/" + dummyPartitionKeyField},
				},
			}, nil)
			if err != nil && !isConflict(err) {
				return fmt.Errorf("create container: %w", err)
			}
			// A new container can reply 408 Request Timeout to the first writes, also after CreateContainer passed.
			// The output test then fails on its first batch. Write a probe item to the partition that the tests use,
			// until the write passes. Upsert is safe to repeat. The probe item has no "foo" field, so the input query
			// does not return it.
			probe := fmt.Appendf(nil, `{"id":"probe","%s":"%s"}`, dummyPartitionKeyField, dummyPartitionKeyValue)
			ctr, err := db.NewContainer(dummyContainer)
			if err != nil {
				return fmt.Errorf("get container: %w", err)
			}
			if _, err := ctr.UpsertItem(ctx, azcosmos.NewPartitionKeyString(dummyPartitionKeyValue), probe, nil); err != nil {
				return fmt.Errorf("write probe item: %w", err)
			}
			return nil
		}
		for {
			err := create()
			if err == nil {
				return
			}
			t.Logf("Cosmos DB setup failed, trying again: %v", err)
			select {
			case <-ctx.Done():
				require.NoError(t, err)
			case <-time.After(time.Second):
			}
		}
	}

	t.Run("cosmosdb output -> input roundtrip", func(t *testing.T) {
		template := `
output:
  azure_cosmosdb:
    endpoint: http://127.0.0.1:$PORT
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
    endpoint: http://127.0.0.1:$PORT
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
endpoint: http://127.0.0.1:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
auto_id: false
operation: Create
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		readConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://127.0.0.1:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
item_id: ${! json("id") }
operation: Read
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		patchConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://127.0.0.1:%s
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
