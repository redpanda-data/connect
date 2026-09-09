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

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func generateCollectionName(testID string) string {
	return regexp.MustCompile("[^a-zA-Z]+").ReplaceAllString(testID, "")
}

func TestIntegrationMongoDB(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)

	var mongoClient *mongo.Client
	require.Eventually(t, func() bool {
		mongoClient, err = mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:" + mp.Port()))
		return err == nil
	}, time.Minute, time.Second)

	template := `
output:
  mongodb:
    url: mongodb://localhost:$PORT
    database: TestDB
    collection: $VAR1
    username: mongoadmin
    password: secret
    operation: insert-one
    document_map: |
      root.id = this.id
      root.content = this.content
    write_concern:
      w: 1
      w_timeout: 1s
`
	queryGetFn := func(_ context.Context, testID, messageID string) (string, []string, error) {
		db := mongoClient.Database("TestDB")
		collection := db.Collection(generateCollectionName(testID))
		idInt, err := strconv.Atoi(messageID)
		if err != nil {
			return "", nil, err
		}

		filter := bson.M{"id": idInt}
		document, err := collection.FindOne(t.Context(), filter).Raw()
		if err != nil {
			return "", nil, err
		}

		value, err := document.LookupErr("content")
		if err != nil {
			return "", nil, err
		}

		return fmt.Sprintf(`{"content":%v,"id":%v}`, value.String(), messageID), nil, err
	}

	t.Run("streams", func(t *testing.T) {
		suite := integration.StreamTests(
			integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
			integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptPort(mp.Port()),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				cName := generateCollectionName(vars.ID)
				vars.General["VAR1"] = cName
				require.NoError(t, mongoClient.Database("TestDB").CreateCollection(ctx, cName))
			}),
		)
	})

	t.Run("cache", func(t *testing.T) {
		cacheTemplate := `
cache_resources:
  - label: testcache
    mongodb:
      url: mongodb://localhost:$PORT
      database: TestDB
      collection: $VAR1
      key_field: key
      value_field: value
      username: mongoadmin
      password: secret
`
		cacheSuite := integration.CacheTests(
			integration.CacheTestOpenClose(),
			integration.CacheTestMissingKey(),
			// integration.CacheTestDoubleAdd(),
			integration.CacheTestDelete(),
			integration.CacheTestGetAndSet(50),
		)
		cacheSuite.Run(
			t, cacheTemplate,
			integration.CacheTestOptPort(mp.Port()),
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
				cName := generateCollectionName(vars.ID)
				vars.General["VAR1"] = cName
				require.NoError(t, mongoClient.Database("TestDB").CreateCollection(ctx, cName))
			}),
		)
	})
}

// freeHostPort reserves an ephemeral port and immediately releases it, so it can
// be used as a fixed docker host port binding.
func freeHostPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	require.NoError(t, l.Close())
	return port
}

// TestIntegrationOutputReconnectsAfterOutage is the end-to-end proof of the
// write-error -> isConnPoolError -> service.ErrNotConnected -> Connect rebuild
// loop that the AWS IAM credential-expiry path depends on. An expired IAM
// session surfaces to the output as a handshake failure on a rebuilt pool
// connection, which is the same class of error as the connection outage staged
// here, and the only recovery is for the output to report ErrNotConnected so the
// framework re-runs Connect (which re-resolves credentials) and retries.
//
// The container is bound to a fixed host port on purpose: docker re-allocates an
// ephemeral published port on restart (verified: 27017 -> 55152 became
// 27017 -> 55153 after stop/start), which would move the server out from under
// the output's static url and make the test prove nothing.
func TestIntegrationOutputReconnectsAfterOutage(t *testing.T) {
	integration.CheckSkip(t)

	hostPort := freeHostPort(t)
	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("27017/tcp"): {{HostPort: hostPort}},
			}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	// The rest of the test is meaningless if the binding did not take effect.
	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)
	require.Equal(t, hostPort, mp.Port(), "container must be bound to the fixed host port")

	mongoClient, err := mongo.Connect(options.Client().
		SetConnectTimeout(10 * time.Second).
		SetTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		SetAuth(options.Credential{Username: "mongoadmin", Password: "secret"}).
		ApplyURI("mongodb://localhost:" + hostPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mongoClient.Disconnect(context.Background()) })
	require.Eventually(t, func() bool {
		return mongoClient.Ping(t.Context(), nil) == nil
	}, time.Minute, time.Second)

	coll := mongoClient.Database("TestDB").Collection("outage")
	countID := func(id int) int64 {
		n, err := coll.CountDocuments(context.Background(), bson.M{"id": id})
		if err != nil {
			t.Logf("count for id %d failed: %v", id, err)
			return -1
		}
		return n
	}

	builder := service.NewStreamBuilder()
	produce, err := builder.AddProducerFunc()
	require.NoError(t, err)
	require.NoError(t, builder.AddOutputYAML(fmt.Sprintf(`
mongodb:
  url: mongodb://localhost:%s
  database: TestDB
  collection: outage
  username: mongoadmin
  password: secret
  operation: insert-one
  document_map: 'root = this'
  write_concern:
    w: 1
    w_timeout: 5s
`, hostPort)))
	stream, err := builder.Build()
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	var runWG sync.WaitGroup
	runWG.Go(func() {
		if err := stream.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stream run: %v", err)
		}
	})
	t.Cleanup(func() {
		cancelRun()
		runWG.Wait()
	})

	send := func(ctx context.Context, id int) error {
		msg := service.NewMessage(nil)
		msg.SetStructured(map[string]any{"id": id})
		return produce(ctx, msg)
	}

	// Healthy baseline: writes land.
	for id := 1; id <= 3; id++ {
		require.NoError(t, send(t.Context(), id))
	}
	require.Eventually(t, func() bool { return countID(1) == 1 && countID(3) == 1 },
		30*time.Second, 250*time.Millisecond, "baseline writes never landed")

	// Outage. Stop (not Terminate) keeps the container and its fixed binding.
	stopTimeout := 30 * time.Second
	require.NoError(t, ctr.Stop(t.Context(), &stopTimeout))

	// This write cannot succeed while the server is down. The framework retries
	// the same batch forever while the output keeps reporting ErrNotConnected,
	// so produce blocks rather than failing - which is what makes the delivery
	// assertion below proof of a successful rebuild rather than of a retry
	// happening to be issued by the caller.
	outageSendCtx, cancelOutageSend := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelOutageSend()
	outageErr := make(chan error, 1)
	go func() { outageErr <- send(outageSendCtx, 4) }()

	// Give the output time to actually observe the failure before healing it.
	time.Sleep(5 * time.Second)
	require.Empty(t, outageErr, "the write must not have been resolved while the server was down")

	require.NoError(t, ctr.Start(t.Context()))
	mp, err = ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)
	require.Equal(t, hostPort, mp.Port(), "host port binding must survive a restart")

	select {
	case err := <-outageErr:
		require.NoError(t, err, "the write issued during the outage must eventually be acked")
	case <-time.After(2 * time.Minute):
		t.Fatal("the write issued during the outage was never resolved after the server came back")
	}
	require.Eventually(t, func() bool { return countID(4) == 1 },
		time.Minute, 500*time.Millisecond, "the write issued during the outage never landed")

	// And the rebuilt client keeps working for fresh traffic.
	require.NoError(t, send(t.Context(), 5))
	require.Eventually(t, func() bool { return countID(5) == 1 },
		time.Minute, 500*time.Millisecond, "writes after the outage never landed")
}

func TestMongoDBConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mongoClient, err := mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:" + mp.Port()))
		if err != nil {
			return false
		}
		defer func() {
			_ = mongoClient.Disconnect(t.Context())
		}()
		return mongoClient.Ping(t.Context(), nil) == nil
	}, time.Minute, time.Second)

	port := mp.Port()

	t.Run("input_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(fmt.Sprintf(`
label: test_input
mongodb:
  url: mongodb://localhost:%v
  database: TestDB
  collection: test-collection
  username: mongoadmin
  password: secret
  query: "root = {}"
`, port)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessInput(t.Context(), "test_input", func(i *service.ResourceInput) {
			connResults := i.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("input_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddInputYAML(`
label: test_input
mongodb:
  url: mongodb://localhost:11111
  database: TestDB
  collection: test-collection
  username: mongoadmin
  password: secret
  query: "root = {}"
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessInput(t.Context(), "test_input", func(i *service.ResourceInput) {
			connResults := i.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})

	t.Run("output_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: test_output
mongodb:
  url: mongodb://localhost:%v
  database: TestDB
  collection: test-collection
  username: mongoadmin
  password: secret
  operation: insert-one
  document_map: "root = this"
  write_concern:
    w: 1
`, port)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("output_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(`
label: test_output
mongodb:
  url: mongodb://localhost:11111
  database: TestDB
  collection: test-collection
  username: mongoadmin
  password: secret
  operation: insert-one
  document_map: "root = this"
  write_concern:
    w: 1
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})
}
