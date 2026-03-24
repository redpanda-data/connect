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
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

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
	t.Parallel()

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

func TestMongoDBConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

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
