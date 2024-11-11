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

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func generateCollectionName(testID string) string {
	return regexp.MustCompile("[^a-zA-Z]+").ReplaceAllString(testID, "")
}

func TestIntegrationMongoDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "latest",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD=secret",
		},
		ExposedPorts: []string{"27017/tcp"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var mongoClient *mongo.Client

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		mongoClient, err = mongo.Connect(context.Background(), options.Client().
			SetConnectTimeout(10*time.Second).
			SetSocketTimeout(30*time.Second).
			SetServerSelectionTimeout(30*time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:"+resource.GetPort("27017/tcp")))
		return err
	}))

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
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		db := mongoClient.Database("TestDB")
		collection := db.Collection(generateCollectionName(testID))
		idInt, err := strconv.Atoi(messageID)
		if err != nil {
			return "", nil, err
		}

		filter := bson.M{"id": idInt}
		document, err := collection.FindOne(context.Background(), filter).Raw()
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
			integration.StreamTestOptPort(resource.GetPort("27017/tcp")),
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
			integration.CacheTestOptPort(resource.GetPort("27017/tcp")),
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
				cName := generateCollectionName(vars.ID)
				vars.General["VAR1"] = cName
				require.NoError(t, mongoClient.Database("TestDB").CreateCollection(ctx, cName))
			}),
		)
	})
}
