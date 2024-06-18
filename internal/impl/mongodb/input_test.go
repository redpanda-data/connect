package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestMongoInputEmptyShutdown(t *testing.T) {
	conf := `
url: "mongodb://localhost:27017"
username: foouser
password: foopass
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`

	spec := mongoConfigSpec()
	env := service.NewEnvironment()
	resources := service.MockResources()

	mongoConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	mongoInput, err := newMongoInput(mongoConfig, resources.Logger())
	require.NoError(t, err)
	require.NoError(t, mongoInput.Close(context.Background()))
}

func TestInputIntegration(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "latest",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD=secret",
		},
		ExposedPorts: []string{"27017"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var mongoClient *mongo.Client
	require.NoError(t, err)

	dbName := "TestDB"
	collName := "TestCollection"
	require.NoError(t, pool.Retry(func() error {
		if mongoClient, err = mongo.Connect(context.Background(), options.Client().
			SetConnectTimeout(10*time.Second).
			SetSocketTimeout(30*time.Second).
			SetServerSelectionTimeout(30*time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:"+resource.GetPort("27017/tcp"))); err != nil {
			return err
		}
		if err := mongoClient.Database(dbName).CreateCollection(context.Background(), collName); err != nil {
			_ = mongoClient.Disconnect(context.Background())
			return err
		}
		return nil
	}))

	coll := mongoClient.Database(dbName).Collection(collName)
	sampleData := []any{
		bson.M{
			"name": "John",
			"age":  15,
		},
		bson.M{
			"name": "Michael",
			"age":  34,
		},
		bson.M{
			"name": "Mary",
			"age":  34,
		},
		bson.M{
			"name": "Mathews",
			"age":  29,
		},
		bson.M{
			"name": "Peter",
			"age":  13,
		},
		bson.M{
			"name": "James",
			"age":  16,
		},
		bson.M{
			"name": "Juliet",
			"age":  53,
		},
	}

	_, err = coll.InsertMany(context.Background(), sampleData)
	require.NoError(t, err)

	type testCase struct {
		query           func(coll *mongo.Collection) (*mongo.Cursor, error)
		placeholderConf string
		jsonMarshalMode JSONMarshalMode
	}
	limit := int64(3)
	cases := map[string]testCase{
		"find": {
			query: func(coll *mongo.Collection) (*mongo.Cursor, error) {
				return coll.Find(context.Background(), bson.M{
					"age": bson.M{
						"$gte": 18,
					},
				}, &options.FindOptions{
					Sort: bson.M{
						"name": 1,
					},
					Limit: &limit,
				})
			},
			placeholderConf: `
url: "mongodb://localhost:%s"
username: mongoadmin
password: secret
database: "TestDB"
collection: "TestCollection"
json_marshal_mode: relaxed
query: |
  root.age = {"$gte": 18}
batchSize: 2
sort:
  name: 1
limit: 3
`,
			jsonMarshalMode: JSONMarshalModeRelaxed,
		},
		"aggregate": {
			query: func(coll *mongo.Collection) (*mongo.Cursor, error) {
				return coll.Aggregate(context.Background(), []any{
					bson.M{
						"$match": bson.M{
							"age": bson.M{
								"$gte": 18,
							},
						},
					},
					bson.M{
						"$sort": bson.M{
							"name": 1,
						},
					},
					bson.M{
						"$limit": limit,
					},
				})
			},
			placeholderConf: `
url: "mongodb://localhost:%s"
username: mongoadmin
password: secret
database: "TestDB"
collection: "TestCollection"
operation: "aggregate"
json_marshal_mode: canonical
query: |
  root = [
    {
      "$match": {
        "age": {
          "$gte": 18
        }
      }
    },
    {
      "$sort": {
        "name": 1
      }
    },
    {
      "$limit": 3
    }
  ]
batchSize: 2
`,
			jsonMarshalMode: JSONMarshalModeCanonical,
		},
	}

	port := resource.GetPort("27017/tcp")
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			testInput(t, port, tc.query, tc.placeholderConf, tc.jsonMarshalMode)
		})
	}
}

func testInput(
	t *testing.T,
	port string,
	controlQuery func(collection *mongo.Collection) (cursor *mongo.Cursor, err error),
	placeholderConf string,
	jsonMarshalMode JSONMarshalMode,
) {
	t.Helper()

	controlCtx := context.Background()
	controlConn, err := mongo.Connect(controlCtx, options.Client().ApplyURI("mongodb://mongoadmin:secret@localhost:"+port))
	require.NoError(t, err)
	controlColl := controlConn.Database("TestDB").Collection("TestCollection")
	controlCur, err := controlQuery(controlColl)
	require.NoError(t, err)
	var wantResults []map[string]any
	err = controlCur.All(controlCtx, &wantResults)
	require.NoError(t, err)
	var wantMsgs [][]byte
	for _, res := range wantResults {
		resBytes, err := bson.MarshalExtJSON(res, jsonMarshalMode == JSONMarshalModeCanonical, false)
		require.NoError(t, err)
		wantMsgs = append(wantMsgs, resBytes)
	}

	conf := fmt.Sprintf(placeholderConf, port)

	spec := mongoConfigSpec()
	env := service.NewEnvironment()
	resources := service.MockResources()

	mongoConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	mongoInput, err := newMongoInput(mongoConfig, resources.Logger())
	require.NoError(t, err)

	ctx := context.Background()
	err = mongoInput.Connect(ctx)
	require.NoError(t, err)

	// read all batches
	var actualMsgs service.MessageBatch
	for {
		batch, ack, err := mongoInput.ReadBatch(ctx)
		if err == service.ErrEndOfInput {
			break
		}
		require.NoError(t, err)
		actualMsgs = append(actualMsgs, batch...)
		require.NoError(t, ack(ctx, nil))
	}

	// compare to wanted messages
	for i, wMsg := range wantMsgs {
		msg := actualMsgs[i]
		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, string(wMsg), string(msgBytes))
	}
	_, ack, err := mongoInput.ReadBatch(ctx)
	assert.Equal(t, service.ErrEndOfInput, err)
	require.Nil(t, ack)

	require.NoError(t, mongoInput.Close(context.Background()))
}
