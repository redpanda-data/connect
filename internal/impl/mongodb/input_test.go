package mongodb

import (
	"context"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSQLSelectInputEmptyShutdown(t *testing.T) {
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

	mongoConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newMongoInput(mongoConfig)
	require.NoError(t, err)
	require.NoError(t, selectInput.Close(context.Background()))
}

func TestInputIntegration(t *testing.T) {
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	dbName := "TestDB"
	collName := "TestCollection"
	require.NoError(t, pool.Retry(func() error {
		url := "mongodb://localhost:" + resource.GetPort("27017/tcp")
		conf := client.NewConfig()
		conf.URL = url
		conf.Username = "mongoadmin"
		conf.Password = "secret"

		if mongoClient == nil {
			mongoClient, err = conf.Client()
			if err != nil {
				return err
			}
		}

		if err := mongoClient.Connect(context.Background()); err != nil {
			return err
		}
		return mongoClient.Database(dbName).CreateCollection(context.Background(), collName)
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
	}
	cases := map[string]testCase{
		"find": {
			query: func(coll *mongo.Collection) (*mongo.Cursor, error) {
				return coll.Find(context.Background(), bson.M{
					"age": bson.M{
						"$gte": 18,
					},
				})
			},
			placeholderConf: `
url: "mongodb://localhost:%s"
username: mongoadmin
password: secret
database: "TestDB"
collection: "TestCollection"
query: |
  root.age = {"$gte": 18}
`,
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
						"$limit": 3,
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
      "$limit": 3
    }
  ]
`,
		},
	}

	port := resource.GetPort("27017/tcp")
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			testInput(port, tc.query, tc.placeholderConf, t)
		})
	}
}

func testInput(port string, controlQuery func(collection *mongo.Collection) (cursor *mongo.Cursor, err error), placeholderConf string, t *testing.T) {
	controlCtx := context.Background()
	controlConn, err := mongo.Connect(controlCtx, options.Client().ApplyURI("mongodb://mongoadmin:secret@localhost:"+port))
	require.NoError(t, err)
	controlColl := controlConn.Database("TestDB").Collection("TestCollection")
	controlCur, err := controlQuery(controlColl)
	require.NoError(t, err)
	var wantResults []map[string]any
	err = controlCur.All(controlCtx, &wantResults)
	require.NoError(t, err)
	var wantMsgs []*service.Message
	for _, res := range wantResults {
		wantMsg := service.NewMessage(nil)
		wantMsg.SetStructured(res)
		wantMsgs = append(wantMsgs, wantMsg)
	}

	conf := fmt.Sprintf(placeholderConf, port)

	spec := mongoConfigSpec()
	env := service.NewEnvironment()

	mongoConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newMongoInput(mongoConfig)
	require.NoError(t, err)

	ctx := context.Background()
	err = selectInput.Connect(ctx)
	require.NoError(t, err)
	for _, wMsg := range wantMsgs {
		msg, _, err := selectInput.Read(ctx)
		require.NoError(t, err)
		assert.Equal(t, wMsg, msg)
	}
	_, _, err = selectInput.Read(ctx)
	assert.Equal(t, service.ErrEndOfInput, err)

	require.NoError(t, selectInput.Close(context.Background()))
}
