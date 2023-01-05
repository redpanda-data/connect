package mongodb_test

import (
	"context"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb"
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func TestProcessorIntegration(t *testing.T) {
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

		return mongoClient.Database("TestDB").CreateCollection(context.Background(), "TestCollection")
	}))

	port := resource.GetPort("27017/tcp")
	t.Run("insert", func(t *testing.T) {
		testMongoDBProcessorInsert(port, t)
	})
	t.Run("delete one", func(t *testing.T) {
		testMongoDBProcessorDeleteOne(port, t)
	})
	t.Run("delete many", func(t *testing.T) {
		testMongoDBProcessorDeleteMany(port, t)
	})
	t.Run("replace one", func(t *testing.T) {
		testMongoDBProcessorReplaceOne(port, t)
	})
	t.Run("update one", func(t *testing.T) {
		testMongoDBProcessorUpdateOne(port, t)
	})
	t.Run("find one", func(t *testing.T) {
		testMongoDBProcessorFindOne(port, t)
	})
}

func testMongoDBProcessorInsert(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := processor.MongoDBConfig{
		MongoDB: c,
		WriteConcern: client.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "",
		},
		Operation:   "insert-one",
		DocumentMap: "root.a = this.foo\nroot.b = this.bar",
	}

	conf.MongoDB = mongoConfig

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	m, err := mongodb.NewProcessor(conf, mgr)
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2"}`),
	}

	resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, len(parts)), message.QuickBatch(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2"}`),
	}

	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record is in the MongoDB
	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	result := collection.FindOne(context.Background(), bson.M{"a": "foo1", "b": "bar1"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	assert.Equal(t, `"foo1"`, aVal.String())
	assert.Equal(t, `"bar1"`, bVal.String())

	result = collection.FindOne(context.Background(), bson.M{"a": "foo2", "b": "bar2"})
	b, err = result.DecodeBytes()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo2"`, aVal.String())
	assert.Equal(t, `"bar2"`, bVal.String())
}

func testMongoDBProcessorDeleteOne(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := processor.MongoDBConfig{
		MongoDB: c,
		WriteConcern: client.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Operation: "delete-one",
		FilterMap: "root.a = this.foo\nroot.b = this.bar",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete", "b": "bar_delete"})
	assert.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := mongodb.NewProcessor(conf, mgr)
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_delete","bar":"bar_delete"}`),
	}

	resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, len(parts)), message.QuickBatch(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_delete","bar":"bar_delete"}`),
	}

	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete", "b": "bar_delete"})
	b, err := result.DecodeBytes()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorDeleteMany(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := processor.MongoDBConfig{
		MongoDB: c,
		WriteConcern: client.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Operation: "delete-many",
		FilterMap: "root.a = this.foo\nroot.b = this.bar",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c1"})
	assert.NoError(t, err)
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c2"})
	assert.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := mongodb.NewProcessor(conf, mgr)
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_delete_many","bar":"bar_delete_many"}`),
	}

	resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, len(parts)), message.QuickBatch(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_delete_many","bar":"bar_delete_many"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many"})
	b, err := result.DecodeBytes()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorReplaceOne(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := processor.MongoDBConfig{
		MongoDB: c,
		WriteConcern: client.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "",
		},
		Operation:   "replace-one",
		DocumentMap: "root.a = this.foo\nroot.b = this.bar",
		FilterMap:   "root.a = this.foo",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_old", "c": "c1"})
	assert.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := mongodb.NewProcessor(conf, mgr)
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_replace","bar":"bar_new"}`),
	}

	resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, len(parts)), message.QuickBatch(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_replace","bar":"bar_new"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_new"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_replace"`, aVal.String())
	assert.Equal(t, `"bar_new"`, bVal.String())
	assert.Equal(t, bson.RawValue{}, cVal)
}

func testMongoDBProcessorUpdateOne(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := processor.MongoDBConfig{
		MongoDB: c,
		WriteConcern: client.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Operation:   "update-one",
		DocumentMap: `root = {"$set": {"a": this.foo, "b": this.bar}}`,
		FilterMap:   "root.a = this.foo",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_old", "c": "c1"})
	assert.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := mongodb.NewProcessor(conf, mgr)
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_update","bar":"bar_update_new"}`),
	}

	resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, len(parts)), message.QuickBatch(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_update","bar":"bar_update_new"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_new"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_update"`, aVal.String())
	assert.Equal(t, `"bar_update_new"`, bVal.String())
	assert.Equal(t, `"c1"`, cVal.String())
}

func testMongoDBProcessorFindOne(port string, t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "mongodb"

	c := client.Config{
		URL:        "mongodb://localhost:" + port,
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	conf.MongoDB = processor.NewMongoDBConfig()
	conf.MongoDB.MongoDB = c
	conf.MongoDB.WriteConcern = client.WriteConcern{
		W:        "1",
		J:        false,
		WTimeout: "100s",
	}
	conf.MongoDB.Operation = "find-one"
	conf.MongoDB.FilterMap = "root.a = this.a"

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo", "b": "bar", "c": "baz", "answer_to_everything": 42})
	assert.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	for _, tt := range []struct {
		name        string
		message     string
		marshalMode client.JSONMarshalMode
		collection  string
		expected    string
		expectedErr error
	}{
		{
			name:        "canonical marshal mode",
			marshalMode: client.JSONMarshalModeCanonical,
			message:     `{"a":"foo","x":"ignore_me_via_filter_map"}`,
			expected:    `{"a":"foo","b":"bar","c":"baz","answer_to_everything":{"$numberInt":"42"}}`,
		},
		{
			name:        "relaxed marshal mode",
			marshalMode: client.JSONMarshalModeRelaxed,
			message:     `{"a":"foo","x":"ignore_me_via_filter_map"}`,
			expected:    `{"a":"foo","b":"bar","c":"baz","answer_to_everything":42}`,
		},
		{
			name:        "no documents found",
			message:     `{"a":"notfound"}`,
			expectedErr: mongo.ErrNoDocuments,
		},
		{
			name:        "collection interpolation",
			marshalMode: client.JSONMarshalModeCanonical,
			collection:  `${!json("col")}`,
			message:     `{"col":"TestCollection","a":"foo"}`,
			expected:    `{"a":"foo","b":"bar","c":"baz","answer_to_everything":{"$numberInt":"42"}}`,
		},
	} {
		if tt.collection != "" {
			conf.MongoDB.MongoDB.Collection = tt.collection
		}

		conf.MongoDB.JSONMarshalMode = tt.marshalMode

		m, err := mongodb.NewProcessor(conf, mgr)
		require.NoError(t, err)
		resMsgs, response := m.ProcessBatch(context.Background(), make([]*tracing.Span, 1), message.QuickBatch([][]byte{[]byte(tt.message)}))
		require.Nil(t, response)
		require.Len(t, resMsgs, 1)
		if tt.expectedErr != nil {
			tmpErr := resMsgs[0].Get(0).ErrorGet()
			require.Error(t, tmpErr)
			require.Equal(t, mongo.ErrNoDocuments.Error(), tmpErr.Error())
			continue
		}

		jdopts := jsondiff.DefaultJSONOptions()
		diff, explanation := jsondiff.Compare(resMsgs[0].Get(0).AsBytes(), []byte(tt.expected), &jdopts)
		assert.Equalf(t, jsondiff.SupersetMatch.String(), diff.String(), "%s: %s", tt.name, explanation)
	}
}
