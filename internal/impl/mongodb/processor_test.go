package mongodb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nsf/jsondiff"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/internal/impl/mongodb"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
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
		mongoClient, err = mongo.Connect(context.Background(), options.Client().
			SetConnectTimeout(10*time.Second).
			SetSocketTimeout(30*time.Second).
			SetServerSelectionTimeout(30*time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:"+resource.GetPort("27017/tcp")))
		if err != nil {
			return err
		}
		if err := mongoClient.Database("TestDB").CreateCollection(context.Background(), "TestCollection"); err != nil {
			_ = mongoClient.Disconnect(context.Background())
			return err
		}
		return nil
	}))

	port := resource.GetPort("27017/tcp")
	t.Run("insert", func(t *testing.T) {
		testMongoDBProcessorInsert(mongoClient, port, t)
	})
	t.Run("delete one", func(t *testing.T) {
		testMongoDBProcessorDeleteOne(mongoClient, port, t)
	})
	t.Run("delete many", func(t *testing.T) {
		testMongoDBProcessorDeleteMany(mongoClient, port, t)
	})
	t.Run("replace one", func(t *testing.T) {
		testMongoDBProcessorReplaceOne(mongoClient, port, t)
	})
	t.Run("update one", func(t *testing.T) {
		testMongoDBProcessorUpdateOne(mongoClient, port, t)
	})
	t.Run("find one", func(t *testing.T) {
		testMongoDBProcessorFindOne(mongoClient, port, t)
	})
	t.Run("upsert", func(t *testing.T) {
		testMongoDBProcessorUpsert(mongoClient, port, t)
	})
}

func testMProc(t testing.TB, port, collection, configYAML string) *mongodb.Processor {
	t.Helper()

	if collection == "" {
		collection = "TestCollection"
	}

	conf, err := mongodb.ProcessorSpec().ParseYAML(fmt.Sprintf(`
url: mongodb://localhost:%v
database: TestDB
collection: %v
username: mongoadmin
password: secret
`, port, collection)+configYAML, nil)
	require.NoError(t, err)

	proc, err := mongodb.ProcessorFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	return proc
}

func assertMessagesEqual(t testing.TB, batch service.MessageBatch, to []string) {
	t.Helper()
	require.Len(t, batch, len(to))
	for i, exp := range to {
		mBytes, err := batch[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, exp, string(mBytes))
	}
}

func testMongoDBProcessorInsert(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: ""
operation: "insert-one"
document_map: |
  root.a = this.foo
  root.b = this.bar
`)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo1","bar":"bar1"}`)),
		service.NewMessage([]byte(`{"foo":"foo2","bar":"bar2"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo1","bar":"bar1"}`,
		`{"foo":"foo2","bar":"bar2"}`,
	})

	// Validate the record is in the MongoDB
	result := collection.FindOne(tCtx, bson.M{"a": "foo1", "b": "bar1"})
	b, err := result.Raw()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	assert.Equal(t, `"foo1"`, aVal.String())
	assert.Equal(t, `"bar1"`, bVal.String())

	result = collection.FindOne(tCtx, bson.M{"a": "foo2", "b": "bar2"})
	b, err = result.Raw()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo2"`, aVal.String())
	assert.Equal(t, `"bar2"`, bVal.String())
}

func testMongoDBProcessorDeleteOne(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: 100s
operation: delete-one
filter_map: |
  root.a = this.foo
  root.b = this.bar
`)

	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err := collection.InsertOne(tCtx, bson.M{"a": "foo_delete", "b": "bar_delete"})
	assert.NoError(t, err)

	resMsgs, response := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo_delete","bar":"bar_delete"}`)),
	})
	require.NoError(t, response)
	require.Len(t, resMsgs, 1)
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo_delete","bar":"bar_delete"}`,
	})

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete", "b": "bar_delete"})
	b, err := result.Raw()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorDeleteMany(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: 100s
operation: delete-many
filter_map: |
  root.a = this.foo
  root.b = this.bar
`)

	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	_, err := collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c1"})
	assert.NoError(t, err)
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c2"})
	assert.NoError(t, err)

	resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo_delete_many","bar":"bar_delete_many"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)

	require.Len(t, resMsgs, 1)
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo_delete_many","bar":"bar_delete_many"}`,
	})

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many"})
	b, err := result.Raw()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorReplaceOne(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: ""
operation: replace-one
document_map: |
  root.a = this.foo
  root.b = this.bar
filter_map: |
  root.a = this.foo
`)

	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	_, err := collection.InsertOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_old", "c": "c1"})
	assert.NoError(t, err)

	resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo_replace","bar":"bar_new"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo_replace","bar":"bar_new"}`,
	})

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_new"})
	b, err := result.Raw()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_replace"`, aVal.String())
	assert.Equal(t, `"bar_new"`, bVal.String())
	assert.Equal(t, bson.RawValue{}, cVal)
}

func testMongoDBProcessorUpdateOne(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: 100s
operation: update-one
document_map: |
  root = { "$set": { "a": this.foo, "b": this.bar } }
filter_map: |
  root.a = this.foo
`)

	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	_, err := collection.InsertOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_old", "c": "c1"})
	assert.NoError(t, err)

	resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo_update","bar":"bar_update_new"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo_update","bar":"bar_update_new"}`,
	})

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_new"})
	b, err := result.Raw()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_update"`, aVal.String())
	assert.Equal(t, `"bar_update_new"`, bVal.String())
	assert.Equal(t, `"c1"`, cVal.String())
}

func testMongoDBProcessorUpsert(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	m := testMProc(t, port, "", `
write_concern:
  w: "1"
  j: false
  timeout: ""
operation: update-one
document_map: |
  root = { "$set": { "a": this.foo, "b": this.bar } }
filter_map: |
  root.a = this.foo
upsert: true
`)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err := collection.Indexes().CreateOne(tCtx, mongo.IndexModel{
		Keys: bson.M{
			"foo": -1,
		},
	})
	require.NoError(t, err)

	resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo1","bar":"bar1"}`)),
		service.NewMessage([]byte(`{"foo":"foo2","bar":"bar2"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)
	require.NoError(t, resMsgs[0][0].GetError())
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo1","bar":"bar1"}`,
		`{"foo":"foo2","bar":"bar2"}`,
	})

	// Validate the record is in the MongoDB
	result := collection.FindOne(tCtx, bson.M{"a": "foo1"})
	b, err := result.Raw()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	assert.Equal(t, `"foo1"`, aVal.String())
	assert.Equal(t, `"bar1"`, bVal.String())

	result = collection.FindOne(tCtx, bson.M{"a": "foo2"})
	b, err = result.Raw()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo2"`, aVal.String())
	assert.Equal(t, `"bar2"`, bVal.String())

	// Override
	resMsgs, err = m.ProcessBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"foo1","bar":"bar3"}`)),
		service.NewMessage([]byte(`{"foo":"foo2","bar":"bar4"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resMsgs, 1)
	require.NoError(t, resMsgs[0][0].GetError())
	assertMessagesEqual(t, resMsgs[0], []string{
		`{"foo":"foo1","bar":"bar3"}`,
		`{"foo":"foo2","bar":"bar4"}`,
	})

	// Validate the record is in the MongoDB
	result = collection.FindOne(tCtx, bson.M{"a": "foo1"})
	b, err = result.Raw()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo1"`, aVal.String())
	assert.Equal(t, `"bar3"`, bVal.String())

	result = collection.FindOne(tCtx, bson.M{"a": "foo2"})
	b, err = result.Raw()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo2"`, aVal.String())
	assert.Equal(t, `"bar4"`, bVal.String())
}

func testMongoDBProcessorFindOne(mongoClient *mongo.Client, port string, t *testing.T) {
	tCtx := context.Background()
	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	_, err := collection.InsertOne(context.Background(), bson.M{"a": "foo", "b": "bar", "c": "baz", "answer_to_everything": 42})
	assert.NoError(t, err)

	for _, tt := range []struct {
		name        string
		message     string
		marshalMode mongodb.JSONMarshalMode
		collection  string
		expected    string
		expectedErr error
	}{
		{
			name:        "canonical marshal mode",
			marshalMode: mongodb.JSONMarshalModeCanonical,
			message:     `{"a":"foo","x":"ignore_me_via_filter_map"}`,
			expected:    `{"a":"foo","b":"bar","c":"baz","answer_to_everything":{"$numberInt":"42"}}`,
		},
		{
			name:        "relaxed marshal mode",
			marshalMode: mongodb.JSONMarshalModeRelaxed,
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
			marshalMode: mongodb.JSONMarshalModeCanonical,
			collection:  `${!json("col")}`,
			message:     `{"col":"TestCollection","a":"foo"}`,
			expected:    `{"a":"foo","b":"bar","c":"baz","answer_to_everything":{"$numberInt":"42"}}`,
		},
	} {
		m := testMProc(t, port, tt.collection, fmt.Sprintf(`
write_concern:
  w: "1"
  j: false
  timeout: 100s
operation: find-one
filter_map: |
  root.a = this.a
json_marshal_mode: %v
`, tt.marshalMode))

		resMsgs, err := m.ProcessBatch(tCtx, service.MessageBatch{
			service.NewMessage([]byte(tt.message)),
		})
		require.NoError(t, err)
		require.Len(t, resMsgs, 1)

		if tt.expectedErr != nil {
			tmpErr := resMsgs[0][0].GetError()
			require.Error(t, tmpErr)
			require.Equal(t, mongo.ErrNoDocuments.Error(), tmpErr.Error())
			continue
		}

		mBytes, err := resMsgs[0][0].AsBytes()
		require.NoError(t, err)

		jdopts := jsondiff.DefaultJSONOptions()
		diff, explanation := jsondiff.Compare(mBytes, []byte(tt.expected), &jdopts)
		assert.Equalf(t, jsondiff.SupersetMatch.String(), diff.String(), "%s: %s", tt.name, explanation)
	}
}
