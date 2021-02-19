package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	bmongo "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const mongoDuplicateKeyErrCode = 11000

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMongoDB] = TypeSpec{
		constructor:       NewMongoDB,
		SupportsPerKeyTTL: false,
		Summary: `
Use a MongoDB instance as a cache.`,
		FieldSpecs: bmongo.ConfigDocs().Add(
			docs.FieldCommon("key_field", "The field in the document that is used as the key."),
			docs.FieldCommon("value_field", "The field in the document that his used as the value."),
		),
	}
}

//------------------------------------------------------------------------------

// MongoDBConfig is a config struct for a mongo connection.
type MongoDBConfig struct {
	bmongo.Config `json:",inline" yaml:",inline"`
	KeyField      string `json:"key_field" yaml:"key_field"`
	ValueField    string `json:"value_field" yaml:"value_field"`
}

// NewMongoDBConfig returns a MongoDBConfig with default values.
func NewMongoDBConfig() MongoDBConfig {
	return MongoDBConfig{
		Config:     bmongo.NewConfig(),
		KeyField:   "",
		ValueField: "",
	}
}

//------------------------------------------------------------------------------

// MongoDB is a cache that connects to mongo databases.
type MongoDB struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	client     *mongo.Client
	collection *mongo.Collection
}

// NewMongoDB returns a MongoDB cache.
func NewMongoDB(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (types.Cache, error) {
	if conf.MongoDB.URL == "" {
		return nil, errors.New("mongodb url must be specified")
	}

	if conf.MongoDB.Database == "" {
		return nil, errors.New("mongodb database must be specified")
	}

	if conf.MongoDB.Collection == "" {
		return nil, errors.New("mongodb collection must be specified")
	}

	if conf.MongoDB.KeyField == "" {
		return nil, errors.New("mongodb key_field must be specified")
	}

	if conf.MongoDB.ValueField == "" {
		return nil, errors.New("mongodb value_field must be specified")
	}

	client, err := conf.MongoDB.Client()
	if err != nil {
		return nil, err
	}

	err = client.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	collection := client.Database(conf.MongoDB.Database).Collection(conf.MongoDB.Collection)

	return &MongoDB{
		conf:  conf,
		log:   log,
		stats: stats,

		client:     client,
		collection: collection,
	}, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist or if the operation failed.
func (m *MongoDB) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.MongoDB.KeyField: key}
	document, err := m.collection.FindOne(ctx, filter).DecodeBytes()

	if err != nil {
		m.log.Debugf("key not found: %s", key)
		return nil, types.ErrKeyNotFound
	}

	value, err := document.LookupErr(m.conf.MongoDB.ValueField)

	if err != nil {
		return nil, fmt.Errorf("error getting field from document %s: %v", m.conf.MongoDB.ValueField, err)
	}

	valueStr := value.StringValue()

	return []byte(valueStr), nil
}

// Set attempts to set the value of a key.
func (m *MongoDB) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.MongoDB.KeyField: key}

	update := bson.M{"$set": bson.M{m.conf.MongoDB.ValueField: string(value)}}

	_, err := m.collection.UpdateOne(ctx, filter, update)

	if err != nil {
		return fmt.Errorf("error setting value for document with key %s: %v", key, err)
	}

	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (m *MongoDB) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := m.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (m *MongoDB) Add(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	document := bson.M{m.conf.MongoDB.KeyField: key, m.conf.MongoDB.ValueField: string(value)}

	_, err := m.collection.InsertOne(ctx, document)

	if err != nil {
		errCode := m.getMongoErrorCode(err)

		if errCode == mongoDuplicateKeyErrCode {
			return types.ErrKeyAlreadyExists
		}
	}

	return err
}

// Delete attempts to remove a key.
func (m *MongoDB) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.MongoDB.KeyField: key}
	_, err := m.collection.DeleteOne(ctx, filter)

	return err
}

// CloseAsync shuts down the cache.
func (m *MongoDB) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (m *MongoDB) WaitForClose(timeout time.Duration) error {
	m.client.Disconnect(context.Background())
	return nil
}

func (m *MongoDB) getMongoErrorCode(err error) int {
	var errorCode int

	switch e := err.(type) {
	default:
		errorCode = 0
	case mongo.WriteException:
		errorCode = e.WriteErrors[0].Code
	case mongo.CommandError:
		errorCode = int(e.Code)
	}

	return errorCode
}

//-----------------------------------------------------------------------------
