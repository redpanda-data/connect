package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const mongoDuplicateKeyErrCode = 11000

func init() {
	bundle.AllCaches.Add(func(c cache.Config, nm bundle.NewManagement) (types.Cache, error) {
		return NewCache(c, nm, nm.Logger(), nm.Metrics())
	}, docs.ComponentSpec{
		Name:    cache.TypeMongoDB,
		Type:    docs.TypeCache,
		Status:  docs.StatusExperimental,
		Summary: `Use a MongoDB instance as a cache.`,
		// TODO: Document that it doesn't support per item TTLs
		Config: docs.FieldComponent().WithChildren(
			client.ConfigDocs().Add(
				docs.FieldCommon("key_field", "The field in the document that is used as the key."),
				docs.FieldCommon("value_field", "The field in the document that is used as the value."),
			)...,
		),
	})
}

//------------------------------------------------------------------------------

// Cache is a cache that connects to mongo databases.
type Cache struct {
	conf  cache.MongoDBConfig
	log   log.Modular
	stats metrics.Type

	client     *mongo.Client
	collection *mongo.Collection
}

// NewCache returns a MongoDB cache.
func NewCache(
	conf cache.Config, mgr types.Manager, log log.Modular, stats metrics.Type,
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

	return &Cache{
		conf:  conf.MongoDB,
		log:   log,
		stats: stats,

		client:     client,
		collection: collection,
	}, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist or if the operation failed.
func (m *Cache) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.KeyField: key}
	document, err := m.collection.FindOne(ctx, filter).DecodeBytes()
	if err != nil {
		m.log.Debugf("key not found: %s", key)
		return nil, types.ErrKeyNotFound
	}

	value, err := document.LookupErr(m.conf.ValueField)
	if err != nil {
		return nil, fmt.Errorf("error getting field from document %s: %v", m.conf.ValueField, err)
	}

	valueStr := value.StringValue()

	return []byte(valueStr), nil
}

// Set attempts to set the value of a key.
func (m *Cache) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.KeyField: key}
	update := bson.M{"$set": bson.M{m.conf.ValueField: string(value)}}

	_, err := m.collection.UpdateOne(ctx, filter, update)
	return err
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (m *Cache) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := m.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (m *Cache) Add(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	document := bson.M{m.conf.KeyField: key, m.conf.ValueField: string(value)}
	_, err := m.collection.InsertOne(ctx, document)
	if err != nil {
		if errCode := m.getMongoErrorCode(err); errCode == mongoDuplicateKeyErrCode {
			err = types.ErrKeyAlreadyExists
		}
	}
	return err
}

// Delete attempts to remove a key.
func (m *Cache) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{m.conf.KeyField: key}
	_, err := m.collection.DeleteOne(ctx, filter)
	return err
}

// CloseAsync shuts down the cache.
func (m *Cache) CloseAsync() {
	m.client.Disconnect(context.Background())
}

// WaitForClose blocks until the cache has closed down.
func (m *Cache) WaitForClose(timeout time.Duration) error {
	return nil
}

func (m *Cache) getMongoErrorCode(err error) int {
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
