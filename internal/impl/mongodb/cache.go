package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/public/service"
)

const mongoDuplicateKeyErrCode = 11000

func mongodbCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("3.43.0").
		Summary(`Use a MongoDB instance as a cache.`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("database").
			Description("The name of the target MongoDB database.")).
		Field(service.NewStringField("collection").
			Description("The name of the target collection.")).
		Field(service.NewStringField("key_field").
			Description("The field in the document that is used as the key.")).
		Field(service.NewStringField("value_field").
			Description("The field in the document that is used as the value."))

	return spec
}

func init() {
	err := service.RegisterCache(
		"mongodb", mongodbCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newMongodbCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newMongodbCacheFromConfig(parsedConf *service.ParsedConfig) (*mongodbCache, error) {
	client, err := getClient(parsedConf)
	if err != nil {
		return nil, err
	}

	database, err := parsedConf.FieldString("database")
	if err != nil {
		return nil, err
	}

	collectionName, err := parsedConf.FieldString("collection")
	if err != nil {
		return nil, err
	}

	keyField, err := parsedConf.FieldString("key_field")
	if err != nil {
		return nil, err
	}

	valueField, err := parsedConf.FieldString("value_field")
	if err != nil {
		return nil, err
	}

	return newMongodbCache(database, collectionName, keyField, valueField, client)
}

//------------------------------------------------------------------------------

type mongodbCache struct {
	client     *mongo.Client
	collection *mongo.Collection

	keyField   string
	valueField string
}

func newMongodbCache(database, collectionName, keyField, valueField string, client *mongo.Client) (*mongodbCache, error) {
	if err := client.Connect(context.Background()); err != nil {
		return nil, err
	}
	return &mongodbCache{
		client:     client,
		collection: client.Database(database).Collection(collectionName),
		keyField:   keyField,
		valueField: valueField,
	}, nil
}

func (m *mongodbCache) Get(ctx context.Context, key string) ([]byte, error) {
	filter := bson.M{m.keyField: key}
	document, err := m.collection.FindOne(ctx, filter).DecodeBytes()
	if err != nil {
		return nil, service.ErrKeyNotFound
	}

	value, err := document.LookupErr(m.valueField)
	if err != nil {
		return nil, fmt.Errorf("error getting field from document %s: %v", m.valueField, err)
	}

	valueStr := value.StringValue()
	return []byte(valueStr), nil
}

func (m *mongodbCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	opts := options.Update().SetUpsert(true)
	filter := bson.M{m.keyField: key}
	update := bson.M{"$set": bson.M{m.valueField: string(value)}}

	_, err := m.collection.UpdateOne(ctx, filter, update, opts)
	return err
}

func (m *mongodbCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	document := bson.M{m.keyField: key, m.valueField: string(value)}
	_, err := m.collection.InsertOne(ctx, document)
	if err != nil {
		if errCode := getMongoErrorCode(err); errCode == mongoDuplicateKeyErrCode {
			err = service.ErrKeyAlreadyExists
		}
	}
	return err
}

func (m *mongodbCache) Delete(ctx context.Context, key string) error {
	filter := bson.M{m.keyField: key}
	_, err := m.collection.DeleteOne(ctx, filter)
	return err
}

func (m *mongodbCache) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func getMongoErrorCode(err error) int {
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
