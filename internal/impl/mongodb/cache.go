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

package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const mongoDuplicateKeyErrCode = 11000

func mongodbCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("3.43.0").
		Summary(`Use a MongoDB instance as a cache.`).
		Fields(clientFields()...).
		Fields(
			service.NewStringField("collection").
				Description("The name of the target collection."),
			service.NewStringField("key_field").
				Description("The field in the document that is used as the key."),
			service.NewStringField("value_field").
				Description("The field in the document that is used as the value."),
		)
}

func init() {
	service.MustRegisterCache(
		"mongodb", mongodbCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newMongodbCacheFromConfig(conf)
		})
}

func newMongodbCacheFromConfig(parsedConf *service.ParsedConfig) (*mongodbCache, error) {
	client, database, err := getClient(parsedConf)
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

	return newMongodbCache(collectionName, keyField, valueField, client, database)
}

//------------------------------------------------------------------------------

type mongodbCache struct {
	client     *mongo.Client
	collection *mongo.Collection

	keyField   string
	valueField string
}

func newMongodbCache(collectionName, keyField, valueField string, client *mongo.Client, database *mongo.Database) (*mongodbCache, error) {
	return &mongodbCache{
		client:     client,
		collection: database.Collection(collectionName),
		keyField:   keyField,
		valueField: valueField,
	}, nil
}

func (m *mongodbCache) Get(ctx context.Context, key string) ([]byte, error) {
	filter := bson.M{m.keyField: key}
	document, err := m.collection.FindOne(ctx, filter).Raw()
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
	opts := options.UpdateOne().SetUpsert(true)
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
