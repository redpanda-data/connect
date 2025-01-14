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
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	mpFieldCollection      = "collection"
	mpFieldJSONMarshalMode = "json_marshal_mode"
)

// ProcessorSpec defines the config spec of the mongodb processor.
func ProcessorSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("3.43.0").
		Categories("Services").
		Summary("Performs operations against MongoDB for each message, allowing you to store or retrieve data within message payloads.").
		Description("").
		Fields(clientFields()...).
		Fields(
			service.NewStringField(mpFieldCollection).
				Description("The name of the target collection."),
			processorOperationDocs(OperationInsertOne),
			writeConcernDocs(),
		).
		Fields(writeMapsFields()...).
		Field(service.NewStringAnnotatedEnumField(mpFieldJSONMarshalMode, map[string]string{
			string(JSONMarshalModeCanonical): "A string format that emphasizes type preservation at the expense of readability and interoperability. That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
			string(JSONMarshalModeRelaxed):   "A string format that emphasizes readability and interoperability at the expense of type preservation. That is, conversion from relaxed format to BSON can lose type information.",
		}).
			Description("The json_marshal_mode setting is optional and controls the format of the output message.").
			Advanced().
			Version("3.60.0").
			Default(string(JSONMarshalModeCanonical)))
	for _, f := range retries.CommonRetryBackOffFields(3, "1s", "5s", "30s") {
		spec = spec.Field(f.Deprecated())
	}
	return spec
}

func init() {
	err := service.RegisterBatchProcessor(
		"mongodb", ProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (proc service.BatchProcessor, err error) {
			proc, err = ProcessorFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Processor encapsulates the logic of the mongodb processor.
type Processor struct {
	log *service.Logger

	client           *mongo.Client
	database         *mongo.Database
	collection       *service.InterpolatedString
	writeConcernSpec *writeConcernSpec
	operation        Operation
	writeMaps        writeMaps

	marshalMode JSONMarshalMode
}

// ProcessorFromParsed returns a mongodb processor from a parsed config.
func ProcessorFromParsed(conf *service.ParsedConfig, res *service.Resources) (mp *Processor, err error) {
	mp = &Processor{
		log: res.Logger(),
	}
	if mp.client, mp.database, err = getClient(conf); err != nil {
		return
	}
	if mp.collection, err = conf.FieldInterpolatedString(mpFieldCollection); err != nil {
		return
	}
	if mp.writeConcernSpec, err = writeConcernSpecFromParsed(conf); err != nil {
		return
	}
	if mp.operation, err = operationFromParsed(conf); err != nil {
		return
	}
	if mp.writeMaps, err = writeMapsFromParsed(conf, mp.operation); err != nil {
		return
	}
	var marshalModeStr string
	if marshalModeStr, err = conf.FieldString(mpFieldJSONMarshalMode); err != nil {
		return
	}
	mp.marshalMode = JSONMarshalMode(marshalModeStr)

	if err = mp.client.Ping(context.Background(), nil); err != nil {
		_ = mp.client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping failed: %v", err)
	}
	return
}

type msgsAndModels struct {
	msgs []*service.Message
	ws   []mongo.WriteModel
}

// ProcessBatch attempts to process a batch of messages.
func (m *Processor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	writeModelsMap := map[string]msgsAndModels{}

	wmExec := m.writeMaps.exec(batch)

	_ = batch.WalkWithBatchedErrors(func(i int, msg *service.Message) (err error) {
		defer func() {
			if err != nil {
				msg.SetError(err)
			}
		}()

		docJSON, filterJSON, hintJSON, err := wmExec.extractFromMessage(m.operation, i)
		if err != nil {
			return err
		}

		findOptions := options.FindOne()
		if hintJSON != nil {
			findOptions.SetHint(hintJSON)
		}

		collectionStr, err := batch.TryInterpolatedString(i, m.collection)
		if err != nil {
			return fmt.Errorf("collection interpolation error: %w", err)
		}

		var writeModel mongo.WriteModel
		switch m.operation {
		case OperationInsertOne:
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case OperationDeleteOne:
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case OperationDeleteMany:
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case OperationReplaceOne:
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &m.writeMaps.upsert,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case OperationUpdateOne:
			writeModel = &mongo.UpdateOneModel{
				Upsert: &m.writeMaps.upsert,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		case OperationFindOne:
			collection := m.database.Collection(collectionStr, m.writeConcernSpec.options)

			var decoded any
			if err = collection.FindOne(ctx, filterJSON, findOptions).Decode(&decoded); err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return err
				}
				m.log.Errorf("Error decoding mongo db result, filter = %v: %s", filterJSON, err)
				return err
			}

			data, err := bson.MarshalExtJSON(decoded, m.marshalMode == JSONMarshalModeCanonical, false)
			if err != nil {
				return err
			}

			msg.SetBytes(data)
			return nil
		}

		if writeModel != nil {
			tmp := writeModelsMap[collectionStr]
			tmp.ws = append(tmp.ws, writeModel)
			tmp.msgs = append(tmp.msgs, msg)
			writeModelsMap[collectionStr] = tmp
		}
		return nil
	})

	if len(writeModelsMap) > 0 {
		for collectionStr, msAndMs := range writeModelsMap {
			m.bulkWrite(ctx, collectionStr, &msAndMs)
		}
	}

	return []service.MessageBatch{batch}, nil
}

func (m *Processor) bulkWrite(ctx context.Context, collectionStr string, msgsAndModels *msgsAndModels) {
	if m.writeConcernSpec.wTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, m.writeConcernSpec.wTimeout)
		defer cancel()
	}

	collection := m.database.Collection(collectionStr, m.writeConcernSpec.options)

	// We should have at least one write model in the slice
	if _, err := collection.BulkWrite(ctx, msgsAndModels.ws); err != nil {
		m.log.Errorf("Bulk write failed in mongodb processor: %v", err)
		for _, msg := range msgsAndModels.msgs {
			msg.SetError(err)
		}
	}
}

// Close the connection to mongodb.
func (m *Processor) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
