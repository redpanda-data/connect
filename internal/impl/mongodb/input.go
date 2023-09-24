package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/public/service"
)

// mongodb input component allowed operations.
const (
	FindInputOperation      = "find"
	AggregateInputOperation = "aggregate"
	DefaultBatchSize        = 1000
)

func mongoConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Version("3.64.0").
		Categories("Services").
		Summary("Executes a query and creates a message for each document received.").
		Description(`Once the documents from the query are exhausted, this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Fields(clientFields()...).
		Field(service.NewStringField("collection").Description("The collection to select from.")).
		Field(service.NewStringEnumField("operation", FindInputOperation, AggregateInputOperation).
			Description("The mongodb operation to perform.").
			Default(FindInputOperation).Advanced().
			Version("4.2.0")).
		Field(service.NewStringAnnotatedEnumField("json_marshal_mode", map[string]string{
			string(JSONMarshalModeCanonical): "A string format that emphasizes type preservation at the expense of readability and interoperability. " +
				"That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
			string(JSONMarshalModeRelaxed): "A string format that emphasizes readability and interoperability at the expense of type preservation." +
				"That is, conversion from relaxed format to BSON can lose type information.",
		}).
			Description("The json_marshal_mode setting is optional and controls the format of the output message.").
			Default(string(JSONMarshalModeCanonical)).
			Advanced().
			Version("4.7.0")).
		Field(service.NewBloblangField("query").
			Description("Bloblang expression describing MongoDB query.").
			Example(`
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`)).
		Field(service.NewIntField("batchSize").
			Description("A number of documents at which the batch should be flushed. Greater than `0`. Operations: `find`, `aggregate`").
			Optional().
			Default(1000).
			Version("4.22.0")).
		Field(service.NewIntMapField("sort").
			Description("An object specifying fields to sort by, and the respective sort order (`1` ascending, `-1` descending). Operations: `find`").
			Optional().
			Example(`
name: 1
age: -1
`).
			Version("4.22.0")).
		Field(service.NewIntField("limit").
			Description("A number of documents to return. Operations: `find`").
			Optional().
			Version("4.22.0"))
}

func init() {
	err := service.RegisterBatchInput(
		"mongodb", mongoConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newMongoInput(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

func newMongoInput(conf *service.ParsedConfig, logger *service.Logger) (service.BatchInput, error) {
	var (
		batchSize, limit int
		sort             map[string]int
	)

	mClient, database, err := getClient(conf)
	if err != nil {
		return nil, err
	}
	collection, err := conf.FieldString("collection")
	if err != nil {
		return nil, err
	}
	operation, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	marshalMode, err := conf.FieldString("json_marshal_mode")
	if err != nil {
		return nil, err
	}
	queryExecutor, err := conf.FieldBloblang("query")
	if err != nil {
		return nil, err
	}
	query, err := queryExecutor.Query(struct{}{})
	if err != nil {
		return nil, err
	}
	if conf.Contains("batchSize") {
		batchSize, err = conf.FieldInt("batchSize")
		if err != nil {
			return nil, err
		}
	}
	if conf.Contains("sort") {
		sort, err = conf.FieldIntMap("sort")
		if err != nil {
			return nil, err
		}
	}
	if conf.Contains("limit") {
		limit, err = conf.FieldInt("limit")
		if err != nil {
			return nil, err
		}
	}
	return service.AutoRetryNacksBatched(&mongoInput{
		query:        query,
		collection:   collection,
		client:       mClient,
		database:     database,
		operation:    operation,
		marshalCanon: marshalMode == string(JSONMarshalModeCanonical),
		batchSize:    int32(batchSize),
		sort:         sort,
		limit:        int64(limit),
		count:        0,
		logger:       logger,
	}), nil
}

type mongoInput struct {
	query        any
	collection   string
	client       *mongo.Client
	database     *mongo.Database
	cursor       *mongo.Cursor
	operation    string
	marshalCanon bool
	batchSize    int32
	sort         map[string]int
	limit        int64
	count        int
	logger       *service.Logger
}

func (m *mongoInput) Connect(ctx context.Context) error {
	if m.cursor != nil {
		return nil
	}

	err := m.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	collection := m.database.Collection(m.collection)
	switch m.operation {
	case "find":
		var findOptions *options.FindOptions
		findOptions, err = m.getFindOptions()
		if err != nil {
			return fmt.Errorf("error parsing 'find' options: %v", err)
		}
		m.cursor, err = collection.Find(ctx, m.query, findOptions)
	case "aggregate":
		var aggregateOptions *options.AggregateOptions
		aggregateOptions, err = m.getAggregateOptions()
		if err != nil {
			return fmt.Errorf("error parsing 'aggregate' options: %v", err)
		}
		m.cursor, err = collection.Aggregate(ctx, m.query, aggregateOptions)
	default:
		return fmt.Errorf("operation '%s' not supported. the supported values are 'find' and 'aggregate'", m.operation)
	}
	if err != nil {
		_ = m.client.Disconnect(ctx)
		return err
	}
	return nil
}

func (m *mongoInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	i := 0
	batch := make(service.MessageBatch, m.batchSize)

	if m.cursor == nil {
		return nil, nil, service.ErrNotConnected
	}

	for m.cursor.Next(ctx) {
		msg := service.NewMessage(nil)
		msg.MetaSet("mongo_database", m.database.Name())
		msg.MetaSet("mongo_collection", m.collection)

		var decoded any
		if err := m.cursor.Decode(&decoded); err != nil {
			msg.SetError(err)
		} else {
			data, err := bson.MarshalExtJSON(decoded, m.marshalCanon, false)
			if err != nil {
				msg.SetError(err)
			}
			msg.SetBytes(data)
		}
		batch[i] = msg
		i++
		m.count++

		if m.cursor.RemainingBatchLength() == 0 {
			return batch[:i], func(ctx context.Context, err error) error {
				return nil
			}, nil
		}
	}
	return nil, nil, service.ErrEndOfInput
}

func (m *mongoInput) Close(ctx context.Context) error {
	if m.cursor != nil && m.client != nil {
		m.logger.Debugf("Got %d documents from '%s' collection", m.count, m.collection)
		return m.client.Disconnect(ctx)
	}
	return nil
}

func (m *mongoInput) getFindOptions() (*options.FindOptions, error) {
	findOptions := options.Find()
	if m.batchSize > 0 {
		findOptions.SetBatchSize(m.batchSize)
	}
	if m.sort != nil {
		findOptions.SetSort(m.sort)
	}
	if m.limit > 0 {
		findOptions.SetLimit(m.limit)
	}
	return findOptions, nil
}

func (m *mongoInput) getAggregateOptions() (*options.AggregateOptions, error) {
	aggregateOptions := options.Aggregate()
	if m.batchSize > 0 {
		aggregateOptions.SetBatchSize(m.batchSize)
	}
	return aggregateOptions, nil
}
