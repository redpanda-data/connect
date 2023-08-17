package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/benthosdev/benthos/v4/public/service"
)

// mongodb input component allowed operations.
const (
	FindInputOperation      = "find"
	AggregateInputOperation = "aggregate"
)

func mongoConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Version("3.64.0").
		Categories("Services").
		Summary("Executes a find query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
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
`))
}

func init() {
	err := service.RegisterInput(
		"mongodb", mongoConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newMongoInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newMongoInput(conf *service.ParsedConfig) (service.Input, error) {
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

	return service.AutoRetryNacks(&mongoInput{
		query:        query,
		collection:   collection,
		client:       mClient,
		database:     database,
		operation:    operation,
		marshalCanon: marshalMode == string(JSONMarshalModeCanonical),
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
}

func (m *mongoInput) Connect(ctx context.Context) error {
	if m.cursor != nil {
		return nil
	}

	if err := m.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	err := m.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	collection := m.database.Collection(m.collection)
	switch m.operation {
	case "find":
		m.cursor, err = collection.Find(ctx, m.query)
	case "aggregate":
		m.cursor, err = collection.Aggregate(ctx, m.query)
	default:
		return fmt.Errorf("opertaion %s not supported. the supported values are \"find\" and \"aggregate\"", m.operation)
	}
	if err != nil {
		_ = m.client.Disconnect(ctx)
		return err
	}
	return nil
}

func (m *mongoInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if m.cursor == nil {
		return nil, nil, service.ErrNotConnected
	}
	if !m.cursor.Next(ctx) {
		return nil, nil, service.ErrEndOfInput
	}
	var decoded any
	if err := m.cursor.Decode(&decoded); err != nil {
		return nil, nil, err
	}

	data, err := bson.MarshalExtJSON(decoded, m.marshalCanon, false)
	if err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetBytes(data)
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (m *mongoInput) Close(ctx context.Context) error {
	if m.cursor != nil && m.client != nil {
		return m.client.Disconnect(ctx)
	}
	return nil
}
