package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

func mongoConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Version("3.64.0").
		Categories("Services").
		Summary("Executes a find query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Field(urlField).
		Field(service.NewStringField("database").Description("The name of the target MongoDB database.")).
		Field(service.NewStringField("collection").Description("The collection to select from.")).
		Field(service.NewStringField("username").Description("The username to connect to the database.").Default("")).
		Field(service.NewStringField("password").Description("The password to connect to the database.").Default("")).
		Field(queryField)
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
	url, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	database, err := conf.FieldString("database")
	if err != nil {
		return nil, err
	}
	collection, err := conf.FieldString("collection")
	if err != nil {
		return nil, err
	}
	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := conf.FieldString("password")
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
	queryBSON, err := bson.Marshal(query)
	if err != nil {
		return nil, err
	}
	config := client.Config{
		URL:        url,
		Database:   database,
		Collection: collection,
		Username:   username,
		Password:   password,
	}
	return service.AutoRetryNacks(&mongoInput{
		queryBSON,
		config, nil, nil}), nil
}

type mongoInput struct {
	query  interface{}
	config client.Config
	client *mongo.Client
	cursor *mongo.Cursor
}

func (m *mongoInput) Connect(ctx context.Context) error {
	var err error
	m.client, err = m.config.Client()
	if err != nil {
		return err
	}
	if err = m.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err = m.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}
	collection := m.client.Database(m.config.Database).Collection(m.config.Collection)
	m.cursor, err = collection.Find(ctx, m.query)
	if err != nil {
		_ = m.client.Disconnect(ctx)
		return err
	}
	return nil
}

func (m *mongoInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if !m.cursor.Next(ctx) {
		return nil, nil, service.ErrEndOfInput
	}
	var result map[string]interface{}
	err := m.cursor.Decode(&result)
	if err != nil {
		return nil, nil, err
	}
	msg := service.NewMessage(nil)
	msg.SetStructured(result)
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (m *mongoInput) Close(ctx context.Context) error {
	if m.client != nil {
		return m.client.Disconnect(ctx)
	}
	return nil
}
