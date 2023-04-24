package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

// mongodb input component allowed operations.
const (
	FindInputOperation         = "find"
	AggregateInputOperation    = "aggregate"
	ChangeStreamInputOperation = "changestream"
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
		Field(service.NewStringEnumField("operation", FindInputOperation, AggregateInputOperation, ChangeStreamInputOperation).
			Description("The mongodb operation to perform.").
			Default(FindInputOperation).Advanced().
			Version("4.2.0")).
		Field(service.NewStringField("resume_token_file").Description("State directory to restore/save resume token").Optional()).
		Field(service.NewStringAnnotatedEnumField("json_marshal_mode", map[string]string{
			string(client.JSONMarshalModeCanonical): "A string format that emphasizes type preservation at the expense of readability and interoperability. " +
				"That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
			string(client.JSONMarshalModeRelaxed): "A string format that emphasizes readability and interoperability at the expense of type preservation." +
				"That is, conversion from relaxed format to BSON can lose type information.",
		}).
			Description("The json_marshal_mode setting is optional and controls the format of the output message.").
			Default(string(client.JSONMarshalModeCanonical)).
			Advanced().
			Version("4.7.0")).
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
	operation, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	marshalMode, err := conf.FieldString("json_marshal_mode")
	if err != nil {
		return nil, err
	}
	var resumeTokenPath *string = nil
	tokenFile, err := conf.FieldString("resume_token_file")
	if nil == err {
		resumeTokenPath = &tokenFile
	}
	config := client.Config{
		URL:        url,
		Database:   database,
		Collection: collection,
		Username:   username,
		Password:   password,
	}
	var query any
	if operation != ChangeStreamInputOperation {
		queryExecutor, err := conf.FieldBloblang("query")
		if err != nil {
			return nil, err
		}
		query, err = queryExecutor.Query(struct{}{})
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacks(&mongoInput{
			query:        query,
			config:       config,
			operation:    operation,
			marshalCanon: marshalMode == string(client.JSONMarshalModeCanonical),
		}), nil
	} else {
		var pipeline mongo.Pipeline
		return &mongoInput{
			query:           pipeline,
			config:          config,
			operation:       operation,
			resumeTokenPath: resumeTokenPath,
			marshalCanon:    marshalMode == string(client.JSONMarshalModeCanonical),
		}, nil
	}
}

type mongoInput struct {
	query           any
	config          client.Config
	client          *mongo.Client
	cursor          *mongo.Cursor
	changeStream    *mongo.ChangeStream
	resumeTokenPath *string
	operation       string
	marshalCanon    bool
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
	switch m.operation {
	case FindInputOperation:
		m.cursor, err = collection.Find(ctx, m.query)
	case AggregateInputOperation:
		m.cursor, err = collection.Aggregate(ctx, m.query)
	case ChangeStreamInputOperation:
		var resumeAfter bson.Raw = nil
		if nil != m.resumeTokenPath {
			if _, err := os.Stat(*m.resumeTokenPath); err == nil {
				c, err := ioutil.ReadFile(*m.resumeTokenPath)
				if nil == err {
					reader := bytes.NewReader(c)
					resumeAfter, err = bson.NewFromIOReader(reader)
					if nil != err {
						resumeAfter = nil
					}
				}
			}
		}
		if nil != resumeAfter {
			m.changeStream, err = collection.Watch(ctx, m.query, &options.ChangeStreamOptions{
				ResumeAfter: resumeAfter,
			})
		} else {
			m.changeStream, err = collection.Watch(ctx, m.query)
		}
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
	var decoded any
	if nil != m.changeStream {
		if m.changeStream.Next(ctx) {
			if err := m.changeStream.Decode(&decoded); err != nil {
				return nil, nil, err
			}
			if nil != m.resumeTokenPath {
				resumeToken := m.changeStream.ResumeToken()
				ioutil.WriteFile(*m.resumeTokenPath, resumeToken, fs.ModePerm)
			}
		}
	} else {
		if !m.cursor.Next(ctx) {
			return nil, nil, service.ErrEndOfInput
		}
		if err := m.cursor.Decode(&decoded); err != nil {
			return nil, nil, err
		}
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
	if m.client != nil {
		return m.client.Disconnect(ctx)
	}
	return nil
}
