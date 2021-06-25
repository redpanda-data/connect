package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/x/service"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/mitchellh/mapstructure"
)

func init() {
	config := service.NewConfigSpec().
		Summary("Executes a PartiQL expression against a DynamoDB table for each message.").
		Description("Both writes or reads are supported, when the query is a read the contents of the message will be replaced with the result. This processor is more efficient when messages are pre-batched as the whole batch will be executed in a single call.").
		Categories("Integration").
		Version("3.48.0").
		Field(service.NewStringField("query").Description("A PartiQL query to execute for each message.")).
		Field(service.NewBoolField("unsafe_dynamic_query").Description("Whether to enable dynamic queries that support interpolation functions.").Advanced().Default(false)).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that, for each message, creates a list of arguments to use with the query.").Default("")).
		Example(
			"Insert",
			`The following example inserts rows into the table footable with the columns foo, bar and baz populated with values extracted from messages:`,
			`
pipeline:
  processors:
    - aws_dynamodb_partiql:
        query: "INSERT INTO footable VALUE {'foo':'?','bar':'?','baz':'?'}"
        args_mapping: |
          root = [
            { "S": this.foo },
            { "S": meta("kafka_topic") },
            { "S": this.document.content },
          ]
`,
		)

	for _, f := range sessionFields() {
		config = config.Field(f)
	}

	err := service.RegisterBatchProcessor(
		"aws_dynamodb_partiql", config,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			sess, err := getSession(conf)
			if err != nil {
				return nil, err
			}
			client := dynamodb.New(sess)
			query, err := conf.FieldString("query")
			if err != nil {
				return nil, err
			}
			args, err := conf.FieldBloblang("args_mapping")
			if err != nil {
				return nil, err
			}
			allowDynQuery, err := conf.FieldBool("unsafe_dynamic_query")
			if err != nil {
				return nil, err
			}
			var dynQuery *service.InterpolatedString
			if allowDynQuery {
				if dynQuery, err = service.NewInterpolatedString(query); err != nil {
					return nil, fmt.Errorf("failed to parse query: %v", err)
				}
			}
			return newDynamoDBPartiQL(mgr.Logger(), client, query, dynQuery, args), nil
		})
	if err != nil {
		panic(err)
	}
}

type dynamoDBPartiQL struct {
	logger *service.Logger
	client dynamodbiface.DynamoDBAPI

	query    string
	dynQuery *service.InterpolatedString
	args     *bloblang.Executor
}

func newDynamoDBPartiQL(
	logger *service.Logger,
	client dynamodbiface.DynamoDBAPI,
	query string,
	dynQuery *service.InterpolatedString,
	args *bloblang.Executor,
) *dynamoDBPartiQL {
	return &dynamoDBPartiQL{
		logger:   logger,
		client:   client,
		query:    query,
		dynQuery: dynQuery,
		args:     args,
	}
}

func cleanNulls(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, v := range t {
			if v == nil {
				delete(t, k)
			} else {
				cleanNulls(v)
			}
		}
	case []interface{}:
		for _, v := range t {
			cleanNulls(v)
		}
	}
}

func (d *dynamoDBPartiQL) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	outBatch := batch.Copy()

	stmts := []*dynamodb.BatchStatementRequest{}
	for i := range batch {
		req := &dynamodb.BatchStatementRequest{}
		req.Statement = &d.query
		if d.dynQuery != nil {
			query := batch.InterpolatedString(i, d.dynQuery)
			req.Statement = &query
		}

		argMsg, err := batch.BloblangQuery(i, d.args)
		if err != nil {
			return nil, fmt.Errorf("error evaluating arg mapping at index %d: %v", i, err)
		}

		argStructured, err := argMsg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("error evaluating arg mapping as structured at index %d: %v", i, err)
		}

		if err := mapstructure.Decode(argStructured, &req.Parameters); err != nil {
			return nil, fmt.Errorf("error converting structured message as dynamodb item at index %d: %v", i, err)
		}

		stmts = append(stmts, req)
	}

	batchResult, err := d.client.BatchExecuteStatementWithContext(ctx, &dynamodb.BatchExecuteStatementInput{
		Statements: stmts,
	})
	if err != nil {
		return nil, err
	}

	for i, res := range batchResult.Responses {
		if res.Error != nil {
			code := ""
			if res.Error.Code != nil {
				code = fmt.Sprintf(" (%v)", *res.Error.Code)
			}
			outBatch[i].SetError(fmt.Errorf("failed to process statement%v: %v", code, *res.Error.Message))
			continue
		}
		if res.Item != nil {
			itemBytes, err := json.Marshal(res.Item)
			if err != nil {
				outBatch[i].SetError(fmt.Errorf("failed to encode PartiQL result: %v", err))
				continue
			}
			var resMap interface{}
			if err := json.Unmarshal(itemBytes, &resMap); err != nil {
				outBatch[i].SetError(fmt.Errorf("failed to decode PartiQL result: %v", err))
				continue
			}
			cleanNulls(resMap)
			outBatch[i].SetStructured(resMap)
		}
	}

	return []service.MessageBatch{outBatch}, nil
}

func (d *dynamoDBPartiQL) Close(ctx context.Context) error {
	return nil
}
