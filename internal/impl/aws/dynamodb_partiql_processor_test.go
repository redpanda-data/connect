package aws

import (
	"context"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/x/service"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	pbatchFn func(context.Context, *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error)
}

func (m *mockDynamoDB) BatchExecuteStatementWithContext(ctx context.Context, input *dynamodb.BatchExecuteStatementInput, _ ...request.Option) (*dynamodb.BatchExecuteStatementOutput, error) {
	return m.pbatchFn(ctx, input)
}

func assertBatchMatches(t *testing.T, exp service.MessageBatch, act []service.MessageBatch) {
	t.Helper()

	require.Len(t, act, 1)
	require.Len(t, act[0], len(exp))
	for i, m := range exp {
		expBytes, _ := m.AsBytes()
		actBytes, _ := act[0][i].AsBytes()
		assert.Equal(t, string(expBytes), string(actBytes))
	}
}

func TestDynamoDBPartiqlWrite(t *testing.T) {
	query := `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
root."-".S = json("content")
`)
	require.NoError(t, err)

	var request []*dynamodb.BatchStatementRequest
	client := &mockDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}

	resBatch, err := db.ProcessBatch(context.Background(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	expected := []*dynamodb.BatchStatementRequest{
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []*dynamodb.AttributeValue{
				{S: aws.String("foo")},
				{S: aws.String("foo stuff")},
			},
		},
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []*dynamodb.AttributeValue{
				{S: aws.String("bar")},
				{S: aws.String("bar stuff")},
			},
		},
	}

	assert.Equal(t, expected, request)
}

func TestDynamoDBPartiqlRead(t *testing.T) {
	query := `SELECT * FROM Orders WHERE OrderID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	var request []*dynamodb.BatchStatementRequest
	client := &mockDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{
				Responses: []*dynamodb.BatchStatementResponse{
					{
						Item: map[string]*dynamodb.AttributeValue{
							"meow":  {S: aws.String("meow1")},
							"meow2": {S: aws.String("meow2")},
						},
					},
					{
						Item: map[string]*dynamodb.AttributeValue{
							"meow":  {S: aws.String("meow1")},
							"meow2": {S: aws.String("meow2")},
						},
					},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}
	expBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
	}

	resBatch, err := db.ProcessBatch(context.Background(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, expBatch, resBatch)

	errStr, _ := resBatch[0][0].MetaGet(processor.FailFlagKey)
	assert.Empty(t, errStr)

	errStr, _ = resBatch[0][1].MetaGet(processor.FailFlagKey)
	assert.Empty(t, errStr)

	expected := []*dynamodb.BatchStatementRequest{
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []*dynamodb.AttributeValue{
				{S: aws.String("foo")},
			},
		},
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []*dynamodb.AttributeValue{
				{S: aws.String("bar")},
			},
		},
	}

	assert.Equal(t, expected, request)
}

func TestDynamoDBPartiqlSadToGoodBatch(t *testing.T) {
	t.Parallel()

	query := `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
root."-".S = json("content")
`)
	require.NoError(t, err)

	var requests [][]*dynamodb.BatchStatementRequest
	client := &mockDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchExecuteStatementOutput{
					Responses: make([]*dynamodb.BatchStatementResponse, len(input.Statements)),
				}
				for i, stmt := range input.Statements {
					res := &dynamodb.BatchStatementResponse{}
					if *stmt.Parameters[0].S == "bar" {
						res.Error = &dynamodb.BatchStatementError{
							Message: aws.String("it all went wrong"),
						}
					}
					output.Responses[i] = res
				}
			} else {
				output = &dynamodb.BatchExecuteStatementOutput{}
			}
			stmts := make([]*dynamodb.BatchStatementRequest, len(input.Statements))
			copy(stmts, input.Statements)
			requests = append(requests, stmts)
			return
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}

	resBatch, err := db.ProcessBatch(context.Background(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	errStr, _ := resBatch[0][1].MetaGet(processor.FailFlagKey)
	assert.Contains(t, errStr, "it all went wrong")

	errStr, _ = resBatch[0][0].MetaGet(processor.FailFlagKey)
	assert.Empty(t, errStr)

	errStr, _ = resBatch[0][2].MetaGet(processor.FailFlagKey)
	assert.Empty(t, errStr)

	expected := [][]*dynamodb.BatchStatementRequest{
		{
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []*dynamodb.AttributeValue{
					{S: aws.String("foo")},
					{S: aws.String("foo stuff")},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []*dynamodb.AttributeValue{
					{S: aws.String("bar")},
					{S: aws.String("bar stuff")},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []*dynamodb.AttributeValue{
					{S: aws.String("baz")},
					{S: aws.String("baz stuff")},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}
