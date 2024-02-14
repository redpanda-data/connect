package aws

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

type mockProcDynamoDB struct {
	dynamoDBAPI
	pbatchFn func(context.Context, *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error)
}

func (m *mockProcDynamoDB) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error) {
	return m.pbatchFn(ctx, params)
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

	var request []types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
	}

	resBatch, err := db.ProcessBatch(context.Background(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	expected := []types.BatchStatementRequest{
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "foo"},
				&types.AttributeValueMemberS{Value: "foo stuff"},
			},
		},
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "bar"},
				&types.AttributeValueMemberS{Value: "bar stuff"},
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

	var request []types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{
				Responses: []types.BatchStatementResponse{
					{
						Item: map[string]types.AttributeValue{
							"meow":  &types.AttributeValueMemberS{Value: "meow1"},
							"meow2": &types.AttributeValueMemberS{Value: "meow2"},
						},
					},
					{
						Item: map[string]types.AttributeValue{
							"meow":  &types.AttributeValueMemberS{Value: "meow1"},
							"meow2": &types.AttributeValueMemberS{Value: "meow2"},
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

	err = resBatch[0][0].GetError()
	assert.NoError(t, err)

	err = resBatch[0][1].GetError()
	assert.NoError(t, err)

	expected := []types.BatchStatementRequest{
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "foo"},
			},
		},
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "bar"},
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

	var requests [][]types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchExecuteStatementOutput{
					Responses: make([]types.BatchStatementResponse, len(input.Statements)),
				}
				for i, stmt := range input.Statements {
					res := types.BatchStatementResponse{}
					if stmt.Parameters[0].(*types.AttributeValueMemberS).Value == "bar" {
						res.Error = &types.BatchStatementError{
							Message: aws.String("it all went wrong"),
						}
					}
					output.Responses[i] = res
				}
			} else {
				output = &dynamodb.BatchExecuteStatementOutput{}
			}
			stmts := make([]types.BatchStatementRequest, len(input.Statements))
			copy(stmts, input.Statements)
			requests = append(requests, stmts)
			return
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
		service.NewMessage([]byte(`{"content":"baz stuff","id":"baz"}`)),
	}

	resBatch, err := db.ProcessBatch(context.Background(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	err = resBatch[0][1].GetError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "it all went wrong")

	err = resBatch[0][0].GetError()
	require.NoError(t, err)

	err = resBatch[0][2].GetError()
	require.NoError(t, err)

	expected := [][]types.BatchStatementRequest{
		{
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "foo"},
					&types.AttributeValueMemberS{Value: "foo stuff"},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "bar"},
					&types.AttributeValueMemberS{Value: "bar stuff"},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "baz"},
					&types.AttributeValueMemberS{Value: "baz stuff"},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}
