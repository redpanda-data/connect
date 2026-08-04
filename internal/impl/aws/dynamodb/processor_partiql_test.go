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

package dynamodb

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockProcDynamoDB struct {
	dynamoDBAPI
	pbatchFn func(context.Context, *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error)
	pexecFn  func(context.Context, *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error)
}

func (m *mockProcDynamoDB) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error) {
	return m.pbatchFn(ctx, params)
}

func (m *mockProcDynamoDB) ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, _ ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error) {
	return m.pexecFn(ctx, params)
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

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, true)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
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

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, true)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}
	expBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
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

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, true)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
		service.NewMessage([]byte(`{"content":"baz stuff","id":"baz"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
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

func TestDynamoDBPartiqlExecuteStatementRead(t *testing.T) {
	query := `SELECT * FROM "Orders"."gsi_index" WHERE OrderID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	var requests []*dynamodb.ExecuteStatementInput
	client := &mockProcDynamoDB{
		pexecFn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			requests = append(requests, input)
			return &dynamodb.ExecuteStatementOutput{
				Items: []map[string]types.AttributeValue{
					{
						"meow":  &types.AttributeValueMemberS{Value: "meow1"},
						"meow2": &types.AttributeValueMemberS{Value: "meow2"},
					},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}
	expBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, expBatch, resBatch)

	require.Len(t, requests, 2)
	assert.Equal(t, aws.String(query), requests[0].Statement)
	assert.Equal(t, []types.AttributeValue{&types.AttributeValueMemberS{Value: "foo"}}, requests[0].Parameters)
	assert.Equal(t, aws.String(query), requests[1].Statement)
	assert.Equal(t, []types.AttributeValue{&types.AttributeValueMemberS{Value: "bar"}}, requests[1].Parameters)
}

func TestDynamoDBPartiqlExecuteStatementWrite(t *testing.T) {
	query := `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
root."-".S = json("content")
`)
	require.NoError(t, err)

	var requests []*dynamodb.ExecuteStatementInput
	client := &mockProcDynamoDB{
		pexecFn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			requests = append(requests, input)
			return &dynamodb.ExecuteStatementOutput{}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	require.Len(t, requests, 2)
	assert.Equal(t, []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "foo"},
		&types.AttributeValueMemberS{Value: "foo stuff"},
	}, requests[0].Parameters)
	assert.Equal(t, []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "bar"},
		&types.AttributeValueMemberS{Value: "bar stuff"},
	}, requests[1].Parameters)
}

func TestDynamoDBPartiqlExecuteStatementSadToGood(t *testing.T) {
	query := `SELECT * FROM "Orders"."gsi_index" WHERE OrderID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	client := &mockProcDynamoDB{
		pexecFn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			if input.Parameters[0].(*types.AttributeValueMemberS).Value == "bar" {
				return nil, errors.New("it all went wrong")
			}
			return &dynamodb.ExecuteStatementOutput{
				Items: []map[string]types.AttributeValue{
					{"meow": &types.AttributeValueMemberS{Value: "meow1"}},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo"}`)),
		service.NewMessage([]byte(`{"id":"bar"}`)),
		service.NewMessage([]byte(`{"id":"baz"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	require.Len(t, resBatch, 1)
	require.Len(t, resBatch[0], 3)

	assert.NoError(t, resBatch[0][0].GetError())
	foo, _ := resBatch[0][0].AsBytes()
	assert.Equal(t, `{"meow":{"S":"meow1"}}`, string(foo))

	err = resBatch[0][1].GetError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "it all went wrong")

	assert.NoError(t, resBatch[0][2].GetError())
	baz, _ := resBatch[0][2].AsBytes()
	assert.Equal(t, `{"meow":{"S":"meow1"}}`, string(baz))
}
