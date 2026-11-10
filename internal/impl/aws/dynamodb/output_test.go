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

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockDynamoDB struct {
	dynamoDBAPI
	fn      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batchFn func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

func (m *mockDynamoDB) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.fn(params)
}

func (m *mockDynamoDB) BatchWriteItem(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return m.batchFn(params)
}

func testDDBOWriter(t *testing.T, conf string) *dynamoDBWriter {
	t.Helper()

	pConf, err := ddboOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	dConf, err := ddboConfigFromParsed(pConf)
	require.NoError(t, err)

	w, err := newDynamoDBWriter(dConf, service.MockResources())
	require.NoError(t, err)

	return w
}

func TestDynamoDBHappy(t *testing.T) {
	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
  content: ${!json("content")}
`)

	var request map[string][]types.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			request = input.RequestItems
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	require.NoError(t, db.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}))

	expected := map[string][]types.WriteRequest{
		"FooTable": {
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{
							Value: "foo",
						},
						"content": &types.AttributeValueMemberS{
							Value: "foo stuff",
						},
					},
				},
			},
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{
							Value: "bar",
						},
						"content": &types.AttributeValueMemberS{
							Value: "bar stuff",
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, request)
}

func TestDynamoDBSadToGood(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
  content: ${!json("content")}
backoff:
  max_elapsed_time: 100ms
`)

	var batchRequest []types.WriteRequest
	var requests []*dynamodb.PutItemInput

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			requests = append(requests, input)
			return nil, nil
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			if len(batchRequest) > 0 {
				t.Error("not expected")
				return nil, errors.New("not implemented")
			}
			if request, ok := input.RequestItems["FooTable"]; ok {
				items := make([]types.WriteRequest, len(request))
				copy(items, request)
				batchRequest = items
			} else {
				t.Error("missing FooTable")
			}
			return &dynamodb.BatchWriteItemOutput{}, errors.New("woop")
		},
	}

	require.NoError(t, db.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}))

	batchExpected := []types.WriteRequest{
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "foo"},
					"content": &types.AttributeValueMemberS{Value: "foo stuff"},
				},
			},
		},
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "bar"},
					"content": &types.AttributeValueMemberS{Value: "bar stuff"},
				},
			},
		},
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "baz"},
					"content": &types.AttributeValueMemberS{Value: "baz stuff"},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.PutItemInput{
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "foo"},
				"content": &types.AttributeValueMemberS{Value: "foo stuff"},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "bar"},
				"content": &types.AttributeValueMemberS{Value: "bar stuff"},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "baz"},
				"content": &types.AttributeValueMemberS{Value: "baz stuff"},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBSadToGoodBatch(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
  content: ${!json("content")}
`)

	var requests [][]types.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (output *dynamodb.BatchWriteItemOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]types.WriteRequest{
						"FooTable": {
							{
								PutRequest: &types.PutRequest{
									Item: map[string]types.AttributeValue{
										"id":      &types.AttributeValueMemberS{Value: "bar"},
										"content": &types.AttributeValueMemberS{Value: "bar stuff"},
									},
								},
							},
						},
					},
				}
			} else {
				output = &dynamodb.BatchWriteItemOutput{}
			}
			if request, ok := input.RequestItems["FooTable"]; ok {
				items := make([]types.WriteRequest, len(request))
				copy(items, request)
				requests = append(requests, items)
			} else {
				t.Error("missing FooTable")
			}
			return
		},
	}

	require.NoError(t, db.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}))

	expected := [][]types.WriteRequest{
		{
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "foo"},
						"content": &types.AttributeValueMemberS{Value: "foo stuff"},
					},
				},
			},
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "bar"},
						"content": &types.AttributeValueMemberS{Value: "bar stuff"},
					},
				},
			},
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "baz"},
						"content": &types.AttributeValueMemberS{Value: "baz stuff"},
					},
				},
			},
		},
		{
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "bar"},
						"content": &types.AttributeValueMemberS{Value: "bar stuff"},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBSad(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
  content: ${!json("content")}
`)

	var batchRequest []types.WriteRequest
	var requests []*dynamodb.PutItemInput

	barErr := errors.New("dont like bar")

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			if len(requests) < 3 {
				requests = append(requests, input)
			}
			if input.Item["id"].(*types.AttributeValueMemberS).Value == "bar" {
				return nil, barErr
			}
			return nil, nil
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			if len(batchRequest) > 0 {
				t.Error("not expected")
				return nil, errors.New("not implemented")
			}
			if request, ok := input.RequestItems["FooTable"]; ok {
				items := make([]types.WriteRequest, len(request))
				copy(items, request)
				batchRequest = items
			} else {
				t.Error("missing FooTable")
			}
			return &dynamodb.BatchWriteItemOutput{}, errors.New("woop")
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}

	expErr := service.NewBatchError(msg, errors.New("woop"))
	expErr.Failed(1, barErr)
	require.Equal(t, expErr, db.WriteBatch(t.Context(), msg))

	batchExpected := []types.WriteRequest{
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "foo"},
					"content": &types.AttributeValueMemberS{Value: "foo stuff"},
				},
			},
		},
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "bar"},
					"content": &types.AttributeValueMemberS{Value: "bar stuff"},
				},
			},
		},
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "baz"},
					"content": &types.AttributeValueMemberS{Value: "baz stuff"},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.PutItemInput{
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "foo"},
				"content": &types.AttributeValueMemberS{Value: "foo stuff"},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "bar"},
				"content": &types.AttributeValueMemberS{Value: "bar stuff"},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: "baz"},
				"content": &types.AttributeValueMemberS{Value: "baz stuff"},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBSadBatch(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
  content: ${!json("content")}
`)

	var requests [][]types.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (output *dynamodb.BatchWriteItemOutput, err error) {
			output = &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]types.WriteRequest{
					"FooTable": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"id":      &types.AttributeValueMemberS{Value: "bar"},
									"content": &types.AttributeValueMemberS{Value: "bar stuff"},
								},
							},
						},
					},
				},
			}
			if len(requests) < 2 {
				if request, ok := input.RequestItems["FooTable"]; ok {
					items := make([]types.WriteRequest, len(request))
					copy(items, request)
					requests = append(requests, items)
				} else {
					t.Error("missing FooTable")
				}
			}
			return
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}

	require.Equal(t, errors.New("setting 1 items"), db.WriteBatch(t.Context(), msg))

	expected := [][]types.WriteRequest{
		{
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "foo"},
						"content": &types.AttributeValueMemberS{Value: "foo stuff"},
					},
				},
			},
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "bar"},
						"content": &types.AttributeValueMemberS{Value: "bar stuff"},
					},
				},
			},
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "baz"},
						"content": &types.AttributeValueMemberS{Value: "baz stuff"},
					},
				},
			},
		},
		{
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":      &types.AttributeValueMemberS{Value: "bar"},
						"content": &types.AttributeValueMemberS{Value: "bar stuff"},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

// failedIndexes returns the per-message errors carried by a batch error,
// keyed by the index of each message within the batch described by idx.
func failedIndexes(t *testing.T, err error, idx *service.Indexer) map[int]error {
	t.Helper()

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)

	failed := map[int]error{}
	batchErr.WalkMessagesIndexedBy(idx, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed[i] = err
		}
		return true
	})
	return failed
}

func TestDynamoDBBuildFailureWritesRest(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
json_map_columns:
  "": .
`)

	var batchRequests [][]types.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			batchRequests = append(batchRequests, input.RequestItems["FooTable"])
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`not json at all`)),
		service.NewMessage([]byte(`{"id":"baz","content":"baz stuff"}`)),
	}
	idx := msg.Index()

	err := db.WriteBatch(t.Context(), msg)
	require.Error(t, err)

	// The messages that built must still be written in one request.
	require.Len(t, batchRequests, 1)
	assert.Equal(t, []types.WriteRequest{
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "foo"},
					"content": &types.AttributeValueMemberS{Value: "foo stuff"},
				},
			},
		},
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":      &types.AttributeValueMemberS{Value: "baz"},
					"content": &types.AttributeValueMemberS{Value: "baz stuff"},
				},
			},
		},
	}, batchRequests[0])

	// Only the message that failed to build is reported as failed.
	failed := failedIndexes(t, err, idx)
	require.Len(t, failed, 1)
	assert.Error(t, failed[1])
}

func TestDynamoDBBuildFailureAll(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
`)

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`not json`)),
		service.NewMessage([]byte(`also not json`)),
	}
	idx := msg.Index()

	err := db.WriteBatch(t.Context(), msg)
	require.Error(t, err)

	failed := failedIndexes(t, err, idx)
	require.Len(t, failed, 2)
	assert.Error(t, failed[0])
	assert.Error(t, failed[1])
}

func TestDynamoDBBuildFailureWithPutFallback(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
max_retries: 1
backoff:
  max_elapsed_time: 5s
`)

	bazErr := errors.New("dont like baz")
	putIDs := map[string]int{}

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			id := input.Item["id"].(*types.AttributeValueMemberS).Value
			putIDs[id]++
			if id == "baz" {
				return nil, bazErr
			}
			return nil, nil
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			require.Len(t, input.RequestItems["FooTable"], 2)
			return nil, errors.New("woop")
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo"}`)),
		service.NewMessage([]byte(`not json`)),
		service.NewMessage([]byte(`{"id":"baz"}`)),
	}
	idx := msg.Index()

	err := db.WriteBatch(t.Context(), msg)
	require.Error(t, err)

	// foo was written once, baz was retried once, and the message that did
	// not build was never sent.
	assert.Equal(t, 1, putIDs["foo"])
	assert.Equal(t, 2, putIDs["baz"])
	assert.NotContains(t, putIDs, "bar")

	// Both the message that failed to build and the one that failed to write
	// are reported as failed, mapped back to their position in the batch.
	failed := failedIndexes(t, err, idx)
	require.Len(t, failed, 2)
	assert.Error(t, failed[1])
	assert.ErrorIs(t, failed[2], bazErr)
}
