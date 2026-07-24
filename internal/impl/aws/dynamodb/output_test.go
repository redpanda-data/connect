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
	"fmt"
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

	expErr := service.NewBatchError(msg, errors.New("setting 1 items"))
	expErr.Failed(1, errors.New("setting 1 items"))
	require.Equal(t, expErr, db.WriteBatch(t.Context(), msg))

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

// TestDynamoDBChunkedBatchWrite verifies that WriteBatch splits batches
// larger than the 25-item BatchWriteItem limit and sends every chunk.
func TestDynamoDBChunkedBatchWrite(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
`)

	var chunkSizes []int
	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			chunkSizes = append(chunkSizes, len(input.RequestItems["FooTable"]))
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	const total = 60
	batch := make(service.MessageBatch, total)
	for i := range total {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d"}`, i))
	}

	require.NoError(t, db.WriteBatch(t.Context(), batch))
	assert.Equal(t, []int{25, 25, 10}, chunkSizes)
}

// TestDynamoDBChunkedFallbackContinuesToNextChunk guards against a bug where
// a BatchWriteItem failure in chunk N that was fully recovered via individual
// PutItem fallback would cause WriteBatch to return nil without sending
// chunks N+1..end, silently dropping those items.
func TestDynamoDBChunkedFallbackContinuesToNextChunk(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
`)

	var batchCalls int
	var putIDs []string
	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			putIDs = append(putIDs, input.Item["id"].(*types.AttributeValueMemberS).Value)
			return &dynamodb.PutItemOutput{}, nil
		},
		batchFn: func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			batchCalls++
			if batchCalls == 1 {
				return &dynamodb.BatchWriteItemOutput{}, errors.New("forced batch failure")
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	const total = 30
	batch := make(service.MessageBatch, total)
	for i := range total {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d"}`, i))
	}

	require.NoError(t, db.WriteBatch(t.Context(), batch))

	// Chunk 0 fell back to individual PutItem for all 25 items; chunk 1
	// must still be sent as a batch after chunk 0 recovers.
	assert.Len(t, putIDs, 25)
	assert.Equal(t, 2, batchCalls)
}

// TestDynamoDBChunkedUnprocessedReturnsBatchError guards against a bug where
// exhausting the unprocessed-items retry budget returned a plain error,
// causing the framework to retry the whole original batch (duplicating
// already-written earlier chunks) and leaving later chunks unreported. The
// writer must return a BatchError that marks only the genuinely unwritten
// items, plus items in unattempted later chunks.
func TestDynamoDBChunkedUnprocessedReturnsBatchError(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
backoff:
  max_elapsed_time: 100ms
`)

	var batchCalls int
	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			batchCalls++
			if batchCalls == 1 {
				return &dynamodb.BatchWriteItemOutput{}, nil
			}
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]types.WriteRequest{
					"FooTable": input.RequestItems["FooTable"],
				},
			}, nil
		},
	}

	const total = 40
	batch := make(service.MessageBatch, total)
	for i := range total {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d"}`, i))
	}

	expErr := service.NewBatchError(batch, errors.New("ran out of request retries"))
	for i := 25; i < total; i++ {
		expErr.Failed(i, errors.New("ran out of request retries"))
	}
	require.Equal(t, expErr, db.WriteBatch(t.Context(), batch))
}

// TestDynamoDBChunkedUnprocessedUnmatchedFallback guards the pessimistic
// fallback path: if the SDK returns an unprocessed WriteRequest that we
// cannot match against any item we sent (an invariant we rely on via
// reflect.DeepEqual), we must still return a BatchError that marks the
// whole chunk as failed — never silently drop an unmatched item.
func TestDynamoDBChunkedUnprocessedUnmatchedFallback(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
backoff:
  max_elapsed_time: 100ms
`)

	db.client = &mockDynamoDB{
		fn: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			// Return an unprocessed item that does not match anything we
			// sent (different id), simulating SDK drift that breaks our
			// DeepEqual mapping heuristic.
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]types.WriteRequest{
					"FooTable": {{PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"id": &types.AttributeValueMemberS{Value: "unknown-id"},
						},
					}}},
				},
			}, nil
		},
	}

	const total = 3
	batch := make(service.MessageBatch, total)
	for i := range total {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d"}`, i))
	}

	expErr := service.NewBatchError(batch, errors.New("ran out of request retries"))
	for i := range total {
		expErr.Failed(i, errors.New("ran out of request retries"))
	}
	require.Equal(t, expErr, db.WriteBatch(t.Context(), batch))
}

// TestDynamoDBChunkedIndividualFallbackGlobalIndex verifies that when a
// non-zero chunk falls back to individual PutItem calls and some of those
// PutItems fail, the returned BatchError marks the failed items at their
// *global* batch indices (chunkStart+j), not their local chunk indices.
func TestDynamoDBChunkedIndividualFallbackGlobalIndex(t *testing.T) {
	t.Parallel()

	db := testDDBOWriter(t, `
table: FooTable
string_columns:
  id: ${!json("id")}
backoff:
  initial_interval: 1ms
  max_interval: 10ms
  max_elapsed_time: 50ms
`)

	barErr := errors.New("dont like id-27")
	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			if input.Item["id"].(*types.AttributeValueMemberS).Value == "id-27" {
				return nil, barErr
			}
			return &dynamodb.PutItemOutput{}, nil
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			// Fail chunk 1 (the second chunk) at the batch level so it
			// drops into the individual fallback with chunkStart=25.
			if input.RequestItems["FooTable"][0].PutRequest.Item["id"].(*types.AttributeValueMemberS).Value == "id-25" {
				return &dynamodb.BatchWriteItemOutput{}, errors.New("chunk 1 batch failure")
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	const total = 30
	batch := make(service.MessageBatch, total)
	for i := range total {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d"}`, i))
	}

	expErr := service.NewBatchError(batch, errors.New("chunk 1 batch failure"))
	expErr.Failed(27, barErr)
	require.Equal(t, expErr, db.WriteBatch(t.Context(), batch))
}
