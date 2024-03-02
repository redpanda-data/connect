package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type mockDynamoDB struct {
	dynamoDBAPI
	fn      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batchFn func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

func (m *mockDynamoDB) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.fn(params)
}

func (m *mockDynamoDB) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
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
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			request = input.RequestItems
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	require.NoError(t, db.WriteBatch(context.Background(), service.MessageBatch{
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

	require.NoError(t, db.WriteBatch(context.Background(), service.MessageBatch{
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
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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

	require.NoError(t, db.WriteBatch(context.Background(), service.MessageBatch{
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
	require.Equal(t, expErr, db.WriteBatch(context.Background(), msg))

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
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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

	require.Equal(t, errors.New("failed to set 1 items"), db.WriteBatch(context.Background(), msg))

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
