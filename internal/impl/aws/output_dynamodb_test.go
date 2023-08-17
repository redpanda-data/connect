package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	fn      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batchFn func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

func (m *mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return m.fn(input)
}

func (m *mockDynamoDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return m.batchFn(input)
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

	var request map[string][]*dynamodb.WriteRequest

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

	expected := map[string][]*dynamodb.WriteRequest{
		"FooTable": {
			&dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id": {
							S: aws.String("foo"),
						},
						"content": {
							S: aws.String("foo stuff"),
						},
					},
				},
			},
			&dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id": {
							S: aws.String("bar"),
						},
						"content": {
							S: aws.String("bar stuff"),
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

	var batchRequest []*dynamodb.WriteRequest
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
				items := make([]*dynamodb.WriteRequest, len(request))
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

	batchExpected := []*dynamodb.WriteRequest{
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("foo")},
					"content": {S: aws.String("foo stuff")},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("bar")},
					"content": {S: aws.String("bar stuff")},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("baz")},
					"content": {S: aws.String("baz stuff")},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.PutItemInput{
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("foo")},
				"content": {S: aws.String("foo stuff")},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("bar")},
				"content": {S: aws.String("bar stuff")},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("baz")},
				"content": {S: aws.String("baz stuff")},
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

	var requests [][]*dynamodb.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (output *dynamodb.BatchWriteItemOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{
						"FooTable": {
							{
								PutRequest: &dynamodb.PutRequest{
									Item: map[string]*dynamodb.AttributeValue{
										"id":      {S: aws.String("bar")},
										"content": {S: aws.String("bar stuff")},
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
				items := make([]*dynamodb.WriteRequest, len(request))
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

	expected := [][]*dynamodb.WriteRequest{
		{
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("foo")},
						"content": {S: aws.String("foo stuff")},
					},
				},
			},
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("bar")},
						"content": {S: aws.String("bar stuff")},
					},
				},
			},
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("baz")},
						"content": {S: aws.String("baz stuff")},
					},
				},
			},
		},
		{
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("bar")},
						"content": {S: aws.String("bar stuff")},
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

	var batchRequest []*dynamodb.WriteRequest
	var requests []*dynamodb.PutItemInput

	barErr := errors.New("dont like bar")

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			if len(requests) < 3 {
				requests = append(requests, input)
			}
			if *input.Item["id"].S == "bar" {
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
				items := make([]*dynamodb.WriteRequest, len(request))
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

	batchExpected := []*dynamodb.WriteRequest{
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("foo")},
					"content": {S: aws.String("foo stuff")},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("bar")},
					"content": {S: aws.String("bar stuff")},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":      {S: aws.String("baz")},
					"content": {S: aws.String("baz stuff")},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.PutItemInput{
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("foo")},
				"content": {S: aws.String("foo stuff")},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("bar")},
				"content": {S: aws.String("bar stuff")},
			},
		},
		{
			TableName: aws.String("FooTable"),
			Item: map[string]*dynamodb.AttributeValue{
				"id":      {S: aws.String("baz")},
				"content": {S: aws.String("baz stuff")},
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

	var requests [][]*dynamodb.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (output *dynamodb.BatchWriteItemOutput, err error) {
			output = &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"FooTable": {
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"id":      {S: aws.String("bar")},
									"content": {S: aws.String("bar stuff")},
								},
							},
						},
					},
				},
			}
			if len(requests) < 2 {
				if request, ok := input.RequestItems["FooTable"]; ok {
					items := make([]*dynamodb.WriteRequest, len(request))
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

	expErr := service.NewBatchError(msg, errors.New("failed to set 1 items"))
	expErr.Failed(1, errors.New("failed to set item"))
	require.Equal(t, expErr, db.WriteBatch(context.Background(), msg))

	expected := [][]*dynamodb.WriteRequest{
		{
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("foo")},
						"content": {S: aws.String("foo stuff")},
					},
				},
			},
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("bar")},
						"content": {S: aws.String("bar stuff")},
					},
				},
			},
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("baz")},
						"content": {S: aws.String("baz stuff")},
					},
				},
			},
		},
		{
			{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String("bar")},
						"content": {S: aws.String("bar stuff")},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}
