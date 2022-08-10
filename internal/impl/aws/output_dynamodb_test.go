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

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
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

func TestDynamoDBHappy(t *testing.T) {
	conf := output.NewDynamoDBConfig()
	conf.StringColumns = map[string]string{
		"id":      `${!json("id")}`,
		"content": `${!json("content")}`,
	}
	conf.Table = "FooTable"

	db, err := newDynamoDBWriter(conf, mock.NewManager())
	require.NoError(t, err)

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

	require.NoError(t, db.WriteBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
	})))

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

	conf := output.NewDynamoDBConfig()
	conf.StringColumns = map[string]string{
		"id":      `${!json("id")}`,
		"content": `${!json("content")}`,
	}
	conf.Backoff.MaxElapsedTime = "100ms"
	conf.Table = "FooTable"

	db, err := newDynamoDBWriter(conf, mock.NewManager())
	require.NoError(t, err)

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

	require.NoError(t, db.WriteBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})))

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

	conf := output.NewDynamoDBConfig()
	conf.StringColumns = map[string]string{
		"id":      `${!json("id")}`,
		"content": `${!json("content")}`,
	}
	conf.Table = "FooTable"

	db, err := newDynamoDBWriter(conf, mock.NewManager())
	require.NoError(t, err)

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

	require.NoError(t, db.WriteBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})))

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

	conf := output.NewDynamoDBConfig()
	conf.StringColumns = map[string]string{
		"id":      `${!json("id")}`,
		"content": `${!json("content")}`,
	}
	conf.Table = "FooTable"

	db, err := newDynamoDBWriter(conf, mock.NewManager())
	require.NoError(t, err)

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

	msg := message.QuickBatch([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})

	expErr := batch.NewError(msg, errors.New("woop"))
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

	conf := output.NewDynamoDBConfig()
	conf.StringColumns = map[string]string{
		"id":      `${!json("id")}`,
		"content": `${!json("content")}`,
	}
	conf.Table = "FooTable"

	db, err := newDynamoDBWriter(conf, mock.NewManager())
	require.NoError(t, err)

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

	msg := message.QuickBatch([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})

	expErr := batch.NewError(msg, errors.New("failed to set 1 items"))
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
