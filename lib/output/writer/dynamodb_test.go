package writer

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	delfn   func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	putfn   func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batchFn func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

func (m *mockDynamoDB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return m.delfn(input)
}

func (m *mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return m.putfn(input)
}

func (m *mockDynamoDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return m.batchFn(input)
}

func TestDynamoDBHappy(t *testing.T) {
	conf := NewDynamoDBConfig()
	conf.JSONMapColumns = map[string]string{
		"": ".",
	}
	conf.Table = "FooTable"
	conf.DeleteMapping = `!this.exists("content")`

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var request map[string][]*dynamodb.WriteRequest

	db.client = &mockDynamoDB{
		delfn: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		putfn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			request = input.RequestItems
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz"}`),
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
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"id": {
							S: aws.String("baz"),
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

	conf := NewDynamoDBConfig()
	conf.JSONMapColumns = map[string]string{
		"": ".",
	}
	conf.Backoff.MaxElapsedTime = "100ms"
	conf.Table = "FooTable"
	conf.DeleteMapping = `!this.exists("content")`

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var batchRequest []*dynamodb.WriteRequest
	var putRequests []*dynamodb.PutItemInput
	var delRequests []*dynamodb.DeleteItemInput

	db.client = &mockDynamoDB{
		delfn: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			delRequests = append(delRequests, input)
			return nil, nil
		},
		putfn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			putRequests = append(putRequests, input)
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

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz"}`),
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
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"id": {S: aws.String("baz")},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	putExpected := []*dynamodb.PutItemInput{
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
	}

	assert.Equal(t, putExpected, putRequests)

	delExpected := []*dynamodb.DeleteItemInput{
		{
			TableName: aws.String("FooTable"),
			Key: map[string]*dynamodb.AttributeValue{
				"id": {S: aws.String("baz")},
			},
		},
	}

	assert.Equal(t, delExpected, delRequests)
}

func TestDynamoDBSadToGoodBatch(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.JSONMapColumns = map[string]string{
		"": ".",
	}
	conf.Table = "FooTable"
	conf.DeleteMapping = `!this.exists("content")`

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var requests [][]*dynamodb.WriteRequest

	db.client = &mockDynamoDB{
		delfn: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		putfn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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
							{
								DeleteRequest: &dynamodb.DeleteRequest{
									Key: map[string]*dynamodb.AttributeValue{
										"id": {S: aws.String("bar")},
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

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz"}`),
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
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"id": {S: aws.String("baz")},
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
			{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"id": {S: aws.String("bar")},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBSad(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.JSONMapColumns = map[string]string{
		"": ".",
	}
	conf.Table = "FooTable"
	conf.DeleteMapping = `!this.exists("content")`

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var batchRequest []*dynamodb.WriteRequest
	var putRequests []*dynamodb.PutItemInput
	var delRequests []*dynamodb.DeleteItemInput

	barErr := errors.New("dont like bar")

	db.client = &mockDynamoDB{
		delfn: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			delRequests = append(delRequests, input)
			return nil, nil
		},
		putfn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			if len(putRequests) < 2 {
				putRequests = append(putRequests, input)
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

	msg := message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz"}`),
	})

	expErr := batch.NewError(msg, errors.New("woop"))
	expErr.Failed(1, barErr)
	require.Equal(t, expErr, db.Write(msg))

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
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"id": {S: aws.String("baz")},
				},
			},
		},
	}

	assert.Equal(t, batchExpected, batchRequest)

	putExpected := []*dynamodb.PutItemInput{
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
	}

	assert.Equal(t, putExpected, putRequests)

	delExpected := []*dynamodb.DeleteItemInput{
		{
			TableName: aws.String("FooTable"),
			Key: map[string]*dynamodb.AttributeValue{
				"id": {S: aws.String("baz")},
			},
		},
	}

	assert.Equal(t, delExpected, delRequests)
}

func TestDynamoDBSadBatch(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.JSONMapColumns = map[string]string{
		"": ".",
	}
	conf.Table = "FooTable"
	conf.DeleteMapping = `!this.exists("content")`

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var requests [][]*dynamodb.WriteRequest

	db.client = &mockDynamoDB{
		delfn: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		putfn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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

	msg := message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz"}`),
	})

	expErr := batch.NewError(msg, errors.New("failed to set 1 items"))
	expErr.Failed(1, errors.New("failed to set item"))
	require.Equal(t, expErr, db.Write(msg))

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
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"id": {S: aws.String("baz")},
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
