package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m *mockDynamoDB) BatchExecuteStatementWithContext(ctx context.Context, input *dynamodb.BatchExecuteStatementInput, _ ...request.Option) (*dynamodb.BatchExecuteStatementOutput, error) {
	return m.pbatchFn(ctx, input)
}

func (m *mockDynamoDB) ExecuteStatementWithContext(ctx context.Context, input *dynamodb.ExecuteStatementInput, _ ...request.Option) (*dynamodb.ExecuteStatementOutput, error) {
	return m.pfn(ctx, input)
}

func TestDynamoDBPartiqlHappy(t *testing.T) {
	conf := NewDynamoDBConfig()
	conf.Query = `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	conf.Args = []string{
		`S = json("id")`,
		`S = json("content")`,
	}

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var request []*dynamodb.BatchStatementRequest

	db.client = &mockDynamoDB{
		pfn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{}, nil
		},
	}

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
	})))

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

func TestDynamoDBPartiqlSadToGood(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.Query = `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	conf.Args = []string{
		`S = json("id")`,
		`S = json("content")`,
	}

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var batchRequest []*dynamodb.BatchStatementRequest
	var requests []*dynamodb.ExecuteStatementInput

	db.client = &mockDynamoDB{
		pfn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			requests = append(requests, input)
			return nil, nil
		},
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			if len(batchRequest) > 0 {
				t.Error("not expected")
				return nil, errors.New("not expected")
			}
			batchRequest = append(batchRequest, input.Statements...)
			return &dynamodb.BatchExecuteStatementOutput{}, errors.New("woop")
		},
	}

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})))

	batchExpected := []*dynamodb.BatchStatementRequest{
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
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.ExecuteStatementInput{
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
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBPartiqlSadToGoodBatch(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.Query = `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	conf.Args = []string{
		`S = json("id")`,
		`S = json("content")`,
	}

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var requests [][]*dynamodb.BatchStatementRequest

	db.client = &mockDynamoDB{
		pfn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchExecuteStatementOutput{
					Responses: make([]*dynamodb.BatchStatementResponse, len(input.Statements)),
				}
				for i, stmt := range input.Statements {
					res := &dynamodb.BatchStatementResponse{}
					if *stmt.Parameters[0].S == "bar" {
						res.Error = &dynamodb.BatchStatementError{}
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

	require.NoError(t, db.Write(message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})))

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
		{
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []*dynamodb.AttributeValue{
					{S: aws.String("bar")},
					{S: aws.String("bar stuff")},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBPartiqlSad(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.Query = `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	conf.Args = []string{
		`S = json("id")`,
		`S = json("content")`,
	}

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var batchRequest []*dynamodb.BatchStatementRequest
	var requests []*dynamodb.ExecuteStatementInput

	barErr := errors.New("dont like bar")

	db.client = &mockDynamoDB{
		pfn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			if len(requests) < 3 {
				requests = append(requests, input)
			}
			if *input.Parameters[0].S == "bar" {
				return nil, barErr
			}
			return nil, nil
		},
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			if len(batchRequest) > 0 {
				t.Error("not expected")
				return nil, errors.New("not implemented")
			}
			batchRequest = append(batchRequest, input.Statements...)
			return &dynamodb.BatchExecuteStatementOutput{}, errors.New("woop")
		},
	}

	msg := message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})

	expErr := batch.NewError(msg, errors.New("woop"))
	expErr.Failed(1, barErr)
	require.Equal(t, expErr, db.Write(msg))

	batchExpected := []*dynamodb.BatchStatementRequest{
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
	}

	assert.Equal(t, batchExpected, batchRequest)

	expected := []*dynamodb.ExecuteStatementInput{
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
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBPartiqlSadBatch(t *testing.T) {
	t.Parallel()

	conf := NewDynamoDBConfig()
	conf.Query = `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	conf.Args = []string{
		`S = json("id")`,
		`S = json("content")`,
	}

	db, err := NewDynamoDB(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var requests [][]*dynamodb.BatchStatementRequest

	db.client = &mockDynamoDB{
		pfn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			output = &dynamodb.BatchExecuteStatementOutput{
				Responses: make([]*dynamodb.BatchStatementResponse, len(input.Statements)),
			}
			for i, stmt := range input.Statements {
				res := &dynamodb.BatchStatementResponse{}
				if *stmt.Parameters[0].S == "bar" {
					res.Error = &dynamodb.BatchStatementError{}
				}
				output.Responses[i] = res
			}
			if len(requests) < 2 {
				stmts := make([]*dynamodb.BatchStatementRequest, len(input.Statements))
				copy(stmts, input.Statements)
				requests = append(requests, stmts)
			}
			return
		},
	}

	msg := message.New([][]byte{
		[]byte(`{"id":"foo","content":"foo stuff"}`),
		[]byte(`{"id":"bar","content":"bar stuff"}`),
		[]byte(`{"id":"baz","content":"baz stuff"}`),
	})

	expErr := batch.NewError(msg, errors.New("failed to process 1 statements"))
	expErr.Failed(1, errors.New("failed to process statement: INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"))
	require.Equal(t, expErr, db.Write(msg))

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
		{
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []*dynamodb.AttributeValue{
					{S: aws.String("bar")},
					{S: aws.String("bar stuff")},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}
