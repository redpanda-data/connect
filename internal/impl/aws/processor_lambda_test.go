package aws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type mockLambda struct {
	lambdaiface.LambdaAPI
	fn func(*lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

func (m *mockLambda) InvokeWithContext(ctx context.Context, in *lambda.InvokeInput, opt ...request.Option) (*lambda.InvokeOutput, error) {
	return m.fn(in)
}

func TestLambdaErrors(t *testing.T) {
	mock := &mockLambda{
		fn: func(ii *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
			require.Equal(t, "foofn", *ii.FunctionName)
			return nil, errors.New("meow " + string(ii.Payload))
		},
	}

	p, err := newLambdaProc(mock, false, "foofn", 3, "", time.Second, service.MockResources())
	require.NoError(t, err)

	bCtx := context.Background()
	inBatch := service.MessageBatch{
		service.NewMessage([]byte("foo")),
		service.NewMessage([]byte("bar")),
		service.NewMessage([]byte("baz")),
	}

	outBatches, err := p.ProcessBatch(bCtx, inBatch)
	require.NoError(t, err)

	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 3)

	assert.EqualError(t, outBatches[0][0].GetError(), "meow foo")
	assert.EqualError(t, outBatches[0][1].GetError(), "meow bar")
	assert.EqualError(t, outBatches[0][2].GetError(), "meow baz")

	p, err = newLambdaProc(mock, true, "foofn", 3, "", time.Second, service.MockResources())
	require.NoError(t, err)

	outBatches, err = p.ProcessBatch(bCtx, inBatch)
	require.NoError(t, err)

	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 3)

	assert.EqualError(t, outBatches[0][0].GetError(), "meow foo")
	assert.EqualError(t, outBatches[0][1].GetError(), "meow bar")
	assert.EqualError(t, outBatches[0][2].GetError(), "meow baz")
}

func TestLambdaMutations(t *testing.T) {
	mock := &mockLambda{
		fn: func(ii *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
			require.Equal(t, "foofn", *ii.FunctionName)
			return &lambda.InvokeOutput{
				Payload: []byte("meow " + string(ii.Payload)),
			}, nil
		},
	}

	p, err := newLambdaProc(mock, false, "foofn", 3, "", time.Second, service.MockResources())
	require.NoError(t, err)

	bCtx := context.Background()
	inBatch := service.MessageBatch{
		service.NewMessage([]byte("foo")),
		service.NewMessage([]byte("bar")),
		service.NewMessage([]byte("baz")),
	}

	outBatches, err := p.ProcessBatch(bCtx, inBatch.Copy())
	require.NoError(t, err)

	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 3)

	b, _ := outBatches[0][0].AsBytes()
	assert.Equal(t, "meow foo", string(b))
	b, _ = outBatches[0][1].AsBytes()
	assert.Equal(t, "meow bar", string(b))
	b, _ = outBatches[0][2].AsBytes()
	assert.Equal(t, "meow baz", string(b))

	// Ensure origin didn't change
	b, _ = inBatch[0].AsBytes()
	assert.Equal(t, "foo", string(b))
	b, _ = inBatch[1].AsBytes()
	assert.Equal(t, "bar", string(b))
	b, _ = inBatch[2].AsBytes()
	assert.Equal(t, "baz", string(b))

	p, err = newLambdaProc(mock, true, "foofn", 3, "", time.Second, service.MockResources())
	require.NoError(t, err)

	outBatches, err = p.ProcessBatch(bCtx, inBatch.Copy())
	require.NoError(t, err)

	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 3)

	b, _ = outBatches[0][0].AsBytes()
	assert.Equal(t, "meow foo", string(b))
	b, _ = outBatches[0][1].AsBytes()
	assert.Equal(t, "meow bar", string(b))
	b, _ = outBatches[0][2].AsBytes()
	assert.Equal(t, "meow baz", string(b))

	// Ensure origin didn't change
	b, _ = inBatch[0].AsBytes()
	assert.Equal(t, "foo", string(b))
	b, _ = inBatch[1].AsBytes()
	assert.Equal(t, "bar", string(b))
	b, _ = inBatch[2].AsBytes()
	assert.Equal(t, "baz", string(b))
}
