package pure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func batchEquals(t testing.TB, exp []string, act service.MessageBatch) {
	actStrs := make([]string, len(exp))
	for i, msg := range act {
		actBytes, err := msg.AsBytes()
		require.NoError(t, err)
		actStrs[i] = string(actBytes)
	}
	assert.Equal(t, exp, actStrs)
}

func TestBatchedInputBasic(t *testing.T) {
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(`
batched:
  child:
    generate:
      mapping: 'root.id = count("TEST_BATCHED_INPUT_BASIC")'
      count: 10
      interval: ""
  policy:
    count: 5
`))

	var outBatches []service.MessageBatch
	require.NoError(t, builder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		outBatches = append(outBatches, mb.DeepCopy())
		return nil
	}))

	strm, err := builder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()
	require.NoError(t, strm.Run(ctx))

	require.Len(t, outBatches, 2)
	require.Len(t, outBatches[0], 5)
	require.Len(t, outBatches[1], 5)

	batchEquals(t, []string{
		`{"id":1}`,
		`{"id":2}`,
		`{"id":3}`,
		`{"id":4}`,
		`{"id":5}`,
	}, outBatches[0])

	batchEquals(t, []string{
		`{"id":6}`,
		`{"id":7}`,
		`{"id":8}`,
		`{"id":9}`,
		`{"id":10}`,
	}, outBatches[1])
}

func TestBatchedInputProcessors(t *testing.T) {
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(`
batched:
  child:
    generate:
      mapping: 'root.id = count("TEST_BATCHED_INPUT_PROCESSORS")'
      count: 10
      interval: ""
    processors:
      - mapping: 'root = content().uppercase()'
  policy:
    count: 5
processors:
  - mutation: 'root.x = batch_size()'
  - mapping: 'root = content() + " and this"'
`))

	var outBatches []service.MessageBatch
	require.NoError(t, builder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		outBatches = append(outBatches, mb.DeepCopy())
		return nil
	}))

	strm, err := builder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()
	require.NoError(t, strm.Run(ctx))

	require.Len(t, outBatches, 2)
	require.Len(t, outBatches[0], 5)
	require.Len(t, outBatches[1], 5)

	batchEquals(t, []string{
		`{"ID":1,"x":5} and this`,
		`{"ID":2,"x":5} and this`,
		`{"ID":3,"x":5} and this`,
		`{"ID":4,"x":5} and this`,
		`{"ID":5,"x":5} and this`,
	}, outBatches[0])

	batchEquals(t, []string{
		`{"ID":6,"x":5} and this`,
		`{"ID":7,"x":5} and this`,
		`{"ID":8,"x":5} and this`,
		`{"ID":9,"x":5} and this`,
		`{"ID":10,"x":5} and this`,
	}, outBatches[1])
}
