package processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fnProcessor struct {
	fn     func(context.Context, *message.Part) ([]*message.Part, error)
	closed bool

	sync.Mutex
}

func (p *fnProcessor) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	return p.fn(ctx, msg)
}

func (p *fnProcessor) Close(ctx context.Context) error {
	p.Lock()
	p.closed = true
	p.Unlock()
	return nil
}

func TestProcessorAirGapShutdown(t *testing.T) {
	rp := &fnProcessor{}
	agrp := NewV2ToV1Processor("foo", rp, metrics.Noop())

	err := agrp.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	rp.Lock()
	assert.False(t, rp.closed)
	rp.Unlock()

	agrp.CloseAsync()
	err = agrp.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	rp.Lock()
	assert.True(t, rp.closed)
	rp.Unlock()
}

func TestProcessorAirGapOneToOne(t *testing.T) {
	agrp := NewV2ToV1Processor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			if b := m.Get(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			newPart := m.Copy()
			newPart.Set([]byte("changed"))
			return []*message.Part{newPart}, nil
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "changed", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "unchanged", string(msg.Get(0).Get()))
}

func TestProcessorAirGapOneToError(t *testing.T) {
	agrp := NewV2ToV1Processor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			_, err := m.JSON()
			return nil, err
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "invalid character 'o' in literal null (expecting 'u')", GetFail(msgs[0].Get(0)))
}

func TestProcessorAirGapOneToMany(t *testing.T) {
	agrp := NewV2ToV1Processor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			if b := m.Get(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			first := m.Copy()
			second := m.Copy()
			third := m.Copy()
			first.Set([]byte("changed 1"))
			second.Set([]byte("changed 2"))
			third.Set([]byte("changed 3"))
			return []*message.Part{first, second, third}, nil
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 3, msgs[0].Len())
	assert.Equal(t, "changed 1", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "changed 2", string(msgs[0].Get(1).Get()))
	assert.Equal(t, "changed 3", string(msgs[0].Get(2).Get()))
	assert.Equal(t, "unchanged", string(msg.Get(0).Get()))
}

//------------------------------------------------------------------------------

type fnBatchProcessor struct {
	fn     func(context.Context, *message.Batch) ([]*message.Batch, error)
	closed bool
}

func (p *fnBatchProcessor) ProcessBatch(ctx context.Context, _ []*tracing.Span, batch *message.Batch) ([]*message.Batch, error) {
	return p.fn(ctx, batch)
}

func (p *fnBatchProcessor) Close(ctx context.Context) error {
	p.closed = true
	return nil
}

func TestBatchProcessorAirGapShutdown(t *testing.T) {
	rp := &fnBatchProcessor{}
	agrp := NewV2BatchedToV1Processor("foo", rp, metrics.Noop())

	err := agrp.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rp.closed)

	agrp.CloseAsync()
	err = agrp.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rp.closed)
}

func TestBatchProcessorAirGapOneToOne(t *testing.T) {
	agrp := NewV2BatchedToV1Processor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs *message.Batch) ([]*message.Batch, error) {
			if b := msgs.Get(0).Get(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			newMsg := msgs.Get(0).Copy()
			newMsg.Set([]byte("changed"))
			newBatch := message.QuickBatch(nil)
			newBatch.Append(newMsg)
			return []*message.Batch{newBatch}, nil
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "changed", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "unchanged", string(msg.Get(0).Get()))
}

func TestBatchProcessorAirGapOneToError(t *testing.T) {
	agrp := NewV2BatchedToV1Processor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs *message.Batch) ([]*message.Batch, error) {
			_, err := msgs.Get(0).JSON()
			return nil, err
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "invalid character 'o' in literal null (expecting 'u')", GetFail(msgs[0].Get(0)))
}

func TestBatchProcessorAirGapOneToMany(t *testing.T) {
	agrp := NewV2BatchedToV1Processor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs *message.Batch) ([]*message.Batch, error) {
			if b := msgs.Get(0).Get(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			first := msgs.Get(0).Copy()
			second := msgs.Get(0).Copy()
			third := msgs.Get(0).Copy()
			first.Set([]byte("changed 1"))
			second.Set([]byte("changed 2"))
			third.Set([]byte("changed 3"))

			firstBatch := message.QuickBatch(nil)
			firstBatch.Append(first, second)

			secondBatch := message.QuickBatch(nil)
			secondBatch.Append(third)
			return []*message.Batch{firstBatch, secondBatch}, nil
		},
	}, metrics.Noop())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, msgs, 2)
	assert.Equal(t, "unchanged", string(msg.Get(0).Get()))

	assert.Equal(t, 2, msgs[0].Len())
	assert.Equal(t, "changed 1", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "changed 2", string(msgs[0].Get(1).Get()))

	assert.Equal(t, 1, msgs[1].Len())
	assert.Equal(t, "changed 3", string(msgs[1].Get(0).Get()))
}
