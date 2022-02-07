package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fnProcessor struct {
	fn     func(context.Context, *Message) (MessageBatch, error)
	closed bool
}

func (p *fnProcessor) Process(ctx context.Context, msg *Message) (MessageBatch, error) {
	return p.fn(ctx, msg)
}

func (p *fnProcessor) Close(ctx context.Context) error {
	p.closed = true
	return nil
}

func TestProcessorAirGapShutdown(t *testing.T) {
	rp := &fnProcessor{}
	agrp := newAirGapProcessor("foo", rp, metrics.Noop())

	err := agrp.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rp.closed)

	agrp.CloseAsync()
	err = agrp.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rp.closed)
}

func TestProcessorAirGapOneToOne(t *testing.T) {
	agrp := newAirGapProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *Message) (MessageBatch, error) {
			if b, err := m.AsBytes(); err != nil || string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			m.SetBytes([]byte("changed"))
			return MessageBatch{m}, nil
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
	agrp := newAirGapProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *Message) (MessageBatch, error) {
			_, err := m.AsStructured()
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
	assert.Equal(t, "invalid character 'o' in literal null (expecting 'u')", processor.GetFail(msgs[0].Get(0)))
}

func TestProcessorAirGapOneToMany(t *testing.T) {
	agrp := newAirGapProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *Message) (MessageBatch, error) {
			if b, err := m.AsBytes(); err != nil || string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			second := m.Copy()
			third := m.Copy()
			m.SetBytes([]byte("changed 1"))
			second.SetBytes([]byte("changed 2"))
			third.SetBytes([]byte("changed 3"))
			return MessageBatch{m, second, third}, nil
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
	fn     func(context.Context, MessageBatch) ([]MessageBatch, error)
	closed bool
}

func (p *fnBatchProcessor) ProcessBatch(ctx context.Context, msg MessageBatch) ([]MessageBatch, error) {
	return p.fn(ctx, msg)
}

func (p *fnBatchProcessor) Close(ctx context.Context) error {
	p.closed = true
	return nil
}

func TestBatchProcessorAirGapShutdown(t *testing.T) {
	rp := &fnBatchProcessor{}
	agrp := newAirGapBatchProcessor("foo", rp, metrics.Noop())

	err := agrp.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rp.closed)

	agrp.CloseAsync()
	err = agrp.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rp.closed)
}

func TestBatchProcessorAirGapOneToOne(t *testing.T) {
	agrp := newAirGapBatchProcessor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs MessageBatch) ([]MessageBatch, error) {
			if b, err := msgs[0].AsBytes(); err != nil || string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			msgs[0].SetBytes([]byte("changed"))
			return []MessageBatch{{msgs[0]}}, nil
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
	agrp := newAirGapBatchProcessor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs MessageBatch) ([]MessageBatch, error) {
			_, err := msgs[0].AsStructured()
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
	assert.Equal(t, "invalid character 'o' in literal null (expecting 'u')", processor.GetFail(msgs[0].Get(0)))
}

func TestBatchProcessorAirGapOneToMany(t *testing.T) {
	agrp := newAirGapBatchProcessor("foo", &fnBatchProcessor{
		fn: func(c context.Context, msgs MessageBatch) ([]MessageBatch, error) {
			if b, err := msgs[0].AsBytes(); err != nil || string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			second := msgs[0].Copy()
			third := msgs[0].Copy()
			msgs[0].SetBytes([]byte("changed 1"))
			second.SetBytes([]byte("changed 2"))
			third.SetBytes([]byte("changed 3"))
			return []MessageBatch{{msgs[0], second}, {third}}, nil
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
