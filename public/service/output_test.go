package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type fnOutput struct {
	connect func() error
	write   func(msg *Message) error
	closed  bool
}

func (f *fnOutput) Connect(ctx context.Context) error {
	return f.connect()
}

func (f *fnOutput) Write(ctx context.Context, msg *Message) error {
	return f.write(msg)
}

func (f *fnOutput) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

func TestOutputAirGapShutdown(t *testing.T) {
	o := &fnOutput{}
	agi := newAirGapWriter(o)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	assert.NoError(t, agi.Close(ctx))
	assert.True(t, o.closed)
}

func TestOutputAirGapSad(t *testing.T) {
	o := &fnOutput{
		connect: func() error {
			return errors.New("bad connect")
		},
		write: func(m *Message) error {
			return errors.New("bad read")
		},
	}
	agi := newAirGapWriter(o)

	err := agi.Connect(context.Background())
	assert.EqualError(t, err, "bad connect")

	err = agi.WriteBatch(context.Background(), message.QuickBatch(nil))
	assert.EqualError(t, err, "bad read")

	o.write = func(m *Message) error {
		return ErrNotConnected
	}

	err = agi.WriteBatch(context.Background(), message.QuickBatch(nil))
	assert.Equal(t, component.ErrNotConnected, err)
}

func TestOutputAirGapHappy(t *testing.T) {
	var wroteMsg string
	o := &fnOutput{
		connect: func() error {
			return nil
		},
		write: func(m *Message) error {
			wroteBytes, _ := m.AsBytes()
			wroteMsg = string(wroteBytes)
			return nil
		},
	}
	agi := newAirGapWriter(o)

	err := agi.Connect(context.Background())
	assert.NoError(t, err)

	inMsg := message.QuickBatch([][]byte{[]byte("hello world")})

	err = agi.WriteBatch(context.Background(), inMsg)
	assert.NoError(t, err)

	assert.Equal(t, "hello world", wroteMsg)
}

type fnBatchOutput struct {
	connect    func() error
	writeBatch func(msgs MessageBatch) error
	closed     bool
}

func (f *fnBatchOutput) Connect(ctx context.Context) error {
	return f.connect()
}

func (f *fnBatchOutput) WriteBatch(ctx context.Context, msgs MessageBatch) error {
	return f.writeBatch(msgs)
}

func (f *fnBatchOutput) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

func TestBatchOutputAirGapShutdown(t *testing.T) {
	o := &fnBatchOutput{}
	agi := newAirGapBatchWriter(o)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	assert.NoError(t, agi.Close(ctx))
	assert.True(t, o.closed)
}

func TestBatchOutputAirGapSad(t *testing.T) {
	o := &fnBatchOutput{
		connect: func() error {
			return errors.New("bad connect")
		},
		writeBatch: func(m MessageBatch) error {
			return errors.New("bad read")
		},
	}
	agi := newAirGapBatchWriter(o)

	err := agi.Connect(context.Background())
	assert.EqualError(t, err, "bad connect")

	err = agi.WriteBatch(context.Background(), message.QuickBatch(nil))
	assert.EqualError(t, err, "bad read")

	o.writeBatch = func(m MessageBatch) error {
		return ErrNotConnected
	}

	err = agi.WriteBatch(context.Background(), message.QuickBatch(nil))
	assert.Equal(t, component.ErrNotConnected, err)
}

func TestBatchOutputAirGapHappy(t *testing.T) {
	var wroteMsg string
	o := &fnBatchOutput{
		connect: func() error {
			return nil
		},
		writeBatch: func(m MessageBatch) error {
			wroteBytes, _ := m[0].AsBytes()
			wroteMsg = string(wroteBytes)
			return nil
		},
	}
	agi := newAirGapBatchWriter(o)

	err := agi.Connect(context.Background())
	assert.NoError(t, err)

	inMsg := message.QuickBatch([][]byte{[]byte("hello world")})

	err = agi.WriteBatch(context.Background(), inMsg)
	assert.NoError(t, err)

	assert.Equal(t, "hello world", wroteMsg)
}
