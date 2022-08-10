package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type fnInput struct {
	connect func() error
	read    func() (*Message, AckFunc, error)
	closed  bool
}

func (f *fnInput) Connect(ctx context.Context) error {
	return f.connect()
}

func (f *fnInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	return f.read()
}

func (f *fnInput) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

func TestInputAirGapShutdown(t *testing.T) {
	i := &fnInput{}
	agi := newAirGapReader(i)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, agi.Close(ctx))
	assert.True(t, i.closed)
}

func TestInputAirGapSad(t *testing.T) {
	i := &fnInput{
		connect: func() error {
			return errors.New("bad connect")
		},
		read: func() (*Message, AckFunc, error) {
			return nil, nil, errors.New("bad read")
		},
	}
	agi := newAirGapReader(i)

	err := agi.Connect(context.Background())
	assert.EqualError(t, err, "bad connect")

	_, _, err = agi.ReadBatch(context.Background())
	assert.EqualError(t, err, "bad read")

	i.read = func() (*Message, AckFunc, error) {
		return nil, nil, ErrNotConnected
	}

	_, _, err = agi.ReadBatch(context.Background())
	assert.Equal(t, component.ErrNotConnected, err)

	i.read = func() (*Message, AckFunc, error) {
		return nil, nil, ErrEndOfInput
	}

	_, _, err = agi.ReadBatch(context.Background())
	assert.Equal(t, component.ErrTypeClosed, err)
}

func TestInputAirGapHappy(t *testing.T) {
	var ackErr error
	ackFn := func(ctx context.Context, err error) error {
		ackErr = err
		return nil
	}
	i := &fnInput{
		connect: func() error {
			return nil
		},
		read: func() (*Message, AckFunc, error) {
			m := &Message{
				part: message.NewPart([]byte("hello world")),
			}
			return m, ackFn, nil
		},
	}
	agi := newAirGapReader(i)

	err := agi.Connect(context.Background())
	assert.NoError(t, err)

	outMsg, outAckFn, err := agi.ReadBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, outMsg.Len())
	assert.Equal(t, "hello world", string(outMsg.Get(0).AsBytes()))

	assert.NoError(t, outAckFn(context.Background(), errors.New("foobar")))
	assert.EqualError(t, ackErr, "foobar")
}

type fnBatchInput struct {
	connect func() error
	read    func() (MessageBatch, AckFunc, error)
	closed  bool
}

func (f *fnBatchInput) Connect(ctx context.Context) error {
	return f.connect()
}

func (f *fnBatchInput) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	return f.read()
}

func (f *fnBatchInput) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

func TestBatchInputAirGapShutdown(t *testing.T) {
	i := &fnBatchInput{}
	agi := newAirGapBatchReader(i)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, agi.Close(ctx))
	assert.True(t, i.closed)
}

func TestBatchInputAirGapSad(t *testing.T) {
	i := &fnBatchInput{
		connect: func() error {
			return errors.New("bad connect")
		},
		read: func() (MessageBatch, AckFunc, error) {
			return nil, nil, errors.New("bad read")
		},
	}
	agi := newAirGapBatchReader(i)

	err := agi.Connect(context.Background())
	assert.EqualError(t, err, "bad connect")

	_, _, err = agi.ReadBatch(context.Background())
	assert.EqualError(t, err, "bad read")

	i.read = func() (MessageBatch, AckFunc, error) {
		return nil, nil, ErrNotConnected
	}

	_, _, err = agi.ReadBatch(context.Background())
	assert.Equal(t, component.ErrNotConnected, err)

	i.read = func() (MessageBatch, AckFunc, error) {
		return nil, nil, ErrEndOfInput
	}

	_, _, err = agi.ReadBatch(context.Background())
	assert.Equal(t, component.ErrTypeClosed, err)
}

func TestBatchInputAirGapHappy(t *testing.T) {
	var ackErr error
	ackFn := func(ctx context.Context, err error) error {
		ackErr = err
		return nil
	}
	i := &fnBatchInput{
		connect: func() error {
			return nil
		},
		read: func() (MessageBatch, AckFunc, error) {
			m := MessageBatch{
				NewMessage([]byte("hello world")),
				NewMessage([]byte("this is a test message")),
				NewMessage([]byte("and it will work")),
			}
			return m, ackFn, nil
		},
	}
	agi := newAirGapBatchReader(i)

	err := agi.Connect(context.Background())
	assert.NoError(t, err)

	outMsg, outAckFn, err := agi.ReadBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, outMsg.Len())
	assert.Equal(t, "hello world", string(outMsg.Get(0).AsBytes()))
	assert.Equal(t, "this is a test message", string(outMsg.Get(1).AsBytes()))
	assert.Equal(t, "and it will work", string(outMsg.Get(2).AsBytes()))

	assert.NoError(t, outAckFn(context.Background(), errors.New("foobar")))
	assert.EqualError(t, ackErr, "foobar")
}
