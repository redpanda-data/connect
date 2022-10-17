package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxInFlightPassThrough(t *testing.T) {
	readerImpl := newMockInput()

	wrapped := InputWithMaxInFlight(0, readerImpl)

	_, isMock := wrapped.(*mockInput)
	require.True(t, isMock)
}

func TestMaxInFlightLimit(t *testing.T) {
	getCtx := func() context.Context {
		t.Helper()
		ctx, done := context.WithTimeout(context.Background(), time.Millisecond*50)
		t.Cleanup(done)
		return ctx
	}

	readerImpl := newMockInput()
	readerImpl.msgsToSnd = []*Message{
		NewMessage([]byte("foo1")),
		NewMessage([]byte("foo2")),
		NewMessage([]byte("foo3")),
		NewMessage([]byte("foo4")),
		NewMessage([]byte("foo5")),
	}

	wrapped := InputWithMaxInFlight(3, readerImpl)

	_, isMock := wrapped.(*mockInput)
	require.False(t, isMock)

	go func() {
		readerImpl.connChan <- nil
		for i := 0; i < 5; i++ {
			readerImpl.readChan <- nil
		}
		readerImpl.closeChan <- nil
	}()
	go func() {
		for i := 0; i < 5; i++ {
			readerImpl.ackChan <- nil
		}
	}()

	require.NoError(t, wrapped.Connect(getCtx()))

	// Message 1 of 3.
	outMsg, aFn1, err := wrapped.Read(getCtx())
	require.NoError(t, err)

	mBytes, err := outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo1", string(mBytes))

	// Message 2 of 3.
	outMsg, aFn2, err := wrapped.Read(getCtx())
	require.NoError(t, err)

	mBytes, err = outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo2", string(mBytes))

	// Message 3 of 3.
	outMsg, aFn3, err := wrapped.Read(getCtx())
	require.NoError(t, err)

	mBytes, err = outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo3", string(mBytes))

	// Message 4 of 3 never comes.
	tmpCtx, done := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer done()

	_, _, err = wrapped.Read(tmpCtx)
	require.Error(t, err)

	// Ack our three messages.
	require.NoError(t, aFn1(getCtx(), nil))
	require.NoError(t, aFn2(getCtx(), nil))
	require.NoError(t, aFn3(getCtx(), nil))

	// Message 4 of 5.
	outMsg, aFn4, err := wrapped.Read(getCtx())
	require.NoError(t, err)

	mBytes, err = outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo4", string(mBytes))

	// Message 5 of 5.
	outMsg, aFn5, err := wrapped.Read(getCtx())
	require.NoError(t, err)

	mBytes, err = outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo5", string(mBytes))

	// Ack last messages.
	require.NoError(t, aFn4(getCtx(), nil))
	require.NoError(t, aFn5(getCtx(), nil))

	require.NoError(t, wrapped.Close(getCtx()))
}

func TestMaxInFlightLimitBatched(t *testing.T) {
	getCtx := func() context.Context {
		t.Helper()
		ctx, done := context.WithTimeout(context.Background(), time.Millisecond*50)
		t.Cleanup(done)
		return ctx
	}

	readerImpl := newMockBatchInput()
	readerImpl.msgsToSnd = []MessageBatch{
		{NewMessage([]byte("foo1"))},
		{NewMessage([]byte("foo2"))},
		{NewMessage([]byte("foo3"))},
		{NewMessage([]byte("foo4"))},
		{NewMessage([]byte("foo5"))},
	}

	wrapped := InputBatchedWithMaxInFlight(3, readerImpl)

	_, isMock := wrapped.(*mockBatchInput)
	require.False(t, isMock)

	go func() {
		readerImpl.connChan <- nil
		for i := 0; i < 5; i++ {
			readerImpl.readChan <- nil
		}
		readerImpl.closeChan <- nil
	}()
	go func() {
		for i := 0; i < 5; i++ {
			readerImpl.ackChan <- nil
		}
	}()

	require.NoError(t, wrapped.Connect(getCtx()))

	// Message 1 of 3.
	outBatch, aFn1, err := wrapped.ReadBatch(getCtx())
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	mBytes, err := outBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo1", string(mBytes))

	// Message 2 of 3.
	outBatch, aFn2, err := wrapped.ReadBatch(getCtx())
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	mBytes, err = outBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo2", string(mBytes))

	// Message 3 of 3.
	outBatch, aFn3, err := wrapped.ReadBatch(getCtx())
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	mBytes, err = outBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo3", string(mBytes))

	// Message 4 of 3 never comes.
	tmpCtx, done := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer done()

	_, _, err = wrapped.ReadBatch(tmpCtx)
	require.Error(t, err)

	// Ack our three messages.
	require.NoError(t, aFn1(getCtx(), nil))
	require.NoError(t, aFn2(getCtx(), nil))
	require.NoError(t, aFn3(getCtx(), nil))

	// Message 4 of 5.
	outBatch, aFn4, err := wrapped.ReadBatch(getCtx())
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	mBytes, err = outBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo4", string(mBytes))

	// Message 5 of 5.
	outBatch, aFn5, err := wrapped.ReadBatch(getCtx())
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	mBytes, err = outBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo5", string(mBytes))

	// Ack last messages.
	require.NoError(t, aFn4(getCtx(), nil))
	require.NoError(t, aFn5(getCtx(), nil))

	require.NoError(t, wrapped.Close(getCtx()))
}
