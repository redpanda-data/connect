package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBatchInput struct {
	msgsToSnd  []MessageBatch
	ackRcvdMut sync.Mutex
	ackRcvd    []error

	connChan  chan error
	readChan  chan error
	ackChan   chan error
	closeChan chan error
}

func newMockBatchInput() *mockBatchInput {
	return &mockBatchInput{
		connChan:  make(chan error),
		readChan:  make(chan error),
		ackChan:   make(chan error),
		closeChan: make(chan error),
	}
}

func (i *mockBatchInput) Connect(ctx context.Context) error {
	cerr, open := <-i.connChan
	if !open {
		return ErrEndOfInput
	}
	return cerr
}

func (i *mockBatchInput) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case err, open := <-i.readChan:
		if !open {
			return nil, nil, ErrEndOfInput
		}
		if err != nil {
			return nil, nil, err
		}
	}
	i.ackRcvdMut.Lock()
	i.ackRcvd = append(i.ackRcvd, errors.New("ack not received"))
	index := len(i.ackRcvd) - 1
	i.ackRcvdMut.Unlock()

	nextBatch := MessageBatch{}
	if len(i.msgsToSnd) > 0 {
		nextBatch = i.msgsToSnd[0]
		i.msgsToSnd = i.msgsToSnd[1:]
	}

	return nextBatch.Copy(), func(ctx context.Context, res error) error {
		i.ackRcvdMut.Lock()
		i.ackRcvd[index] = res
		i.ackRcvdMut.Unlock()
		return <-i.ackChan
	}, nil
}

func (i *mockBatchInput) Close(ctx context.Context) error {
	return <-i.closeChan
}

func TestBatchAutoRetryConfig(t *testing.T) {
	spec := NewConfigSpec().Field(NewAutoRetryNacksToggleField())
	for conf, shouldRetry := range map[string]bool{
		`{}`:                       true,
		`auto_replay_nacks: false`: false,
		`auto_replay_nacks: true`:  true,
	} {
		inConf, err := spec.ParseYAML(conf, nil)
		require.NoError(t, err, conf)

		readerImpl := newMockBatchInput()
		pres, err := AutoRetryNacksBatchedToggled(inConf, readerImpl)
		require.NoError(t, err, conf)

		_, isWrapped := pres.(*autoRetryInputBatched)
		assert.Equal(t, shouldRetry, isWrapped, conf)
	}
}

func TestBatchAutoRetryClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockBatchInput()
	pres := AutoRetryNacksBatched(readerImpl)

	expErr := errors.New("foo error")

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := pres.Connect(ctx)
		require.NoError(t, err)

		assert.Equal(t, expErr, pres.Close(ctx))
	}()

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.closeChan <- expErr:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	wg.Wait()
}

func TestBatchAutoRetryHappy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockBatchInput()
	readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, MessageBatch{
		NewMessage([]byte("foo")),
		NewMessage([]byte("bar")),
	})

	pres := AutoRetryNacksBatched(readerImpl)

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	require.NoError(t, pres.Connect(ctx))

	batch, _, err := pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 2)

	act, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo", string(act))

	act, err = batch[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "bar", string(act))
}

func TestBatchAutoRetryErrorProp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockBatchInput()
	pres := AutoRetryNacksBatched(readerImpl)

	expErr := errors.New("foo")

	go func() {
		select {
		case readerImpl.connChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	assert.Equal(t, expErr, pres.Connect(ctx))

	_, _, err := pres.ReadBatch(ctx)
	assert.Equal(t, expErr, err)

	_, aFn, err := pres.ReadBatch(ctx)
	require.NoError(t, err)

	assert.Equal(t, expErr, aFn(ctx, nil))
}

func TestBatchAutoRetryErrorBackoff(t *testing.T) {
	t.Skip("Not liked by the race detector")
	t.Parallel()

	readerImpl := newMockBatchInput()
	pres := AutoRetryNacksBatched(readerImpl)

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.closeChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	require.NoError(t, pres.Connect(ctx))

	i := 0
	for {
		_, aFn, actErr := pres.ReadBatch(ctx)
		if actErr != nil {
			assert.Equal(t, ctx.Err(), actErr)
			break
		}
		require.NoError(t, aFn(ctx, errors.New("no thanks")))
		i++
		if i == 10 {
			t.Error("Expected backoff to prevent this")
			break
		}
	}

	require.NoError(t, pres.Close(context.Background()))
}

func TestBatchAutoRetryBuffer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockBatchInput()
	pres := AutoRetryNacksBatched(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgsToSnd = []MessageBatch{
			{NewMessage([]byte(content))},
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}
	sendAck := func() {
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	// Send message normally.
	exp := "msg 1"
	exp2 := "msg 2"
	exp3 := "msg 3"

	go sendMsg(exp)
	msg, aFn, err := pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err := msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	// Prime second message.
	go sendMsg(exp2)

	// Fail previous message, expecting it to be resent.
	_ = aFn(ctx, errors.New("failed"))
	msg, aFn, err = pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err = msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	// Read the primed message.
	var aFn2 AckFunc
	msg, aFn2, err = pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err = msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp2, string(b))

	// Fail both messages, expecting them to be resent.
	_ = aFn(ctx, errors.New("failed again"))
	_ = aFn2(ctx, errors.New("failed again"))

	// Read both messages.
	msg, aFn, err = pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err = msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	msg, aFn2, err = pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err = msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp2, string(b))

	// Prime a new message and also an acknowledgement.
	go sendMsg(exp3)
	go sendAck()
	go sendAck()

	// Ack all messages.
	_ = aFn(ctx, nil)
	_ = aFn2(ctx, nil)

	msg, _, err = pres.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, msg, 1)

	b, err = msg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp3, string(b))
}
