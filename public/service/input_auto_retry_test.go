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

type mockInput struct {
	msgsToSnd  []*Message
	ackRcvdMut sync.Mutex
	ackRcvd    []error

	connChan  chan error
	readChan  chan error
	ackChan   chan error
	closeChan chan error
}

func newMockInput() *mockInput {
	return &mockInput{
		connChan:  make(chan error),
		readChan:  make(chan error),
		ackChan:   make(chan error),
		closeChan: make(chan error),
	}
}

func (i *mockInput) Connect(ctx context.Context) error {
	cerr, open := <-i.connChan
	if !open {
		return ErrEndOfInput
	}
	return cerr
}

func (i *mockInput) Read(ctx context.Context) (*Message, AckFunc, error) {
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

	nextMsg := NewMessage(nil)
	if len(i.msgsToSnd) > 0 {
		nextMsg = i.msgsToSnd[0]
		i.msgsToSnd = i.msgsToSnd[1:]
	}

	return nextMsg.Copy(), func(ctx context.Context, res error) error {
		i.ackRcvdMut.Lock()
		i.ackRcvd[index] = res
		i.ackRcvdMut.Unlock()
		return <-i.ackChan
	}, nil
}

func (i *mockInput) Close(ctx context.Context) error {
	return <-i.closeChan
}

func TestAutoRetryConfig(t *testing.T) {
	spec := NewConfigSpec().Field(NewAutoRetryNacksToggleField())
	for conf, shouldRetry := range map[string]bool{
		`{}`:                       true,
		`auto_replay_nacks: false`: false,
		`auto_replay_nacks: true`:  true,
	} {
		inConf, err := spec.ParseYAML(conf, nil)
		require.NoError(t, err, conf)

		readerImpl := newMockInput()
		pres, err := AutoRetryNacksToggled(inConf, readerImpl)
		require.NoError(t, err, conf)

		_, isWrapped := pres.(*autoRetryInput)
		assert.Equal(t, shouldRetry, isWrapped, conf)
	}
}

func TestAutoRetryClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockInput()
	pres := AutoRetryNacks(readerImpl)

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

func TestAutoRetryHappy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockInput()
	readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, NewMessage([]byte("foo")))

	pres := AutoRetryNacks(readerImpl)

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

	msg, _, err := pres.Read(ctx)
	require.NoError(t, err)

	act, err := msg.AsBytes()
	require.NoError(t, err)

	assert.Equal(t, "foo", string(act))
}

func TestAutoRetryErrorProp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockInput()
	pres := AutoRetryNacks(readerImpl)

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

	_, _, err := pres.Read(ctx)
	assert.Equal(t, expErr, err)

	_, aFn, err := pres.Read(ctx)
	require.NoError(t, err)

	assert.Equal(t, expErr, aFn(ctx, nil))
}

func TestAutoRetryErrorBackoff(t *testing.T) {
	t.Skip("Not liked by the race detector")
	t.Parallel()

	readerImpl := newMockInput()
	pres := AutoRetryNacks(readerImpl)

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
		_, aFn, actErr := pres.Read(ctx)
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

func TestAutoRetryBuffer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockInput()
	pres := AutoRetryNacks(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgsToSnd = []*Message{
			NewMessage([]byte(content)),
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
	msg, aFn, err := pres.Read(ctx)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	// Mutate the message to ensure it's not changed
	msg.SetBytes([]byte("mutated message"))

	// Prime second message.
	go sendMsg(exp2)

	// Fail previous message, expecting it to be resent.
	_ = aFn(ctx, errors.New("failed"))
	msg, aFn, err = pres.Read(ctx)
	require.NoError(t, err)

	b, err = msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	// Read the primed message.
	var aFn2 AckFunc
	msg, aFn2, err = pres.Read(ctx)
	require.NoError(t, err)

	b, err = msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp2, string(b))

	// Fail both messages, expecting them to be resent.
	_ = aFn(ctx, errors.New("failed again"))
	_ = aFn2(ctx, errors.New("failed again"))

	// Read both messages.
	msg, aFn, err = pres.Read(ctx)
	require.NoError(t, err)

	b, err = msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp, string(b))

	msg, aFn2, err = pres.Read(ctx)
	require.NoError(t, err)

	b, err = msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp2, string(b))

	// Prime a new message and also an acknowledgement.
	go sendMsg(exp3)
	go sendAck()
	go sendAck()

	// Ack all messages.
	_ = aFn(ctx, nil)
	_ = aFn2(ctx, nil)

	msg, _, err = pres.Read(ctx)
	require.NoError(t, err)

	b, err = msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, exp3, string(b))
}

func TestAutoRetryReadAfterClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	readerImpl := newMockInput()
	readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, NewMessage([]byte("foo")))

	pres := AutoRetryNacks(readerImpl)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
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

		// Force mockInput.Read() to return ErrEndOfInput after the first
		// message was read
		close(readerImpl.readChan)
	}()

	require.NoError(t, pres.Connect(ctx))

	msg, fn, err := pres.Read(ctx)
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Acknowledge the message explicitly so the input doesn't attempt to
		// redeliver it
		require.NoError(t, fn(ctx, err))
	}()

	select {
	// Push a message on the ackChan to unblock the message acknowledgement
	case readerImpl.ackChan <- nil:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	act, err := msg.AsBytes()
	require.NoError(t, err)

	assert.Equal(t, "foo", string(act))

	// Wait for the input to close and for the message ack to be sent
	wg.Wait()

	_, _, err = pres.Read(ctx)
	assert.Equal(t, ErrEndOfInput, err)
}
