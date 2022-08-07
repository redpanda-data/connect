package buffer

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

func TestStreamMemoryBuffer(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	var incr, total uint8 = 100, 50

	tChan := make(chan message.Transaction)
	resChan := make(chan error)

	b := NewStream("meow", newMemoryBuffer(int(total)), component.NoopObservability())
	require.NoError(t, b.Consume(tChan))

	var i uint8

	// Check correct flow no blocking
	for ; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		// Send to buffer
		case tChan <- message.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v send", i)
		}

		// Instant response from buffer
		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v response", i)
		}

		// Receive on output
		var outTr message.Transaction
		select {
		case outTr = <-b.TransactionChan():
			assert.Equal(t, i, outTr.Payload.Get(0).AsBytes()[0])
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v read", i)
		}

		// Response from output
		require.NoError(t, outTr.Ack(tCtx, nil))
	}

	for i = 0; i <= total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		case tChan <- message.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v send", i)
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v response", i)
		}
	}

	// Should have reached limit here
	msgBytes := make([][]byte, 1)
	msgBytes[0] = make([]byte, int(incr)+1)

	select {
	case tChan <- message.NewTransaction(message.QuickBatch(msgBytes), resChan):
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for final buffered message send")
	}

	// Response should block until buffer is relieved
	select {
	case res := <-resChan:
		if res != nil {
			t.Fatal(res)
		} else {
			t.Fatalf("Overflowed response returned before timeout")
		}
	case <-time.After(100 * time.Millisecond):
	}

	var outTr message.Transaction

	// Extract last message
	select {
	case outTr = <-b.TransactionChan():
		assert.Equal(t, byte(0), outTr.Payload.Get(0).AsBytes()[0])
		require.NoError(t, outTr.Ack(tCtx, nil))
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for final buffered message read")
	}

	// Response from the last attempt should no longer be blocking
	select {
	case res := <-resChan:
		assert.NoError(t, res)
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Final buffered response blocked")
	}

	// Extract all other messages
	for i = 1; i <= total; i++ {
		select {
		case outTr = <-b.TransactionChan():
			assert.Equal(t, i, outTr.Payload.Get(0).AsBytes()[0])
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v read", i)
		}
		require.NoError(t, outTr.Ack(tCtx, nil))
	}

	// Get final message
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for buffered message %v read", i)
	}

	require.NoError(t, outTr.Ack(tCtx, nil))

	b.TriggerCloseNow()
	require.NoError(t, b.WaitForClose(tCtx))

	close(resChan)
	close(tChan)
}

func TestStreamBufferClosing(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	var incr, total uint8 = 100, 5

	tChan := make(chan message.Transaction)
	resChan := make(chan error)

	b := NewStream("meow", newMemoryBuffer(int(total)), component.NoopObservability())
	require.NoError(t, b.Consume(tChan))

	var i uint8

	// Populate buffer with some messages
	for i = 0; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		case tChan <- message.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v send", i)
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v response", i)
		}
	}

	// Close input, this should prompt the stack buffer to Flush().
	close(tChan)

	// Receive all of those messages from the buffer
	for i = 0; i < total; i++ {
		select {
		case val := <-b.TransactionChan():
			assert.Equal(t, i, val.Payload.Get(0).AsBytes()[0])
			require.NoError(t, val.Ack(tCtx, nil))
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for final buffered message read")
		}
	}

	// The buffer should now be closed, therefore so should our read channel.
	select {
	case _, open := <-b.TransactionChan():
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for final buffered message read")
	}

	// Should already be shut down.
	assert.NoError(t, b.WaitForClose(tCtx))
}

//------------------------------------------------------------------------------

type readErrorBuffer struct {
	readErrs chan error
}

func (r *readErrorBuffer) Read(ctx context.Context) (message.Batch, AckFunc, error) {
	select {
	case err := <-r.readErrs:
		return nil, nil, err
	default:
	}
	return message.QuickBatch([][]byte{[]byte("hello world")}), func(c context.Context, e error) error {
		return nil
	}, nil
}

func (r *readErrorBuffer) Write(ctx context.Context, msg message.Batch, aFn AckFunc) error {
	return aFn(context.Background(), nil)
}

func (r *readErrorBuffer) EndOfInput() {
}

func (r *readErrorBuffer) Close(ctx context.Context) error {
	return nil
}

func TestStreamReadErrors(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	tChan := make(chan message.Transaction)
	resChan := make(chan error)

	errBuf := &readErrorBuffer{
		readErrs: make(chan error, 2),
	}
	errBuf.readErrs <- errors.New("first error")
	errBuf.readErrs <- errors.New("second error")

	b := NewStream("meow", errBuf, component.NoopObservability())
	require.NoError(t, b.Consume(tChan))

	var tran message.Transaction
	select {
	case tran = <-b.TransactionChan():
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}

	require.Equal(t, 1, tran.Payload.Len())
	assert.Equal(t, "hello world", string(tran.Payload.Get(0).AsBytes()))

	require.NoError(t, tran.Ack(tCtx, nil))

	b.TriggerCloseNow()
	require.NoError(t, b.WaitForClose(tCtx))

	close(resChan)
	close(tChan)
}
