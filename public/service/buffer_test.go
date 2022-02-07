package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/buffer"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryBuffer struct {
	messages       chan MessageBatch
	endOfInputChan chan struct{}
	closeOnce      sync.Once
}

func newMemoryBuffer(n int) *memoryBuffer {
	return &memoryBuffer{
		messages:       make(chan MessageBatch, n),
		endOfInputChan: make(chan struct{}),
	}
}

func (m *memoryBuffer) WriteBatch(ctx context.Context, batch MessageBatch, aFn AckFunc) error {
	select {
	case m.messages <- batch:
	case <-ctx.Done():
		return ctx.Err()
	}
	return aFn(context.Background(), nil)
}

func yoloIgnoreNacks(context.Context, error) error {
	// YOLO: Drop messages that are nacked
	return nil
}

func (m *memoryBuffer) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	select {
	case msg := <-m.messages:
		return msg, yoloIgnoreNacks, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-m.endOfInputChan:
		// Input has ended, so return ErrEndOfBuffer if our buffer is empty.
		select {
		case msg := <-m.messages:
			return msg, yoloIgnoreNacks, nil
		default:
			return nil, nil, ErrEndOfBuffer
		}
	}
}

func (m *memoryBuffer) EndOfInput() {
	m.closeOnce.Do(func() {
		close(m.endOfInputChan)
	})
}

func (m *memoryBuffer) Close(ctx context.Context) error {
	// Nothing to clean up
	return nil
}

func TestStreamMemoryBuffer(t *testing.T) {
	var incr, total uint8 = 100, 50

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	b := buffer.NewStream("meow", newAirGapBatchBuffer(newMemoryBuffer(int(total))), log.Noop(), metrics.Noop())
	require.NoError(t, b.Consume(tChan))

	var i uint8

	// Check correct flow no blocking
	for ; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		// Send to buffer
		case tChan <- types.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v send", i)
		}

		// Instant response from buffer
		select {
		case res := <-resChan:
			require.NoError(t, res.Error())
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v response", i)
		}

		// Receive on output
		var outTr types.Transaction
		select {
		case outTr = <-b.TransactionChan():
			assert.Equal(t, i, outTr.Payload.Get(0).Get()[0])
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v read", i)
		}

		// Response from output
		select {
		case outTr.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered response send back %v", i)
		}
	}

	for i = 0; i <= total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		case tChan <- types.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v send", i)
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res.Error())
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v response", i)
		}
	}

	// Should have reached limit here
	msgBytes := make([][]byte, 1)
	msgBytes[0] = make([]byte, int(incr)+1)

	select {
	case tChan <- types.NewTransaction(message.QuickBatch(msgBytes), resChan):
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for final buffered message send")
	}

	// Response should block until buffer is relieved
	select {
	case res := <-resChan:
		if res.Error() != nil {
			t.Fatal(res.Error())
		} else {
			t.Fatalf("Overflowed response returned before timeout")
		}
	case <-time.After(100 * time.Millisecond):
	}

	var outTr types.Transaction

	// Extract last message
	select {
	case outTr = <-b.TransactionChan():
		assert.Equal(t, byte(0), outTr.Payload.Get(0).Get()[0])
		outTr.ResponseChan <- response.NewAck()
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for final buffered message read")
	}

	// Response from the last attempt should no longer be blocking
	select {
	case res := <-resChan:
		assert.NoError(t, res.Error())
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Final buffered response blocked")
	}

	// Extract all other messages
	for i = 1; i <= total; i++ {
		select {
		case outTr = <-b.TransactionChan():
			assert.Equal(t, i, outTr.Payload.Get(0).Get()[0])
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v read", i)
		}

		select {
		case outTr.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered response send back %v", i)
		}
	}

	// Get final message
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for buffered message %v read", i)
	}

	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for buffered response send back %v", i)
	}

	b.CloseAsync()
	require.NoError(t, b.WaitForClose(time.Second))

	close(resChan)
	close(tChan)
}

func TestStreamBufferClosing(t *testing.T) {
	var incr, total uint8 = 100, 5

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	b := buffer.NewStream("meow", newAirGapBatchBuffer(newMemoryBuffer(int(total))), log.Noop(), metrics.Noop())
	require.NoError(t, b.Consume(tChan))

	var i uint8

	// Populate buffer with some messages
	for i = 0; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = i

		select {
		case tChan <- types.NewTransaction(message.QuickBatch(msgBytes), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for buffered message %v send", i)
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res.Error())
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
			assert.Equal(t, i, val.Payload.Get(0).Get()[0])
			val.ResponseChan <- response.NewAck()
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
	assert.NoError(t, b.WaitForClose(time.Second))
}
