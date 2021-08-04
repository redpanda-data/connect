package buffer

import (
	"context"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/types"
)

type memoryBuffer struct {
	messages       chan types.Message
	endOfInputChan chan struct{}
	closeOnce      sync.Once
}

func newMemoryBuffer(n int) *memoryBuffer {
	return &memoryBuffer{
		messages:       make(chan types.Message, n),
		endOfInputChan: make(chan struct{}),
	}
}

func (m *memoryBuffer) Read(ctx context.Context) (types.Message, AckFunc, error) {
	select {
	case msg := <-m.messages:
		return msg, func(c context.Context, e error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-m.endOfInputChan:
		// Input has ended, so return ErrEndOfBuffer if our buffer is empty.
		select {
		case msg := <-m.messages:
			return msg, func(c context.Context, e error) error {
				// YOLO: Drop messages that are nacked
				return nil
			}, nil
		default:
			return nil, nil, types.ErrTypeClosed
		}
	}
}

func (m *memoryBuffer) Write(ctx context.Context, msg types.Message) error {
	select {
	case m.messages <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
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
