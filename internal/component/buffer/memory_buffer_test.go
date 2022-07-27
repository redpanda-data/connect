package buffer

import (
	"context"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type memoryBuffer struct {
	messages       chan message.Batch
	endOfInputChan chan struct{}
	closeOnce      sync.Once
}

func newMemoryBuffer(n int) *memoryBuffer {
	return &memoryBuffer{
		messages:       make(chan message.Batch, n),
		endOfInputChan: make(chan struct{}),
	}
}

func (m *memoryBuffer) Read(ctx context.Context) (message.Batch, AckFunc, error) {
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
			return nil, nil, component.ErrTypeClosed
		}
	}
}

func (m *memoryBuffer) Write(ctx context.Context, msg message.Batch, aFn AckFunc) error {
	select {
	case m.messages <- msg:
		if err := aFn(context.Background(), nil); err != nil {
			return err
		}
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
