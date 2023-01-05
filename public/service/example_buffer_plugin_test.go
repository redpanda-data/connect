package service_test

import (
	"context"
	"sync"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type memoryBuffer struct {
	messages       chan service.MessageBatch
	endOfInputChan chan struct{}
	closeOnce      sync.Once
}

func newMemoryBuffer(n int) *memoryBuffer {
	return &memoryBuffer{
		messages:       make(chan service.MessageBatch, n),
		endOfInputChan: make(chan struct{}),
	}
}

func (m *memoryBuffer) WriteBatch(ctx context.Context, batch service.MessageBatch, aFn service.AckFunc) error {
	select {
	case m.messages <- batch:
	case <-ctx.Done():
		return ctx.Err()
	}
	// We weaken delivery guarantees here by acknowledging receipt of our batch
	// immediately.
	return aFn(ctx, nil)
}

func yoloIgnoreNacks(context.Context, error) error {
	// YOLO: Drop messages that are nacked
	return nil
}

func (m *memoryBuffer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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
			return nil, nil, service.ErrEndOfBuffer
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

// This example demonstrates how to create a buffer plugin. Buffers are an
// advanced component type that most plugin authors aren't likely to require.
func Example_bufferPlugin() {
	configSpec := service.NewConfigSpec().
		Summary("Creates a lame memory buffer that loses data on forced restarts or service crashes.").
		Field(service.NewIntField("max_batches").Default(100))

	err := service.RegisterBatchBuffer("lame_memory", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			capacity, err := conf.FieldInt("max_batches")
			if err != nil {
				return nil, err
			}
			return newMemoryBuffer(capacity), nil
		})
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI(context.Background())
}
