package pure

import (
	"context"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/public/service"
)

func memoryBufferConfig() *service.ConfigSpec {
	bs := policy.FieldSpec()
	bs.Name = "batch_policy"
	bs.Description = "Optionally configure a policy to flush buffered messages in batches."
	bs.Examples = nil
	newChildren := []docs.FieldSpec{
		docs.FieldBool("enabled", "Whether to batch messages as they are flushed.").HasDefault(false),
	}
	for _, f := range bs.Children {
		if f.Name == "count" {
			f = f.HasDefault(0)
		}
		if !f.IsDeprecated {
			newChildren = append(newChildren, f)
		}
	}
	bs.Children = newChildren

	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Stores consumed messages in memory and acknowledges them at the input level. During shutdown Benthos will make a best attempt at flushing all remaining messages before exiting cleanly.").
		Description(`
This buffer is appropriate when consuming messages from inputs that do not gracefully handle back pressure and where delivery guarantees aren't critical.

This buffer has a configurable limit, where consumption will be stopped with back pressure upstream if the total size of messages in the buffer reaches this amount. Since this calculation is only an estimate, and the real size of messages in RAM is always higher, it is recommended to set the limit significantly below the amount of RAM available.

## Delivery Guarantees

This buffer intentionally weakens the delivery guarantees of the pipeline and therefore should never be used in places where data loss is unacceptable.

## Batching

It is possible to batch up messages sent from this buffer using a [batch policy](/docs/configuration/batching#batch-policy).`).
		Field(service.NewIntField("limit").
			Description(`The maximum buffer size (in bytes) to allow before applying backpressure upstream.`).
			Default(524288000)).
		Field(service.NewInternalField(bs))
}

func init() {
	err := service.RegisterBatchBuffer(
		"memory", memoryBufferConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return newMemoryBufferFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newMemoryBufferFromConfig(conf *service.ParsedConfig, res *service.Resources) (*memoryBuffer, error) {
	limit, err := conf.FieldInt("limit")
	if err != nil {
		return nil, err
	}

	batchingEnabled, err := conf.FieldBool("batch_policy", "enabled")
	if err != nil {
		return nil, err
	}

	var batcher *service.Batcher
	if batchingEnabled {
		batchPol, err := conf.FieldBatchPolicy("batch_policy")
		if err != nil {
			return nil, err
		}
		if batcher, err = batchPol.NewBatcher(res); err != nil {
			return nil, err
		}
	} else {
		if batcher, err = (service.BatchPolicy{Count: 1}).NewBatcher(res); err != nil {
			return nil, err
		}
	}

	return newMemoryBuffer(limit, batcher), nil
}

//------------------------------------------------------------------------------

type measuredBatch struct {
	b    service.MessageBatch
	size int
}

type memoryBuffer struct {
	batches []measuredBatch
	bytes   int

	cap        int
	cond       *sync.Cond
	endOfInput bool
	closed     bool

	batcher *service.Batcher
}

func newMemoryBuffer(capacity int, batcher *service.Batcher) *memoryBuffer {
	return &memoryBuffer{
		cap:     capacity,
		cond:    sync.NewCond(&sync.Mutex{}),
		batcher: batcher,
	}
}

//------------------------------------------------------------------------------

func (m *memoryBuffer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ctx, done := context.WithCancel(ctx)
	defer done()

	go func() {
		<-ctx.Done()
		m.cond.Broadcast()
	}()

	var batchReady, timedBatch bool

	triggerTimed := func() {
		batchReady = false
		timedBatch = false

		timedDur, exists := m.batcher.UntilNext()
		if !exists {
			return
		}

		timer := time.NewTimer(timedDur)
		go func() {
			defer timer.Stop()
			select {
			case <-timer.C:
				m.cond.L.Lock()
				defer m.cond.L.Unlock()
				timedBatch = true
				batchReady = true
				m.cond.Broadcast()
			case <-ctx.Done():
			}
		}()
	}
	triggerTimed()

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	// The output batch we're forming from the buffered batches
	var outBatch service.MessageBatch

	// The batches that have made up our output batch, this could be multiple
	// batches if we have a batching policy
	var batchSources []measuredBatch

	// The size of the batches that formed our output batch
	var outSize int

	for {
		if m.closed {
			return nil, nil, service.ErrEndOfBuffer
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		for len(m.batches) > 0 && !batchReady {
			outSize += m.batches[0].size
			for _, msg := range m.batches[0].b {
				batchReady = m.batcher.Add(msg.Copy())
			}
			batchSources = append(batchSources, m.batches[0])

			m.batches[0] = measuredBatch{}
			m.batches = m.batches[1:]
		}

		if batchReady || m.endOfInput {
			var err error
			if outBatch, err = m.batcher.Flush(ctx); err != nil {
				return nil, nil, err
			}
			if m.endOfInput && len(batchSources) == 0 {
				return nil, nil, service.ErrEndOfBuffer
			}
			if timedBatch && len(outBatch) == 0 {
				triggerTimed()
				continue
			}
			break
		}

		// None of our exit conditions triggered, so exit
		m.cond.Wait()
	}

	m.cond.Broadcast()
	return outBatch, func(ctx context.Context, err error) error {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()
		if err == nil {
			m.bytes -= outSize
		} else {
			m.batches = append(batchSources, m.batches...)
		}
		m.cond.Broadcast()
		return nil
	}, nil
}

// PushMessage adds a new message to the stack. Returns the backlog in bytes.
func (m *memoryBuffer) WriteBatch(ctx context.Context, msgBatch service.MessageBatch, aFn service.AckFunc) error {
	// Deep copy before acknowledging in order to avoid vague ownership
	msgBatch = msgBatch.DeepCopy()
	if err := aFn(ctx, nil); err != nil {
		return err
	}

	extraBytes := 0
	for _, b := range msgBatch {
		bBytes, err := b.AsBytes()
		if err != nil {
			return err
		}
		extraBytes += len(bBytes)
	}

	if extraBytes > m.cap {
		return component.ErrMessageTooLarge
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.closed {
		return component.ErrTypeClosed
	}

	for (m.bytes + extraBytes) > m.cap {
		m.cond.Wait()
		if m.closed {
			return component.ErrTypeClosed
		}
	}

	m.batches = append(m.batches, measuredBatch{
		b:    msgBatch,
		size: extraBytes,
	})
	m.bytes += extraBytes

	m.cond.Broadcast()
	return nil
}

func (m *memoryBuffer) EndOfInput() {
	go func() {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()

		m.endOfInput = true
		m.cond.Broadcast()

		for m.bytes > 0 && !m.closed {
			m.cond.Wait()
		}
		m.closed = true
		m.cond.Broadcast()
	}()
}

func (m *memoryBuffer) Close(ctx context.Context) error {
	m.cond.L.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.cond.L.Unlock()
	return nil
}
