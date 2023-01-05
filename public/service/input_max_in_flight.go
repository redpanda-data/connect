package service

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// NewInputMaxInFlightField returns a field spec for a common max_in_flight
// input field.
func NewInputMaxInFlightField() *ConfigField {
	return NewIntField("max_in_flight").
		Description("Optionally sets a limit on the number of messages that can be flowing through a Benthos stream pending acknowledgment from the input at any given time. Once a message has been either acknowledged or rejected (nacked) it is no longer considered pending. If the input produces logical batches then each batch is considered a single count against the maximum. **WARNING**: Batching policies at the output level will stall if this field limits the number of messages below the batching threshold. Zero (default) or lower implies no limit.").
		Default(0).
		Advanced()
}

//------------------------------------------------------------------------------

// InputWithMaxInFlight wraps an input with a component that limits the number of
// messages being processed at a given time. When the limit is reached a new
// message will not be consumed until an ack/nack has been returned.
func InputWithMaxInFlight(n int, i Input) Input {
	if n <= 0 {
		return i
	}
	return &maxInFlight{
		i:       i,
		ackSema: semaphore.NewWeighted(int64(n)),
	}
}

type maxInFlight struct {
	i       Input
	ackSema *semaphore.Weighted
}

func (m *maxInFlight) Connect(ctx context.Context) error {
	return m.i.Connect(ctx)
}

func (m *maxInFlight) Read(ctx context.Context) (*Message, AckFunc, error) {
	if err := m.ackSema.Acquire(ctx, 1); err != nil {
		return nil, nil, err
	}
	mRes, aFn, err := m.i.Read(ctx)
	if err != nil {
		m.ackSema.Release(1)
		return nil, nil, err
	}
	return mRes, func(ctx context.Context, err error) error {
		aerr := aFn(ctx, err)
		m.ackSema.Release(1)
		return aerr
	}, nil
}

func (m *maxInFlight) Close(ctx context.Context) error {
	return m.i.Close(ctx)
}

//------------------------------------------------------------------------------

// InputBatchedWithMaxInFlight wraps a batched input with a component that
// limits the number of batches being processed at a given time. When the limit
// is reached a new message batch will not be consumed until an ack/nack has
// been returned.
func InputBatchedWithMaxInFlight(n int, i BatchInput) BatchInput {
	if n <= 0 {
		return i
	}
	return &maxInFlightBatched{
		i:       i,
		ackSema: semaphore.NewWeighted(int64(n)),
	}
}

type maxInFlightBatched struct {
	i       BatchInput
	ackSema *semaphore.Weighted
}

func (m *maxInFlightBatched) Connect(ctx context.Context) error {
	return m.i.Connect(ctx)
}

func (m *maxInFlightBatched) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	if err := m.ackSema.Acquire(ctx, 1); err != nil {
		return nil, nil, err
	}
	mRes, aFn, err := m.i.ReadBatch(ctx)
	if err != nil {
		m.ackSema.Release(1)
		return nil, nil, err
	}
	return mRes, func(ctx context.Context, err error) error {
		aerr := aFn(ctx, err)
		m.ackSema.Release(1)
		return aerr
	}, nil
}

func (m *maxInFlightBatched) Close(ctx context.Context) error {
	return m.i.Close(ctx)
}
