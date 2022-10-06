package service

import (
	"context"
	"errors"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Output is an interface implemented by Benthos outputs that support single
// message writes. Each call to Write should block until either the message has
// been successfully or unsuccessfully sent, or the context is cancelled.
//
// Multiple write calls can be performed in parallel, and the constructor of an
// output must provide a MaxInFlight parameter indicating the maximum number of
// parallel write calls the output supports.
type Output interface {
	// Establish a connection to the downstream service. Connect will always be
	// called first when a writer is instantiated, and will be continuously
	// called with back off until a nil error is returned.
	//
	// The provided context remains open only for the duration of the connecting
	// phase, and should not be used to establish the lifetime of the connection
	// itself.
	//
	// Once Connect returns a nil error the write method will be called until
	// either ErrNotConnected is returned, or the writer is closed.
	Connect(context.Context) error

	// Write a message to a sink, or return an error if delivery is not
	// possible.
	//
	// If this method returns ErrNotConnected then write will not be called
	// again until Connect has returned a nil error.
	Write(context.Context, *Message) error

	Closer
}

//------------------------------------------------------------------------------

// BatchOutput is an interface implemented by Benthos outputs that require
// Benthos to batch messages before dispatch in order to improve throughput.
// Each call to WriteBatch should block until either all messages in the batch
// have been successfully or unsuccessfully sent, or the context is cancelled.
//
// Multiple write calls can be performed in parallel, and the constructor of an
// output must provide a MaxInFlight parameter indicating the maximum number of
// parallel batched write calls the output supports.
type BatchOutput interface {
	// Establish a connection to the downstream service. Connect will always be
	// called first when a writer is instantiated, and will be continuously
	// called with back off until a nil error is returned.
	//
	// Once Connect returns a nil error the write method will be called until
	// either ErrNotConnected is returned, or the writer is closed.
	Connect(context.Context) error

	// Write a batch of messages to a sink, or return an error if delivery is
	// not possible.
	//
	// If this method returns ErrNotConnected then write will not be called
	// again until Connect has returned a nil error.
	WriteBatch(context.Context, MessageBatch) error

	Closer
}

//------------------------------------------------------------------------------

// Implements output.AsyncSink.
type airGapWriter struct {
	w Output
}

func newAirGapWriter(w Output) ioutput.AsyncSink {
	return &airGapWriter{w: w}
}

func (a *airGapWriter) Connect(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	err := a.w.Write(ctx, newMessageFromPart(msg.Get(0)))
	if err != nil && errors.Is(err, ErrNotConnected) {
		err = component.ErrNotConnected
	}
	return err
}

func (a *airGapWriter) Close(ctx context.Context) error {
	return a.w.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements output.AsyncSink.
type airGapBatchWriter struct {
	w BatchOutput
}

func newAirGapBatchWriter(w BatchOutput) ioutput.AsyncSink {
	return &airGapBatchWriter{w: w}
}

func (a *airGapBatchWriter) Connect(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapBatchWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	parts := make([]*Message, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		parts[i] = newMessageFromPart(part)
		return nil
	})
	err := a.w.WriteBatch(ctx, parts)
	if err != nil && errors.Is(err, ErrNotConnected) {
		err = component.ErrNotConnected
	}
	err = fromPublicBatchError(err)
	return err
}

func (a *airGapBatchWriter) Close(ctx context.Context) error {
	return a.w.Close(context.Background())
}

//------------------------------------------------------------------------------

// ResourceOutput provides access to an output resource.
type ResourceOutput struct {
	o ioutput.Sync
}

func newResourceOutput(o ioutput.Sync) *ResourceOutput {
	return &ResourceOutput{o: o}
}

// Write a message to the output, or return an error either if delivery is not
// possible or the context is cancelled.
func (o *ResourceOutput) Write(ctx context.Context, m *Message) error {
	payload := message.Batch{m.part}
	return o.writeMsg(ctx, payload)
}

// WriteBatch attempts to write a message batch to the output, and returns an
// error either if delivery is not possible or the context is cancelled.
func (o *ResourceOutput) WriteBatch(ctx context.Context, b MessageBatch) error {
	payload := make(message.Batch, len(b))
	for i, m := range b {
		payload[i] = m.part
	}
	return toPublicBatchError(o.writeMsg(ctx, payload))
}

func (o *ResourceOutput) writeMsg(ctx context.Context, payload message.Batch) error {
	var wg sync.WaitGroup
	var ackErr error
	wg.Add(1)

	if err := o.o.WriteTransaction(ctx, message.NewTransactionFunc(payload, func(ctx context.Context, err error) error {
		ackErr = err
		wg.Done()
		return nil
	})); err != nil {
		return err
	}

	wg.Wait()
	return ackErr
}

//------------------------------------------------------------------------------

// OwnedOutput provides direct ownership of an output extracted from a plugin
// config. Connectivity of the output is handled internally, and so the owner
// of this type should only be concerned with writing messages and eventually
// calling Close to terminate the output.
type OwnedOutput struct {
	o         ioutput.Streamed
	closeOnce sync.Once
	t         chan message.Transaction
}

func newOwnedOutput(o ioutput.Streamed) (*OwnedOutput, error) {
	tChan := make(chan message.Transaction)
	if err := o.Consume(tChan); err != nil {
		return nil, err
	}
	return &OwnedOutput{
		o: o,
		t: tChan,
	}, nil
}

// Write a message to the output, or return an error either if delivery is not
// possible or the context is cancelled.
func (o *OwnedOutput) Write(ctx context.Context, m *Message) error {
	payload := message.Batch{m.part}

	resChan := make(chan error, 1)
	select {
	case o.t <- message.NewTransaction(payload, resChan):
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case res := <-resChan:
		return res
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WriteBatch attempts to write a message batch to the output, and returns an
// error either if delivery is not possible or the context is cancelled.
func (o *OwnedOutput) WriteBatch(ctx context.Context, b MessageBatch) error {
	payload := make(message.Batch, len(b))
	for i, m := range b {
		payload[i] = m.part
	}

	resChan := make(chan error, 1)
	select {
	case o.t <- message.NewTransaction(payload, resChan):
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case res := <-resChan:
		return toPublicBatchError(res)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close the output.
func (o *OwnedOutput) Close(ctx context.Context) error {
	o.closeOnce.Do(func() {
		close(o.t)
	})
	return o.o.WaitForClose(ctx)
}
