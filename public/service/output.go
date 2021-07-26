package service

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
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

// Implements output.AsyncSink
type airGapWriter struct {
	w Output

	sig *shutdown.Signaller
}

func newAirGapWriter(w Output) output.AsyncSink {
	return &airGapWriter{w, shutdown.NewSignaller()}
}

func (a *airGapWriter) ConnectWithContext(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	err := a.w.Write(ctx, newMessageFromPart(msg.Get(0)))
	if err != nil && errors.Is(err, ErrNotConnected) {
		err = types.ErrNotConnected
	}
	return err
}

func (a *airGapWriter) CloseAsync() {
	go func() {
		if err := a.w.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *airGapWriter) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// Implements output.AsyncSink
type airGapBatchWriter struct {
	w BatchOutput

	sig *shutdown.Signaller
}

func newAirGapBatchWriter(w BatchOutput) output.AsyncSink {
	return &airGapBatchWriter{w, shutdown.NewSignaller()}
}

func (a *airGapBatchWriter) ConnectWithContext(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapBatchWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	parts := make([]*Message, msg.Len())
	_ = msg.Iter(func(i int, part types.Part) error {
		parts[i] = newMessageFromPart(part)
		return nil
	})
	err := a.w.WriteBatch(ctx, parts)
	if err != nil && errors.Is(err, ErrNotConnected) {
		err = types.ErrNotConnected
	}
	return err
}

func (a *airGapBatchWriter) CloseAsync() {
	go func() {
		if err := a.w.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *airGapBatchWriter) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}
