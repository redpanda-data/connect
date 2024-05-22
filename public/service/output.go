package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
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

func newAirGapWriter(w Output) output.AsyncSink {
	return &airGapWriter{w: w}
}

func (a *airGapWriter) Connect(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return publicToInternalErr(a.w.Write(ctx, NewInternalMessage(msg.Get(0))))
}

func (a *airGapWriter) Close(ctx context.Context) error {
	return a.w.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements output.AsyncSink.
type airGapBatchWriter struct {
	w BatchOutput
}

func newAirGapBatchWriter(w BatchOutput) output.AsyncSink {
	return &airGapBatchWriter{w: w}
}

func (a *airGapBatchWriter) Connect(ctx context.Context) error {
	return a.w.Connect(ctx)
}

func (a *airGapBatchWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	parts := make([]*Message, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		parts[i] = NewInternalMessage(part)
		return nil
	})
	return publicToInternalErr(a.w.WriteBatch(ctx, parts))
}

func (a *airGapBatchWriter) Close(ctx context.Context) error {
	return a.w.Close(context.Background())
}

//------------------------------------------------------------------------------

// ResourceOutput provides access to an output resource.
type ResourceOutput struct {
	o output.Sync
}

func newResourceOutput(o output.Sync) *ResourceOutput {
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
	o         output.Streamed
	closeOnce sync.Once
	t         atomic.Pointer[chan message.Transaction]
	primeMut  sync.Mutex
}

func newOwnedOutput(o output.Streamed) (*OwnedOutput, error) {
	return &OwnedOutput{
		o: o,
	}, nil
}

// BatchedWith returns a copy of the OwnedOutput where messages will be batched
// according to the provided batcher.
func (o *OwnedOutput) BatchedWith(b *Batcher) *OwnedOutput {
	return &OwnedOutput{
		o: batcher.New(b.p, o.o, b.mgr),
	}
}

// Prime attempts to establish the output connection ready for consuming data.
// This is done automatically once data is written. However, pre-emptively
// priming the connection before data is received is generally a better idea for
// short lived outputs as it'll speed up the first write.
func (o *OwnedOutput) Prime() error {
	o.primeMut.Lock()
	defer o.primeMut.Unlock()

	tChan := make(chan message.Transaction)
	if err := o.o.Consume(tChan); err != nil {
		return err
	}
	o.t.Store(&tChan)
	return nil
}

// PrimeBuffered performs the same output preparation as Prime but the internal
// transaction channel used for delivering data is buffered with the provided
// size. This allows for multiple write transactions to be written to the buffer
// and may improve the chance of delivery when using the WriteBatchNonBlocking
// method.
func (o *OwnedOutput) PrimeBuffered(n int) error {
	o.primeMut.Lock()
	defer o.primeMut.Unlock()

	tChan := make(chan message.Transaction, n)
	if err := o.o.Consume(tChan); err != nil {
		return err
	}
	o.t.Store(&tChan)
	return nil
}

func (o *OwnedOutput) getTChan() (chan message.Transaction, error) {
	if t := o.t.Load(); t != nil {
		return *t, nil
	}
	if err := o.Prime(); err != nil {
		return nil, err
	}
	return *o.t.Load(), nil
}

// Write a message to the output, or return an error either if delivery is not
// possible or the context is cancelled.
func (o *OwnedOutput) Write(ctx context.Context, m *Message) error {
	t, err := o.getTChan()
	if err != nil {
		return err
	}

	payload := message.Batch{m.part}

	resChan := make(chan error, 1)
	select {
	case t <- message.NewTransaction(payload, resChan):
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
	t, err := o.getTChan()
	if err != nil {
		return err
	}

	payload := make(message.Batch, len(b))
	for i, m := range b {
		payload[i] = m.part
	}

	resChan := make(chan error, 1)
	select {
	case t <- message.NewTransaction(payload, resChan):
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

// ErrBlockingWrite is returned when a non-blocking write is aborted
var ErrBlockingWrite = errors.New("a blocking write attempt was aborted")

// WriteBatchNonBlocking attempts to write a message batch to the output, but if
// the write is blocked (the read channel is full or not being listened to) then
// the write is aborted immediately in order to prevent blocking the caller.
//
// Instead of blocking until an acknowledgement of delivery is returned this
// method returns immediately and the provided acknowledgement function is
// called when appropriate.
//
// If the write is aborted then ErrBlockingWrite is returned. An error may also
// be returned if the output cannot be primed.
func (o *OwnedOutput) WriteBatchNonBlocking(b MessageBatch, aFn AckFunc) error {
	t, err := o.getTChan()
	if err != nil {
		return err
	}

	payload := make(message.Batch, len(b))
	for i, m := range b {
		payload[i] = m.part
	}

	select {
	case t <- message.NewTransactionFunc(payload, func(ctx context.Context, err error) error {
		err = toPublicBatchError(err)
		return aFn(ctx, err)
	}):
	default:
		return ErrBlockingWrite
	}
	return nil
}

// Close the output.
func (o *OwnedOutput) Close(ctx context.Context) error {
	o.closeOnce.Do(func() {
		if t := o.t.Load(); t != nil {
			close(*t)
		}
	})
	return o.o.WaitForClose(ctx)
}

type outputUnwrapper struct {
	o output.Streamed
}

func (w outputUnwrapper) Unwrap() output.Streamed {
	return w.o
}

// XUnwrapper is for internal use only, do not use this.
func (o *OwnedOutput) XUnwrapper() any {
	return outputUnwrapper{o: o.o}
}

//------------------------------------------------------------------------------

var docsAsync = `
This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ` + "`max_in_flight`" + `.`

var docsBatches = `
This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more [in this doc](/docs/configuration/batching).`

// OutputPerformanceDocs returns a string of markdown documentation that can be
// added to outputs as standard performance advice based on whether the output
// benefits from a max_in_flight field, batching or both.
func OutputPerformanceDocs(benefitsFromMaxInFlight, benefitsFromBatching bool) (content string) {
	if !benefitsFromMaxInFlight && !benefitsFromBatching {
		return
	}
	content += "\n\n## Performance"
	if benefitsFromMaxInFlight {
		content += "\n" + docsAsync
	}
	if benefitsFromBatching {
		content += "\n" + docsBatches
	}
	return content
}
