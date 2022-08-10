package input

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type asyncCutOffMsg struct {
	msg   message.Batch
	ackFn AsyncAckFn
}

// AsyncCutOff is a wrapper for input.Async implementations that exits from
// WaitForClose immediately. This is only useful when the underlying readable
// resource cannot be closed reliably and can block forever.
type AsyncCutOff struct {
	msgChan chan asyncCutOffMsg
	errChan chan error

	ctx   context.Context
	close func()

	r Async
}

// NewAsyncCutOff returns a new AsyncCutOff wrapper around a input.Async.
func NewAsyncCutOff(r Async) *AsyncCutOff {
	ctx, done := context.WithCancel(context.Background())
	return &AsyncCutOff{
		msgChan: make(chan asyncCutOffMsg),
		errChan: make(chan error),
		ctx:     ctx,
		close:   done,
		r:       r,
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (c *AsyncCutOff) Connect(ctx context.Context) error {
	return c.r.Connect(ctx)
}

// ReadBatch attempts to read a new message from the source.
func (c *AsyncCutOff) ReadBatch(ctx context.Context) (message.Batch, AsyncAckFn, error) {
	go func() {
		msg, ackFn, err := c.r.ReadBatch(ctx)
		if err == nil {
			select {
			case c.msgChan <- asyncCutOffMsg{
				msg:   msg,
				ackFn: ackFn,
			}:
			case <-ctx.Done():
			}
		} else {
			select {
			case c.errChan <- err:
			case <-ctx.Done():
			}
		}
	}()
	select {
	case m := <-c.msgChan:
		return m.msg, m.ackFn, nil
	case e := <-c.errChan:
		return nil, nil, e
	case <-ctx.Done():
		go func() {
			_ = c.r.Close(context.Background())
		}()
	case <-c.ctx.Done():
	}
	return nil, nil, component.ErrTypeClosed
}

// Close triggers the asynchronous closing of the reader.
func (c *AsyncCutOff) Close(ctx context.Context) error {
	go func() {
		_ = c.r.Close(context.Background())
	}()
	c.close()
	return nil
}
