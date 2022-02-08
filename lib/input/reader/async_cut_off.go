package reader

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/message"
)

//------------------------------------------------------------------------------

type asyncCutOffMsg struct {
	msg   *message.Batch
	ackFn AsyncAckFn
}

// AsyncCutOff is a wrapper for reader.Async implementations that exits from
// WaitForClose immediately. This is only useful when the underlying readable
// resource cannot be closed reliably and can block forever.
type AsyncCutOff struct {
	msgChan chan asyncCutOffMsg
	errChan chan error
	ctx     context.Context
	close   func()

	r Async
}

// NewAsyncCutOff returns a new AsyncCutOff wrapper around a reader.Async.
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

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (c *AsyncCutOff) ConnectWithContext(ctx context.Context) error {
	return c.r.ConnectWithContext(ctx)
}

// ReadWithContext attempts to read a new message from the source.
func (c *AsyncCutOff) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	go func() {
		msg, ackFn, err := c.r.ReadWithContext(ctx)
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
		c.r.CloseAsync()
	case <-c.ctx.Done():
	}
	return nil, nil, component.ErrTypeClosed
}

// CloseAsync triggers the asynchronous closing of the reader.
func (c *AsyncCutOff) CloseAsync() {
	c.r.CloseAsync()
	c.close()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (c *AsyncCutOff) WaitForClose(tout time.Duration) error {
	return nil // We don't block here.
}

//------------------------------------------------------------------------------
