package reader

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// CutOff is a wrapper for reader.Type implementations that exits from
// WaitForClose immediately. This is only useful when the underlying readable
// resource cannot be closed reliably and can block forever.
type CutOff struct {
	msgChan   chan types.Message
	errChan   chan error
	closeChan chan struct{}

	r Type
}

// NewCutOff returns a new CutOff wrapper around a reader.Type.
func NewCutOff(r Type) *CutOff {
	return &CutOff{
		msgChan:   make(chan types.Message),
		errChan:   make(chan error),
		closeChan: make(chan struct{}),
		r:         r,
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if unsuccessful
// returns an error. If the attempt is successful (or not necessary) returns
// nil.
func (c *CutOff) Connect() error {
	return c.r.Connect()
}

// Acknowledge instructs whether messages read since the last Acknowledge call
// were successfully propagated. If the error is nil this will be forwarded to
// the underlying wrapped reader. If a non-nil error is returned the buffer of
// messages will be resent.
func (c *CutOff) Acknowledge(err error) error {
	return c.r.Acknowledge(err)
}

// Read attempts to read a new message from the source.
func (c *CutOff) Read() (types.Message, error) {
	go func() {
		msg, err := c.r.Read()
		if err == nil {
			c.msgChan <- msg
		} else {
			c.errChan <- err
		}
	}()
	select {
	case m := <-c.msgChan:
		return m, nil
	case e := <-c.errChan:
		return nil, e
	case <-c.closeChan:
	}
	return nil, types.ErrTypeClosed
}

// CloseAsync triggers the asynchronous closing of the reader.
func (c *CutOff) CloseAsync() {
	c.r.CloseAsync()
	close(c.closeChan)
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (c *CutOff) WaitForClose(tout time.Duration) error {
	return nil // We don't block here.
}

//------------------------------------------------------------------------------
