package reader

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type asyncPreserverResend struct {
	msg   types.Message
	ackFn AsyncAckFn
}

// AsyncPreserver is a wrapper for reader.Async implementations that keeps a
// buffer of sent messages until they are acknowledged. If an error occurs
// during message propagation the contents of the buffer will be resent instead
// of reading new messages until it is depleted. AsyncPreserver implements
// reader.Async.
//
// Wrapping an input with this type is useful when your source of messages
// doesn't have a concept of a NoAck (like Kafka), and instead of "rejecting"
// messages we always intend to simply retry them until success.
type AsyncPreserver struct {
	resendMessages  []asyncPreserverResend
	resendInterrupt func()
	msgsMut         sync.Mutex

	r Async
}

// NewAsyncPreserver returns a new AsyncPreserver wrapper around a reader.Async.
func NewAsyncPreserver(r Async) *AsyncPreserver {
	return &AsyncPreserver{
		r:               r,
		resendInterrupt: func() {},
	}
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *AsyncPreserver) ConnectWithContext(ctx context.Context) error {
	return p.r.ConnectWithContext(ctx)
}

func (p *AsyncPreserver) wrapAckFn(msg types.Message, ackFn AsyncAckFn) AsyncAckFn {
	return func(ctx context.Context, res types.Response) error {
		if res.Error() != nil {
			resendMsg := msg
			walkable, ok := res.Error().(batch.WalkableError)
			if ok && msg.Len() > 1 {
				tmpMsg := message.New(nil)
				walkable.WalkParts(func(i int, p types.Part, e error) bool {
					if e != nil {
						tmpMsg.Append(msg.Get(i))
					}
					return true
				})
				if tmpMsg.Len() > 0 {
					resendMsg = tmpMsg
				}
			}
			p.msgsMut.Lock()
			p.resendMessages = append(p.resendMessages, asyncPreserverResend{
				msg:   resendMsg,
				ackFn: ackFn,
			})
			p.resendInterrupt()
			p.msgsMut.Unlock()
			return nil
		}
		return ackFn(ctx, res)
	}
}

// ReadWithContext attempts to read a new message from the source.
func (p *AsyncPreserver) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	// If we have messages queued to be resent we prioritise them over reading
	// new messages.
	p.msgsMut.Lock()
	if lMsgs := len(p.resendMessages); lMsgs > 0 {
		resend := p.resendMessages[0]
		if lMsgs > 1 {
			p.resendMessages = p.resendMessages[1:]
		} else {
			p.resendMessages = nil
		}
		p.msgsMut.Unlock()
		return resend.msg, p.wrapAckFn(resend.msg, resend.ackFn), nil
	}
	p.resendInterrupt = cancel
	p.msgsMut.Unlock()

	msg, aFn, err := p.r.ReadWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg, p.wrapAckFn(msg, aFn), nil
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *AsyncPreserver) CloseAsync() {
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *AsyncPreserver) WaitForClose(tout time.Duration) error {
	return p.r.WaitForClose(tout)
}

//------------------------------------------------------------------------------
