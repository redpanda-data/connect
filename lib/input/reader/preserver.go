// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Preserver is a wrapper for reader.Type implementations that keeps a buffer of
// sent messages until they are acknowledged. If an error occurs during message
// propagation the contents of the buffer will be resent instead of reading new
// messages until it is depleted. Preserver implements reader.Type.
type Preserver struct {
	unAckMessages  []types.Message
	resendMessages []types.Message

	throt *throttle.Type

	r Type
}

// NewPreserver returns a new Preserver wrapper around a reader.Type.
func NewPreserver(r Type) *Preserver {
	return &Preserver{
		r: r,
		throt: throttle.New(
			throttle.OptThrottlePeriod(time.Second),
		),
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if unsuccessful
// returns an error. If the attempt is successful (or not necessary) returns
// nil.
func (p *Preserver) Connect() error {
	return p.r.Connect()
}

// Acknowledge instructs whether messages read since the last Acknowledge call
// were successfully propagated. If the error is nil this will be forwarded to
// the underlying wrapped reader. If a non-nil error is returned the buffer of
// messages will be resent.
func (p *Preserver) Acknowledge(err error) error {
	if err == nil {
		p.throt.Reset()
		p.unAckMessages = nil
		if len(p.resendMessages) == 0 {
			// Only propagate ack if we are done resending buffered messages.
			return p.r.Acknowledge(err)
		}
		return nil
	}

	// Do not propagate errors since we are handling them here by resending.
	p.resendMessages = append(p.resendMessages, p.unAckMessages...)
	p.unAckMessages = nil
	p.throt.Retry()
	return nil
}

// Read attempts to read a new message from the source.
func (p *Preserver) Read() (types.Message, error) {
	// If we have messages queued to be resent we prioritise them over reading
	// new messages.
	if lMsgs := len(p.resendMessages); lMsgs > 0 {
		msg := p.resendMessages[0]
		if lMsgs > 1 {
			p.resendMessages = p.resendMessages[1:]
		} else {
			p.resendMessages = nil
		}
		p.unAckMessages = append(p.unAckMessages, msg)
		return msg, nil
	}
	msg, err := p.r.Read()
	if err == nil {
		p.unAckMessages = append(p.unAckMessages, msg)
	}
	return msg, err
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *Preserver) CloseAsync() {
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *Preserver) WaitForClose(tout time.Duration) error {
	return p.r.WaitForClose(tout)
}

//------------------------------------------------------------------------------
