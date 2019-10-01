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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AsyncBundleUnacks is a wrapper for reader.Async implementations that,
// whenever an unack is given as a response to an async message, keeps the
// underlying ack function buffered. Once a non-unack response is received it is
// provided to all buffered ack functions.
//
// Unacks are only returned by the batch processor, and once it is removed this
// component can also be removed.
//
// TODO: V4 Remove this.
type AsyncBundleUnacks struct {
	pendingAcks []AsyncAckFn
	acksMut     sync.RWMutex

	r Async
}

// NewAsyncBundleUnacks returns a new AsyncBundleUnacks wrapper around a
// reader.Async.
func NewAsyncBundleUnacks(r Async) *AsyncBundleUnacks {
	return &AsyncBundleUnacks{
		r: r,
	}
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *AsyncBundleUnacks) ConnectWithContext(ctx context.Context) error {
	return p.r.ConnectWithContext(ctx)
}

func (p *AsyncBundleUnacks) wrapAckFn(ackFn AsyncAckFn) AsyncAckFn {
	p.acksMut.Lock()
	accumulatedAcks := p.pendingAcks
	p.pendingAcks = nil
	p.acksMut.Unlock()
	return func(ctx context.Context, res types.Response) error {
		if res.Error() == nil && res.SkipAck() {
			p.acksMut.Lock()
			p.pendingAcks = append(p.pendingAcks, accumulatedAcks...)
			p.pendingAcks = append(p.pendingAcks, ackFn)
			p.acksMut.Unlock()
			return nil
		}

		nPendingAcks := len(accumulatedAcks)
		if nPendingAcks == 0 {
			return ackFn(ctx, res)
		}

		errs := []error{}
		for _, aFn := range accumulatedAcks {
			if err := aFn(ctx, res); err != nil {
				errs = append(errs, err)
			}
		}
		if err := ackFn(ctx, res); err != nil {
			errs = append(errs, err)
		}
		if len(errs) == 1 {
			return errs[0]
		}
		if len(errs) > 0 {
			return fmt.Errorf("failed to send grouped acknowledgements: %s", errs)
		}
		return nil
	}
}

// ReadWithContext attempts to read a new message from the source.
func (p *AsyncBundleUnacks) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, aFn, err := p.r.ReadWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg, p.wrapAckFn(aFn), nil
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *AsyncBundleUnacks) CloseAsync() {
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *AsyncBundleUnacks) WaitForClose(tout time.Duration) error {
	return p.r.WaitForClose(tout)
}

//------------------------------------------------------------------------------
