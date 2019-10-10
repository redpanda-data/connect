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

	"github.com/Jeffail/benthos/v3/lib/types"
)

// Type is a type that reads Benthos messages from an external source. If the
// source supports acknowledgements then it is the responsibility of Type
// implementations to ensure acknowledgements are not sent for consumed messages
// until a subsequent Acknowledge call contains a nil error.
type Type interface {
	// Connect attempts to establish a connection to the source, if unsuccessful
	// returns an error. If the attempt is successful (or not necessary) returns
	// nil.
	Connect() error

	// Acknowledge, if applicable to the source, should send acknowledgments for
	// (or commit) all unacknowledged (or uncommitted) messages that have thus
	// far been consumed. If the error is non-nil this means the message was
	// unsuccessfully propagated down the pipeline, in which case it is up to
	// the implementation to decide whether to simply retry uncommitted messages
	// that are buffered locally, or to send the error upstream.
	Acknowledge(err error) error

	// Read attempts to read a new message from the source.
	Read() (types.Message, error)

	types.Closable
}

// Sync is a type that reads Benthos messages from an external source. Messages
// can be read continuously, but acknowledgements must be made synchronously
// and apply for all read messages.
type Sync interface {
	// ConnectWithContext attempts to establish a connection to the source, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	ConnectWithContext(ctx context.Context) error

	// ReadNextWithContext attempts to read a new message from the source. If
	// successful a message is returned. Messages returned remain unacknowledged
	// until the next AcknowledgeWithContext call.
	ReadNextWithContext(ctx context.Context) (types.Message, error)

	// Acknowledge, if applicable to the source, should send acknowledgments for
	// (or commit) all unacknowledged (or uncommitted) messages that have thus
	// far been consumed. If the error is non-nil this means the message was
	// unsuccessfully propagated down the pipeline, in which case it is up to
	// the implementation to decide whether to simply retry uncommitted messages
	// that are buffered locally, or to send the error upstream.
	AcknowledgeWithContext(ctx context.Context, err error) error

	types.Closable
}

// AsyncAckFn is a function used to acknowledge receipt of a message batch. The
// provided response indicates whether the message batch was successfully
// delivered. Returns an error if the acknowledge was not propagated.
type AsyncAckFn func(context.Context, types.Response) error

var noopAsyncAckFn AsyncAckFn = func(context.Context, types.Response) error {
	return nil
}

// Async is a type that reads Benthos messages from an external source and
// allows acknowledgements for a message batch to be propagated asynchronously.
// If the source supports acknowledgements then it is the responsibility of Type
// implementations to ensure acknowledgements are not sent for consumed messages
// until a subsequent Acknowledge call contains a nil error.
type Async interface {
	// ConnectWithContext attempts to establish a connection to the source, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	ConnectWithContext(ctx context.Context) error

	// ReadWithContext attempts to read a new message from the source. If
	// successful a message is returned along with a function used to
	// acknowledge receipt of the returned message. It's safe to process the
	// returned message and read the next message asynchronously.
	ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error)

	types.Closable
}
