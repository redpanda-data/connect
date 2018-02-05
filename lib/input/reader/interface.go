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
	"github.com/Jeffail/benthos/lib/types"
)

// Type is a type that writes Benthos messages to a third party sink.
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
