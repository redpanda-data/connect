/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package impl

import "github.com/jeffail/benthos/lib/types"

//------------------------------------------------------------------------------

// Buffer - Represents a method of storing messages.
type Buffer interface {
	// ShiftMessage - Remove the oldest message from the stack. Returns the
	// backlog in bytes.
	ShiftMessage() (int, error)

	// NextMessage - Read the oldest message, the message is preserved until
	// ShiftMessage is called.
	NextMessage() (types.Message, error)

	// PushMessage - Add a new message to the stack. Returns the backlog in
	// bytes.
	PushMessage(types.Message) (int, error)

	// CloseOnceEmpty - Close the Buffer once the buffer has been emptied, this
	// is a way for a writer to signal to a reader that it is finished writing
	// messages, and therefore the reader can close once it is caught up. This
	// call blocks until the close is completed.
	CloseOnceEmpty()

	// Close - Close the Buffer so that blocked readers or writers become
	// unblocked.
	Close()
}

//------------------------------------------------------------------------------
