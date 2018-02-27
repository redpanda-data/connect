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

package types

//------------------------------------------------------------------------------

// Transaction is a type respesenting a transaction containing a payload (the
// message) and a response channel, which is used to indicate whether the
// message was successfully propagated to the next destination.
type Transaction struct {
	// Payload is the message payload of this transaction.
	Payload Message

	// ResponseChan should receive a response at the end of a transaction (once
	// the message is no longer owned by the receiver.) The response itself
	// indicates whether the message has been propagated successfully.
	ResponseChan chan<- Response
}

//------------------------------------------------------------------------------

// NewTransaction creates a new transaction object from a message payload and a
// response channel.
func NewTransaction(payload Message, resChan chan<- Response) Transaction {
	return Transaction{
		Payload:      payload,
		ResponseChan: resChan,
	}
}

//------------------------------------------------------------------------------
