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

package types

import (
	"time"
)

//--------------------------------------------------------------------------------------------------

// Closable - Defines a type that can be safely closed down and cleaned up.
type Closable interface {
	// CloseAsync - Trigger a closure of this object but do not block until completion.
	CloseAsync()

	// WaitForClose - A blocking call to wait until the object has finished closing down and
	// cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

//--------------------------------------------------------------------------------------------------

// Responder - Defines a type that will send a response every time a message is received.
type Responder interface {
	// ResponseChan - Returns a response for every input message received.
	ResponseChan() <-chan Response
}

// ResponderListener - A type that listens to a Responder type.
type ResponderListener interface {
	// StartListening - Starts the type listening to a channel.
	StartListening(<-chan Response) error
}

//--------------------------------------------------------------------------------------------------

// MessageSender - A type that sends messages to an output.
type MessageSender interface {
	// MessageChan - Returns the channel used for consuming messages from this input.
	MessageChan() <-chan Message
}

// MessageReceiver - A type that receives messages from an input.
type MessageReceiver interface {
	// StartReceiving - Starts the type receiving messages from a channel.
	StartReceiving(<-chan Message) error
}

//--------------------------------------------------------------------------------------------------

// Producer - The higher level producer type.
type Producer interface {
	MessageSender
	ResponderListener
}

// Consumer - The higher level consumer type.
type Consumer interface {
	MessageReceiver
	Responder
}

//--------------------------------------------------------------------------------------------------
