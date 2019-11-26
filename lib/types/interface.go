// Copyright (c) 2014 Ashley Jeffs
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

import (
	"net/http"
	"time"
)

//------------------------------------------------------------------------------

// Cache is a key/value store that can be shared across components and executing
// threads of a Benthos service.
type Cache interface {
	// Get attempts to locate and return a cached value by its key, returns an
	// error if the key does not exist or if the command fails.
	Get(key string) ([]byte, error)

	// Set attempts to set the value of a key, returns an error if the command
	// fails.
	Set(key string, value []byte) error

	// SetMulti attempts to set the value of multiple keys, returns an error if
	// any of the keys fail.
	SetMulti(items map[string][]byte) error

	// Add attempts to set the value of a key only if the key does not already
	// exist, returns an error if the key already exists or if the command
	// fails.
	Add(key string, value []byte) error

	// Delete attempts to remove a key. Returns an error if a failure occurs.
	Delete(key string) error

	Closable
}

//------------------------------------------------------------------------------

// RateLimit is a strategy for limiting access to a shared resource, this
// strategy can be safely used by components in parallel.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access() (time.Duration, error)

	Closable
}

//------------------------------------------------------------------------------

// Condition reads a message, calculates a condition and returns a boolean.
type Condition interface {
	// Check tests a message against a configured condition.
	Check(msg Message) bool
}

//------------------------------------------------------------------------------

// Processor reads a message, performs some form of data processing to the
// message, and returns either a slice of >= 1 resulting messages or a response
// to return to the message origin.
type Processor interface {
	// ProcessMessage attempts to process a message. Since processing can fail
	// this call returns both a slice of messages in case of success or a
	// response in case of failure. If the slice of messages is empty the
	// response will be returned to the source.
	ProcessMessage(Message) ([]Message, Response)

	Closable
}

//------------------------------------------------------------------------------

// Manager is an interface expected by Benthos components that allows them to
// register their service wide behaviours such as HTTP endpoints and event
// listeners, and obtain service wide shared resources such as caches.
type Manager interface {
	// RegisterEndpoint registers a server wide HTTP endpoint.
	RegisterEndpoint(path, desc string, h http.HandlerFunc)

	// GetCache attempts to find a service wide cache by its name.
	GetCache(name string) (Cache, error)

	// GetCondition attempts to find a service wide condition by its name.
	GetCondition(name string) (Condition, error)

	// GetProcessor attempts to find a service wide processor by its name.
	// TODO: V4 Add this
	// GetProcessor(name string) (Processor, error)

	// GetRateLimit attempts to find a service wide rate limit by its name.
	GetRateLimit(name string) (RateLimit, error)

	// GetPlugin attempts to find a service wide resource plugin by its name.
	GetPlugin(name string) (interface{}, error)

	// GetPipe attempts to find a service wide transaction chan by its name.
	GetPipe(name string) (<-chan Transaction, error)

	// SetPipe registers a transaction chan under a name.
	SetPipe(name string, t <-chan Transaction)

	// UnsetPipe removes a named transaction chan.
	UnsetPipe(name string, t <-chan Transaction)
}

//------------------------------------------------------------------------------

// Closable defines a type that can be safely closed down and cleaned up. This
// interface is required for many components within Benthos, but if your
// implementation is stateless and does not require shutting down then this
// interface can be implemented with empty shims.
type Closable interface {
	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

//------------------------------------------------------------------------------

// Producer is a type that sends messages as transactions and waits for a
// response back, the response indicates whether the message was successfully
// propagated to a new destination (and can be discarded from the source.)
type Producer interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan Transaction
}

// Consumer is the higher level consumer type.
type Consumer interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan Transaction) error
}

//------------------------------------------------------------------------------

// Output is a closable Consumer.
type Output interface {
	Consumer
	Closable

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool
}

// Input is a closable Producer.
type Input interface {
	Producer
	Closable

	// Connected returns a boolean indicating whether this input is currently
	// connected to its target.
	Connected() bool
}

// Pipeline is an interface that implements both the Consumer and Producer
// interfaces, and can therefore be used to pipe messages from Producer to a
// Consumer.
type Pipeline interface {
	Producer
	Consumer
	Closable
}

//------------------------------------------------------------------------------

// ProcessorConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom processor
// implementation.
type ProcessorConstructorFunc func() (Processor, error)

// PipelineConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom pipeline
// implementation.
//
// An integer pointer is provided to pipeline constructors that tracks the
// number of components spanning multiple pipelines. Each pipeline is expected
// to increment i by the number of components they contain, and may use the
// value for metric and logging namespacing.
type PipelineConstructorFunc func(i *int) (Pipeline, error)

//------------------------------------------------------------------------------

// Response is a response from an output, agent or broker that confirms the
// input of successful message receipt.
type Response interface {
	// Error returns a non-nil error if the message failed to reach its
	// destination.
	Error() error

	// SkipAck indicates that even though there may not have been an error in
	// processing a message, it should not be acknowledged. If SkipAck is false
	// and Error is nil then all unacknowledged messages should be acknowledged
	// also.
	//
	// TODO: V4 Remove this.
	SkipAck() bool
}

//------------------------------------------------------------------------------
