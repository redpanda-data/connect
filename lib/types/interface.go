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

	// Add attempts to set the value of a key only if the key does not already
	// exist, returns an error if the key already exists or if the command
	// fails.
	Add(key string, value []byte) error

	// Delete attempts to remove a key. Returns an error if a failure occurs.
	Delete(key string) error
}

//------------------------------------------------------------------------------

// Condition reads a message, calculates a condition and returns a boolean.
type Condition interface {
	// Check tests a message against a configured condition.
	Check(msg Message) bool
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

	// GetPipe attempts to find a service wide transaction chan by its name.
	GetPipe(name string) (<-chan Transaction, error)

	// SetPipe registers a transaction chan under a name.
	SetPipe(name string, t <-chan Transaction)

	// UnsetPipe removes a named transaction chan.
	UnsetPipe(name string, t <-chan Transaction)
}

//------------------------------------------------------------------------------

// Closable defines a type that can be safely closed down and cleaned up.
type Closable interface {
	// CloseAsync triggers a closure of this object but does not block until
	// completion.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the object has finished
	// closing down and cleaning up resources.
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

// Output is a closable consumer.
type Output interface {
	Consumer
	Closable
}

// Input is a closable producer.
type Input interface {
	Producer
	Closable
}

//------------------------------------------------------------------------------

// Message is an interface representing a payload of data that was received from
// an input. Messages contain multiple parts, where each part is a byte array.
// If an input supports only single part messages they will still be read as
// multipart messages with one part.
type Message interface {
	// Get attempts to access a message part from an index. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then nil is returned. It is
	// not safe to edit the contents of a message directly.
	Get(p int) []byte

	// GetAll returns all message parts as a two-dimensional byte array. It is
	// NOT safe to edit the contents of the result.
	GetAll() [][]byte

	// Set edits the contents of an existing message part. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then nothing is done.
	Set(p int, b []byte)

	// SetAll replaces all parts of a message with a new set.
	SetAll(p [][]byte)

	// GetJSON returns a message part parsed as JSON into an `interface{}` type.
	// This is lazily evaluated and the result is cached. If multiple layers of
	// a pipeline extract the same part as JSON it will only be unmarshalled
	// once, unless the content of the part has changed. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1.
	GetJSON(p int) (interface{}, error)

	// SetJSON sets a message part to the marshalled bytes of a JSON object, but
	// also caches the object itself. If the JSON contents of a part is
	// subsequently queried it will receive the cached version as long as the
	// raw content has not changed. If the index is negative then the part is
	// found by counting backwards from the last part starting at -1.
	SetJSON(p int, jObj interface{}) error

	// Append appends new message parts to the message and returns the index of
	// last part to be added.
	Append(b ...[]byte) int

	// Len returns the number of parts this message contains.
	Len() int

	// Iter will iterate each message part in order, calling the closure
	// argument with the index and contents of the message part.
	Iter(f func(i int, b []byte) error) error

	// Bytes returns a binary representation of the message, which can be later
	// parsed back into a multipart message with `FromBytes`. The result of this
	// call can itself be the part of a new message, which is a useful way of
	// transporting multiple part messages across protocols that only support
	// single parts.
	Bytes() []byte

	// LazyCondition lazily evaluates conditions on the message by caching the
	// results as per a label to identify the condition. The cache of results is
	// cleared whenever the contents of the message is changed.
	LazyCondition(label string, cond Condition) bool

	// ShallowCopy creates a shallow copy of the message, where the list of
	// message parts can be edited independently from the original version.
	// However, editing the byte array contents of a message part will still
	// alter the contents of the original.
	ShallowCopy() Message

	// DeepCopy creates a deep copy of the message, where the message part
	// contents are entirely copied and are therefore safe to edit without
	// altering the original.
	DeepCopy() Message

	// CreatedAt returns the time at which the message was created.
	CreatedAt() time.Time
}

//------------------------------------------------------------------------------
