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
	"time"
)

//------------------------------------------------------------------------------

// Metadata is an interface representing the metadata of a message part within
// a batch.
type Metadata interface {
	// Get returns a metadata value if a key exists, otherwise an empty string.
	Get(key string) string

	// Set sets the value of a metadata key.
	Set(key, value string)

	// Delete removes the value of a metadata key.
	Delete(key string)

	// Iter iterates each metadata key/value pair.
	Iter(f func(k, v string) error) error

	// Copy returns a copy of the metadata object that can be edited without
	// changing the contents of the original.
	Copy() Metadata
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

	// SetAll replaces all parts of a message with a new set of payloads.
	SetAll(p [][]byte)

	// GetMetadata returns the metadata of a message part. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then an empty metadata
	// object is returned.
	GetMetadata(p int) Metadata

	// SetMetadata sets the metadata of message parts by their index. Multiple
	// indexes can be specified in order to set the same metadata values to each
	// part. If zero indexes are specified the metadata is set for all message
	// parts. If an index is negative then the part is found by counting
	// backwards from the last part starting at -1. If an index is out of bounds
	// then nothing is done.
	SetMetadata(m Metadata, p ...int)

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
	// However, editing the byte array contents of a message part will alter the
	// contents of the original, and if another process edits the bytes of the
	// original it will also affect the contents of this message.
	ShallowCopy() Message

	// DeepCopy creates a deep copy of the message, where the message part
	// contents are entirely copied and are therefore safe to edit without
	// altering the original.
	DeepCopy() Message

	// CreatedAt returns the time at which the message was created.
	CreatedAt() time.Time
}

//------------------------------------------------------------------------------
