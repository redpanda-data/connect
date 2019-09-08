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

// FailFlagKey is a metadata key used for flagging processor errors in Benthos.
// If a message part has any non-empty value for this metadata key then it will
// be interpretted as having failed a processor step somewhere in the pipeline.
var FailFlagKey = "benthos_processing_failed"

//------------------------------------------------------------------------------

// Metadata is an interface representing the metadata of a message part within
// a batch.
type Metadata interface {
	// Get returns a metadata value if a key exists, otherwise an empty string.
	Get(key string) string

	// Set sets the value of a metadata key.
	Set(key, value string) Metadata

	// Delete removes the value of a metadata key.
	Delete(key string) Metadata

	// Iter iterates each metadata key/value pair.
	Iter(f func(k, v string) error) error

	// Copy returns a copy of the metadata object that can be edited without
	// changing the contents of the original.
	Copy() Metadata
}

//------------------------------------------------------------------------------

// Part is an interface representing a message part. It contains a byte array
// of raw data, metadata, and lazily parsed formats of the payload such as JSON.
type Part interface {
	// Get returns a slice of bytes which is the underlying data of the part.
	// It is not safe to edit the contents of this slice directly, to make
	// changes to the contents of a part the data should be copied and set using
	// SetData.
	Get() []byte

	// Metadata returns the metadata of a part.
	Metadata() Metadata

	// GetContext returns the underlying context attached to this message part,
	// or context.Background if one has not already been attached.
	// TODO: V4 Add this.
	// GetContext() context.Context

	// WithContext returns the underlying message part with a new context
	// attached.
	// TODO: V4 Add this.
	// WithContext(ctx context.Context) Part

	// JSON attempts to parse the part as a JSON document and either returns the
	// result or an error. The resulting document is also cached such that
	// subsequent calls do not reparse the same data. If changes are made to the
	// document it must be set using SetJSON, otherwise the underlying byte
	// representation will not reflect the changes.
	JSON() (interface{}, error)

	// Set changes the underlying byte slice. Returns the edited message part
	// for daisy-chaining further calls.
	Set(d []byte) Part

	// SetMetadata changes the underlying metadata to a new object. Returns the
	// edited message part for daisy-chaining further calls.
	SetMetadata(m Metadata) Part

	// SetJSON attempts to marshal a JSON document into a byte slice and stores
	// the result as the new contents of the part. The document is cached such
	// that subsequent calls to JSON() receive it rather than reparsing the
	// resulting byte slice.
	SetJSON(doc interface{}) error

	// IsEmpty returns true if the message part has zero contents (not including
	// metadata).
	IsEmpty() bool

	// Copy creates a shallow copy of the message, where values and metadata can
	// be edited independently from the original version. However, editing the
	// byte slice contents will alter the contents of the original, and if
	// another process edits the bytes of the original it will also affect the
	// contents of this message.
	Copy() Part

	// DeepCopy creates a deep copy of the message part, where the contents are
	// copied and are therefore safe to edit without altering the original.
	DeepCopy() Part
}

//------------------------------------------------------------------------------

// Message is an interface representing a payload of data that was received from
// an input. Messages contain multiple parts, where each part is a byte array.
// If an input supports only single part messages they will still be read as
// multipart messages with one part. Multiple part messages are synonymous with
// batches, and it is up to each component within Benthos to work with a batch
// appropriately.
type Message interface {
	// Get attempts to access a message part from an index. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then an empty part is
	// returned.
	Get(p int) Part

	// SetAll replaces all parts of a message with a new set.
	SetAll(parts []Part)

	// Append appends new message parts to the message and returns the index of
	// last part to be added.
	Append(part ...Part) int

	// Len returns the number of parts this message contains.
	Len() int

	// Iter will iterate each message part in order, calling the closure
	// argument with the index and contents of the message part.
	Iter(f func(i int, part Part) error) error

	// Copy creates a shallow copy of the message, where the list of message
	// parts can be edited independently from the original version. However,
	// editing the byte array contents of a message part will alter the contents
	// of the original, and if another process edits the bytes of the original
	// it will also affect the contents of this message.
	Copy() Message

	// DeepCopy creates a deep copy of the message, where the message part
	// contents are entirely copied and are therefore safe to edit without
	// altering the original.
	DeepCopy() Message

	// CreatedAt returns the time at which the message was created.
	CreatedAt() time.Time
}

//------------------------------------------------------------------------------
