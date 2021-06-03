package service

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// MessageHandlerFunc is a function signature defining a component that consumes
// Benthos messages. An error must be returned if the context is cancelled, or
// if the message could not be delivered or processed.
type MessageHandlerFunc func(context.Context, *Message) error

// Message represents a single discrete message passing through a Benthos
// pipeline. It is safe to mutate the message via Set methods, but the
// underlying byte data should not be edited directly.
type Message struct {
	part       types.Part
	partCopied bool
}

// NewMessage creates a new message with an initial raw bytes content. The
// initial content can be nil, which is recommended if you intend to set it with
// structured contents.
func NewMessage(content []byte) *Message {
	return &Message{
		part:       message.NewPart(content),
		partCopied: true,
	}
}

func newMessageFromPart(part types.Part) *Message {
	return &Message{part, false}
}

// Copy creates a shallow copy of a message that is safe to mutate with Set
// methods without mutating the original. Both messages will share a context,
// and therefore a tracing ID, if one has been associated with them.
//
// Note that this does not perform a deep copy of the byte or structured
// contents of the message, and therefore it is not safe to perform inline
// mutations on those values without copying them.
func (m *Message) Copy() *Message {
	return &Message{
		part:       m.part.Copy(),
		partCopied: true,
	}
}

func (m *Message) ensureCopied() {
	if !m.partCopied {
		m.part = m.part.Copy()
		m.partCopied = true
	}
}

// AsBytes returns the underlying byte array contents of a message or, if the
// contents are a structured type, attempts to marshal the contents as a JSON
// document and returns either the byte array result or an error.
//
// It is NOT safe to mutate the contents of the returned slice.
func (m *Message) AsBytes() ([]byte, error) {
	// TODO: Escalate errors in marshalling once we're able.
	return m.part.Get(), nil
}

// AsStructured returns the underlying structured contents of a message or, if
// the contents are a byte array, attempts to parse the bytes contents as a JSON
// document and returns either the structured result or an error.
//
// It is NOT safe to mutate the contents of the returned value if it is a
// reference type (slice or map). In order to safely mutate the structured
// contents of a message use AsStructuredMut.
func (m *Message) AsStructured() (interface{}, error) {
	return m.part.JSON()
}

// AsStructuredMut returns the underlying structured contents of a message or,
// if the contents are a byte array, attempts to parse the bytes contents as a
// JSON document and returns either the structured result or an error.
//
// It is safe to mutate the contents of the returned value even if it is a
// reference type (slice or map), as the structured contents will be lazily deep
// cloned if it is still owned by an upstream component.
func (m *Message) AsStructuredMut() (interface{}, error) {
	// TODO: Use refactored APIs to determine if the contents are owned.
	v, err := m.part.JSON()
	if err != nil {
		return nil, err
	}
	return message.CopyJSON(v)
}

// SetBytes sets the underlying contents of the message as a byte slice.
func (m *Message) SetBytes(b []byte) {
	m.ensureCopied()
	m.part.Set(b)
}

// SetStructured sets the underlying contents of the message as a structured
// type. This structured value should be a scalar Go type, or either a
// map[string]interface{} or []interface{} containing the same types all the way
// through the hierarchy, this ensures that other processors are able to work
// with the contents and that they can be JSON marshalled when coerced into a
// byte array.
func (m *Message) SetStructured(i interface{}) {
	m.ensureCopied()
	m.part.SetJSON(i)
}

// MetaGet attempts to find a metadata key from the message and returns a string
// result and a boolean indicating whether it was found.
func (m *Message) MetaGet(key string) (string, bool) {
	v := m.part.Metadata().Get(key)
	return v, len(v) > 0
}

// MetaSet sets the value of a metadata key. If the value is an empty string the
// metadata key is deleted.
func (m *Message) MetaSet(key, value string) {
	m.ensureCopied()
	if value == "" {
		m.part.Metadata().Delete(key)
	} else {
		m.part.Metadata().Set(key, value)
	}
}

// MetaDelete removes a key from the message metadata.
func (m *Message) MetaDelete(key string) {
	m.ensureCopied()
	m.part.Metadata().Delete(key)
}

// MetaWalk iterates each metadata key/value pair and executes a provided
// closure on each iteration. To stop iterating, return an error from the
// closure. An error returned by the closure will be returned by this function.
func (m *Message) MetaWalk(fn func(string, string) error) error {
	return m.part.Metadata().Iter(fn)
}
