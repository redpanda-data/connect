package service

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// MessageHandlerFunc is a function signature defining a component that consumes
// Benthos messages. An error must be returned if the context is cancelled, or
// if the message could not be delivered or processed.
type MessageHandlerFunc func(context.Context, *Message) error

// MessageBatchHandlerFunc is a function signature defining a component that
// consumes Benthos message batches. An error must be returned if the context is
// cancelled, or if the messages could not be delivered or processed.
type MessageBatchHandlerFunc func(context.Context, MessageBatch) error

// Message represents a single discrete message passing through a Benthos
// pipeline. It is safe to mutate the message via Set methods, but the
// underlying byte data should not be edited directly.
type Message struct {
	part *message.Part
}

// MessageBatch describes a collection of one or more messages.
type MessageBatch []*Message

// Copy creates a new slice of the same messages, which can be modified without
// changing the contents of the original batch.
func (b MessageBatch) Copy() MessageBatch {
	bCopy := make(MessageBatch, len(b))
	for i, m := range b {
		bCopy[i] = m.Copy()
	}
	return bCopy
}

// DeepCopy creates a new slice of the same messages, which can be modified
// without changing the contents of the original batch and are unchanged from
// deep mutations performed on the source message.
//
// This is required in situations where a component wishes to retain a copy of a
// message batch beyond the boundaries of a process or write command. This is
// specifically required for buffer implementations that operate by keeping a
// reference to the message.
func (b MessageBatch) DeepCopy() MessageBatch {
	bCopy := make(MessageBatch, len(b))
	for i, m := range b {
		bCopy[i] = m.DeepCopy()
	}
	return bCopy
}

// NewMessage creates a new message with an initial raw bytes content. The
// initial content can be nil, which is recommended if you intend to set it with
// structured contents.
func NewMessage(content []byte) *Message {
	return &Message{
		part: message.NewPart(content),
	}
}

func newMessageFromPart(part *message.Part) *Message {
	return &Message{part}
}

// Copy creates a shallow copy of a message that is safe to mutate with Set
// methods without mutating the original. Both messages will share a context,
// and therefore a tracing ID, if one has been associated with them.
func (m *Message) Copy() *Message {
	return &Message{
		part: m.part.ShallowCopy(),
	}
}

// DeepCopy creates a deep copy of a message and its contents that is safe to
// mutate with Set methods without mutating the original, and mutations on the
// inner (deep) contents of the source message will not mutate the copy.
//
// This is required in situations where a component wishes to retain a copy of a
// message beyond the boundaries of a process or write command. This is
// specifically required for buffer implementations that operate by keeping a
// reference to the message.
func (m *Message) DeepCopy() *Message {
	return &Message{
		part: m.part.DeepCopy(),
	}
}

// Context returns a context associated with the message, or a background
// context in the absence of one.
func (m *Message) Context() context.Context {
	return message.GetContext(m.part)
}

// WithContext returns a new message with a provided context associated with it.
func (m *Message) WithContext(ctx context.Context) *Message {
	return &Message{
		part: message.WithContext(ctx, m.part),
	}
}

// AsBytes returns the underlying byte array contents of a message or, if the
// contents are a structured type, attempts to marshal the contents as a JSON
// document and returns either the byte array result or an error.
//
// It is NOT safe to mutate the contents of the returned slice.
func (m *Message) AsBytes() ([]byte, error) {
	// TODO: Escalate errors in marshalling once we're able.
	return m.part.AsBytes(), nil
}

// AsStructured returns the underlying structured contents of a message or, if
// the contents are a byte array, attempts to parse the bytes contents as a JSON
// document and returns either the structured result or an error.
//
// It is NOT safe to mutate the contents of the returned value if it is a
// reference type (slice or map). In order to safely mutate the structured
// contents of a message use AsStructuredMut.
func (m *Message) AsStructured() (any, error) {
	return m.part.AsStructured()
}

// AsStructuredMut returns the underlying structured contents of a message or,
// if the contents are a byte array, attempts to parse the bytes contents as a
// JSON document and returns either the structured result or an error.
//
// It is safe to mutate the contents of the returned value even if it is a
// reference type (slice or map), as the structured contents will be lazily deep
// cloned if it is still owned by an upstream component.
func (m *Message) AsStructuredMut() (any, error) {
	v, err := m.part.AsStructuredMut()
	if err != nil {
		return nil, err
	}
	return v, nil
}

// SetBytes sets the underlying contents of the message as a byte slice.
func (m *Message) SetBytes(b []byte) {
	m.part.SetBytes(b)
}

// SetStructured sets the underlying contents of the message as a structured
// type. This structured value should be a scalar Go type, or either a
// map[string]interface{} or []interface{} containing the same types all the way
// through the hierarchy, this ensures that other processors are able to work
// with the contents and that they can be JSON marshalled when coerced into a
// byte array.
//
// The provided structure is considered read-only, which means subsequent
// processors will need to fully clone the structure in order to perform
// mutations on the data.
func (m *Message) SetStructured(i any) {
	m.part.SetStructured(i)
}

// SetStructuredMut sets the underlying contents of the message as a structured
// type. This structured value should be a scalar Go type, or either a
// map[string]interface{} or []interface{} containing the same types all the way
// through the hierarchy, this ensures that other processors are able to work
// with the contents and that they can be JSON marshalled when coerced into a
// byte array.
//
// The provided structure is considered mutable, which means subsequent
// processors might mutate the structure without performing a deep copy.
func (m *Message) SetStructuredMut(i any) {
	m.part.SetStructuredMut(i)
}

// SetError marks the message as having failed a processing step and adds the
// error to it as context. Messages marked with errors can be handled using a
// range of methods outlined in https://www.benthos.dev/docs/configuration/error_handling.
func (m *Message) SetError(err error) {
	m.part.ErrorSet(err)
}

// GetError returns an error associated with a message, or nil if there isn't
// one. Messages marked with errors can be handled using a range of methods
// outlined in https://www.benthos.dev/docs/configuration/error_handling.
func (m *Message) GetError() error {
	return m.part.ErrorGet()
}

// MetaGet attempts to find a metadata key from the message and returns a string
// result and a boolean indicating whether it was found.
//
// Strong advice: Use MetaGetMut instead.
func (m *Message) MetaGet(key string) (string, bool) {
	v, exists := m.part.MetaGetMut(key)
	if !exists {
		return "", false
	}
	return query.IToString(v), true
}

// MetaGetMut attempts to find a metadata key from the message and returns the
// value if found, and a boolean indicating whether it was found. The value
// returned is mutable, and so it is safe to modify even though it may be a
// reference type such as a slice or map.
func (m *Message) MetaGetMut(key string) (any, bool) {
	v, exists := m.part.MetaGetMut(key)
	if !exists {
		return "", false
	}
	return v, true
}

// MetaSet sets the value of a metadata key. If the value is an empty string the
// metadata key is deleted.
//
// Strong advice: Use MetaSetMut instead.
func (m *Message) MetaSet(key, value string) {
	if value == "" {
		m.part.MetaDelete(key)
	} else {
		m.part.MetaSetMut(key, value)
	}
}

// MetaSetMut sets the value of a metadata key to any value. The value provided
// is stored as mutable, and therefore if it is a reference type such as a slice
// or map then it could be modified by a downstream component.
func (m *Message) MetaSetMut(key string, value any) {
	m.part.MetaSetMut(key, value)
}

// MetaDelete removes a key from the message metadata.
func (m *Message) MetaDelete(key string) {
	m.part.MetaDelete(key)
}

// MetaWalk iterates each metadata key/value pair and executes a provided
// closure on each iteration. To stop iterating, return an error from the
// closure. An error returned by the closure will be returned by this function.
//
// Strong advice: Use MetaWalkMut instead.
func (m *Message) MetaWalk(fn func(string, string) error) error {
	return m.part.MetaIterStr(fn)
}

// MetaWalkMut iterates each metadata key/value pair and executes a provided
// closure on each iteration. To stop iterating, return an error from the
// closure. An error returned by the closure will be returned by this function.
func (m *Message) MetaWalkMut(fn func(key string, value any) error) error {
	return m.part.MetaIterMut(fn)
}

//------------------------------------------------------------------------------

// BloblangQuery executes a parsed Bloblang mapping on a message and returns a
// message back or an error if the mapping fails. If the mapping results in the
// root being deleted the returned message will be nil, which indicates it has
// been filtered.
func (m *Message) BloblangQuery(blobl *bloblang.Executor) (*Message, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := message.Batch{m.part}

	res, err := uw.MapPart(0, msg)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return newMessageFromPart(res), nil
	}
	return nil, nil
}

// BloblangMutate executes a parsed Bloblang mapping onto a message where the
// contents of the message are mutated directly rather than creating an entirely
// new object.
//
// Returns the same message back in a mutated form, or an error if the mapping
// fails. If the mapping results in the root being deleted the returned message
// will be nil, which indicates it has been filtered.
//
// Note that using overlay means certain functions within the Bloblang mapping
// will behave differently. In the root of the mapping the right-hand keywords
// `root` and `this` refer to the same mutable root of the output document.
func (m *Message) BloblangMutate(blobl *bloblang.Executor) (*Message, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := message.Batch{m.part}

	res, err := uw.MapOnto(m.part, 0, msg)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return newMessageFromPart(res), nil
	}
	return nil, nil
}

// BloblangQuery executes a parsed Bloblang mapping on a message batch, from the
// perspective of a particular message index, and returns a message back or an
// error if the mapping fails. If the mapping results in the root being deleted
// the returned message will be nil, which indicates it has been filtered.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
func (b MessageBatch) BloblangQuery(index int, blobl *bloblang.Executor) (*Message, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}

	res, err := uw.MapPart(index, msg)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return newMessageFromPart(res), nil
	}
	return nil, nil
}

// BloblangMutate executes a parsed Bloblang mapping onto a message within the
// batch, where the contents of the message are mutated directly rather than
// creating an entirely new object.
//
// Returns the same message back in a mutated form, or an error if the mapping
// fails. If the mapping results in the root being deleted the returned message
// will be nil, which indicates it has been filtered.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
//
// Note that using overlay means certain functions within the Bloblang mapping
// will behave differently. In the root of the mapping the right-hand keywords
// `root` and `this` refer to the same mutable root of the output document.
func (b MessageBatch) BloblangMutate(index int, blobl *bloblang.Executor) (*Message, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}

	res, err := uw.MapOnto(b[index].part, index, msg)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return newMessageFromPart(res), nil
	}
	return nil, nil
}

// TryInterpolatedString resolves an interpolated string expression on a message
// batch, from the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
func (b MessageBatch) TryInterpolatedString(index int, i *InterpolatedString) (string, error) {
	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}
	return i.expr.String(index, msg)
}

// TryInterpolatedBytes resolves an interpolated string expression on a message
// batch, from the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
func (b MessageBatch) TryInterpolatedBytes(index int, i *InterpolatedString) ([]byte, error) {
	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}
	return i.expr.Bytes(index, msg)
}

// InterpolatedString resolves an interpolated string expression on a message
// batch, from the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
//
// Deprecated: Use TryInterpolatedString instead.
func (b MessageBatch) InterpolatedString(index int, i *InterpolatedString) string {
	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}
	s, _ := i.expr.String(index, msg)
	return s
}

// InterpolatedBytes resolves an interpolated string expression on a message
// batch, from the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
//
// Deprecated: Use TryInterpolatedBytes instead.
func (b MessageBatch) InterpolatedBytes(index int, i *InterpolatedString) []byte {
	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}
	bRes, _ := i.expr.Bytes(index, msg)
	return bRes
}
