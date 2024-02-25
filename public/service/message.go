package service

import (
	"context"
	"errors"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
	"github.com/benthosdev/benthos/v4/internal/value"
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
	part  *message.Part
	onErr func(err error)
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

// WalkWithBatchedErrors walks a batch and executes a closure function for each
// message. If the provided closure returns an error then iteration of the batch
// is not stopped and instead a *BatchError is created and populated.
//
// The one exception to this behaviour is when an error is returned that is
// considered fatal such as ErrNotConnected, in which case iteration is
// terminated early and that error is returned immediately.
//
// This is a useful pattern for batched outputs that deliver messages
// individually.
func (b MessageBatch) WalkWithBatchedErrors(fn func(int, *Message) error) error {
	if len(b) == 1 {
		return fn(0, b[0])
	}

	var batchErr *BatchError
	for i, m := range b {
		tmpErr := fn(i, m)
		if tmpErr != nil {
			if errors.Is(tmpErr, ErrNotConnected) {
				return tmpErr
			}
			if batchErr == nil {
				batchErr = NewBatchError(b, tmpErr)
			}
			_ = batchErr.Failed(i, tmpErr)
		}
	}

	if batchErr != nil {
		return batchErr
	}
	return nil
}

// Index mutates the batch in situ such that each message in the batch retains
// knowledge of where in the batch it currently resides. An indexer is then
// returned which can be used as a way of re-acquiring the original order of a
// batch derived from this one even after filtering, duplication and reordering
// has been done by other components.
//
// This can be useful in situations where a batch of messages is going to be
// mutated outside of the control of this component (by processors, for example)
// in ways that may change the ordering or presence of messages in the resulting
// batch. Having an indexer that we created prior to this processing allows us
// to take the resulting batch and join the messages within to the messages we
// started with.
func (b MessageBatch) Index() *Indexer {
	parts := make(message.Batch, len(b))
	for i, m := range b {
		parts[i] = m.part
	}

	var s *message.SortGroup
	s, parts = message.NewSortGroup(parts)

	for i, p := range parts {
		b[i].part = p
	}

	return &Indexer{
		wrapped:     s,
		sourceBatch: b.Copy(),
	}
}

// Indexer encapsulates the ability to acquire the original index of a message
// from a derivative batch as it was when the indexer was created. This can be
// useful in situations where a batch is being dispatched to processors or
// outputs and a derivative batch needs to be associated with the origin.
type Indexer struct {
	wrapped     *message.SortGroup
	sourceBatch MessageBatch
}

// IndexOf attempts to obtain the index of a message as it occurred within the
// origin batch known at the time the indexer was created. If the message is an
// orphan and does not originate from that batch then -1 is returned. It is
// possible that zero, one or more derivative messages yield any given index of
// the origin batch due to filtering and/or duplication enacted on the batch.
func (s *Indexer) IndexOf(m *Message) int {
	return s.wrapped.GetIndex(m.part)
}

// NewMessage creates a new message with an initial raw bytes content. The
// initial content can be nil, which is recommended if you intend to set it with
// structured contents.
func NewMessage(content []byte) *Message {
	return &Message{
		part: message.NewPart(content),
	}
}

// NewInternalMessage returns a message wrapped around an instantiation of the
// internal message package. This function is for internal use only and intended
// as a scaffold for internal components migrating to the new APIs.
func NewInternalMessage(imsg *message.Part) *Message {
	return &Message{part: imsg}
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
	if m.onErr != nil {
		m.onErr(err)
	}
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
	return value.IToString(v), true
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
		return NewInternalMessage(res), nil
	}
	return nil, nil
}

// BloblangQueryValue executes a parsed Bloblang mapping on a message and
// returns the raw value result, or an error if either the mapping fails.
// The error bloblang.ErrRootDeleted is returned if the root of the mapping
// value is deleted, this is in order to allow distinction between a real nil
// value and a deleted value.
func (m *Message) BloblangQueryValue(blobl *bloblang.Executor) (any, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := message.Batch{m.part}

	res, err := uw.Exec(query.FunctionContext{
		Maps:     uw.Maps(),
		Vars:     map[string]any{},
		Index:    0,
		MsgBatch: msg,
	})
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case value.Delete:
		return nil, bloblang.ErrRootDeleted
	case value.Nothing:
		return nil, nil
	}
	return res, nil
}

// BloblangMutate executes a parsed Bloblang mapping onto a message where the
// contents of the message are mutated directly rather than creating an entirely
// new object.
//
// Returns the same message back in a mutated form, or an error if the mapping
// fails. If the mapping results in the root being deleted the returned message
// will be nil, which indicates it has been filtered.
//
// Note that using a Mutate execution means certain functions within the
// Bloblang mapping will behave differently. In the root of the mapping the
// right-hand keywords `root` and `this` refer to the same mutable root of the
// output document.
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
		return NewInternalMessage(res), nil
	}
	return nil, nil
}

// BloblangMutateFrom executes a parsed Bloblang mapping onto a message where
// the reference material for the mapping comes from a provided message rather
// than the target message of the map. Contents of the target message are
// mutated directly rather than creating an entirely new object.
//
// Returns the same message back in a mutated form, or an error if the mapping
// fails. If the mapping results in the root being deleted the returned message
// will be nil, which indicates it has been filtered.
//
// Note that using a MutateFrom execution means certain functions within the
// Bloblang mapping will behave differently. In the root of the mapping the
// right-hand keyword `root` refers to the same mutable root of the output
// document, but the keyword `this` refers to the message being provided as an
// argument.
func (m *Message) BloblangMutateFrom(blobl *bloblang.Executor, from *Message) (*Message, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := message.Batch{from.part}

	res, err := uw.MapOnto(m.part, 0, msg)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return NewInternalMessage(res), nil
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
		return NewInternalMessage(res), nil
	}
	return nil, nil
}

// BloblangQueryValue executes a parsed Bloblang mapping on a message batch,
// from the perspective of a particular message index, and returns the raw value
// result or an error if the mapping fails. The error bloblang.ErrRootDeleted is
// returned if the root of the mapping value is deleted, this is in order to
// allow distinction between a real nil value and a deleted value.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
func (b MessageBatch) BloblangQueryValue(index int, blobl *bloblang.Executor) (any, error) {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}

	res, err := uw.Exec(query.FunctionContext{
		Maps:     uw.Maps(),
		Vars:     map[string]any{},
		Index:    index,
		MsgBatch: msg,
	})
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case value.Delete:
		return nil, bloblang.ErrRootDeleted
	case value.Nothing:
		return nil, nil
	}
	return res, nil
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
		return NewInternalMessage(res), nil
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

// AddSyncResponse attempts to add this batch of messages, in its exact current
// condition, to the synchronous response destined for the original source input
// of this data. Synchronous responses aren't supported by all inputs, and so
// it's possible that attempting to mark a batch as ready for a synchronous
// response will return an error.
func (b MessageBatch) AddSyncResponse() error {
	parts := make([]*message.Part, len(b))
	for i, m := range b {
		parts[i] = m.part
	}
	return transaction.SetAsResponse(parts)
}
