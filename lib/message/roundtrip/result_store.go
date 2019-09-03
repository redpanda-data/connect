// Copyright (c) 2019 Ashley Jeffs
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

package roundtrip

import (
	"context"
	"errors"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ErrNoStore is an error returned by components attempting to write a message
// batch to a ResultStore but are unable to locate the store within the batch
// context.
var ErrNoStore = errors.New("result store not found within batch context")

// ResultStoreKeyType is the recommended type of a context key for adding
// ResultStores to a message context.
type ResultStoreKeyType int

// ResultStoreKey is the recommended key value for adding ResultStores to a
// message context.
const ResultStoreKey ResultStoreKeyType = iota

// ResultStore is a type designed to be propagated along with a message as a way
// for an output destination to store the final version of the message payload
// as it saw it.
//
// It is intended that this structure is placed within a message via an attached
// context, usually under the key 'result_store'.
type ResultStore interface {
	// Add a message to the store. The message will be deep copied and have its
	// context wiped before storing, and is therefore safe to add even when
	// ownership of the message is about to be yielded.
	Add(msg types.Message)

	// Get the stored slice of messages.
	Get() []types.Message

	// Clear any currently stored messages.
	Clear()
}

//------------------------------------------------------------------------------

type resultStoreImpl struct {
	payloads []types.Message
	sync.RWMutex
}

func (r *resultStoreImpl) Add(msg types.Message) {
	r.Lock()
	defer r.Unlock()
	strippedParts := make([]types.Part, msg.Len())
	msg.DeepCopy().Iter(func(i int, p types.Part) error {
		strippedParts[i] = message.WithContext(context.Background(), p)
		return nil
	})
	msg.SetAll(strippedParts)
	r.payloads = append(r.payloads, msg)
}

func (r *resultStoreImpl) Get() []types.Message {
	r.RLock()
	defer r.RUnlock()
	return r.payloads
}

func (r *resultStoreImpl) Clear() {
	r.Lock()
	r.payloads = nil
	r.Unlock()
}

//------------------------------------------------------------------------------

// NewResultStore returns an implementation of ResultStore.
func NewResultStore() ResultStore {
	return &resultStoreImpl{}
}

//------------------------------------------------------------------------------

// AddResultStore sets a result store within the context of the provided message
// that allows a roundtrip.Writer or any other component to propagate a
// resulting message back to the origin.
func AddResultStore(msg types.Message, store ResultStore) {
	parts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		ctx := message.GetContext(p)
		parts[i] = message.WithContext(context.WithValue(ctx, ResultStoreKey, store), p)
		return nil
	})
	msg.SetAll(parts)
}

// SetAsResponse takes a mutated message and stores it as a response message,
// this action fails if the message does not contain a valid ResultStore within
// its context.
func SetAsResponse(msg types.Message) error {
	ctx := message.GetContext(msg.Get(0))
	store, ok := ctx.Value(ResultStoreKey).(ResultStore)
	if !ok {
		return ErrNoStore
	}
	store.Add(msg)
	return nil
}

//------------------------------------------------------------------------------
