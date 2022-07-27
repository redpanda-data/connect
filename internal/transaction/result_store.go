package transaction

import (
	"context"
	"errors"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/message"
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
	Add(msg message.Batch)

	// Get the stored slice of messages.
	Get() []message.Batch

	// Clear any currently stored messages.
	Clear()
}

//------------------------------------------------------------------------------

type resultStoreImpl struct {
	payloads []message.Batch
	sync.RWMutex
}

func (r *resultStoreImpl) Add(msg message.Batch) {
	r.Lock()
	defer r.Unlock()

	newBatch := make(message.Batch, len(msg))
	for i, p := range msg {
		newBatch[i] = message.WithContext(context.Background(), p.DeepCopy())
	}
	r.payloads = append(r.payloads, newBatch)
}

func (r *resultStoreImpl) Get() []message.Batch {
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
func AddResultStore(msg message.Batch, store ResultStore) {
	for i, p := range msg {
		ctx := message.GetContext(p)
		msg[i] = message.WithContext(context.WithValue(ctx, ResultStoreKey, store), p)
	}
}

// SetAsResponse takes a mutated message and stores it as a response message,
// this action fails if the message does not contain a valid ResultStore within
// its context.
func SetAsResponse(msg message.Batch) error {
	ctx := message.GetContext(msg.Get(0))
	store, ok := ctx.Value(ResultStoreKey).(ResultStore)
	if !ok {
		return ErrNoStore
	}
	store.Add(msg)
	return nil
}

//------------------------------------------------------------------------------
