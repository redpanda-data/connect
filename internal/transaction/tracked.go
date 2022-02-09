package transaction

import (
	"context"
	"errors"

	"github.com/Jeffail/benthos/v3/internal/batch"
	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
)

// Tracked is a transaction type that adds identifying tags to messages such
// that an error returned resulting from multiple transaction messages can be
// reduced.
type Tracked struct {
	msg     *message.Batch
	group   *imessage.SortGroup
	resChan chan<- response.Error
}

// NewTracked creates a transaction from a message batch and a response channel.
// The message is tagged with an identifier for the transaction, and if an error
// is returned from a downstream component that merged messages from other
// transactions the tag can be used in order to determine whether the message
// owned by this transaction succeeded.
func NewTracked(msg *message.Batch, resChan chan<- response.Error) *Tracked {
	group, trackedMsg := imessage.NewSortGroup(msg)
	return &Tracked{
		msg:     trackedMsg,
		resChan: resChan,
		group:   group,
	}
}

// Message returns the message owned by this transaction.
func (t *Tracked) Message() *message.Batch {
	return t.msg
}

// ResponseChan returns the response channel owned by this transaction.
func (t *Tracked) ResponseChan() chan<- response.Error {
	return t.resChan
}

func (t *Tracked) getResFromGroup(walkable batch.WalkableError) response.Error {
	remainingIndexes := make(map[int]struct{}, t.msg.Len())
	for i := 0; i < t.msg.Len(); i++ {
		remainingIndexes[i] = struct{}{}
	}

	var res response.Error
	walkable.WalkParts(func(_ int, p *message.Part, err error) bool {
		if index := t.group.GetIndex(p); index >= 0 {
			if err != nil {
				res = response.NewError(err)
				return false
			}
			delete(remainingIndexes, index)
			if len(remainingIndexes) == 0 {
				return false
			}
		}
		return true
	})
	if res.AckError() != nil {
		return res
	}

	if len(remainingIndexes) > 0 {
		return response.NewError(errors.Unwrap(walkable))
	}
	return response.NewError(nil)
}

func (t *Tracked) resFromError(err error) response.Error {
	res := response.NewError(nil)
	if err != nil {
		if walkable, ok := err.(batch.WalkableError); ok {
			res = t.getResFromGroup(walkable)
		} else {
			res = response.NewError(err)
		}
	}
	return res
}

// Ack provides a response to the upstream service from an error.
func (t *Tracked) Ack(ctx context.Context, err error) error {
	select {
	case t.resChan <- t.resFromError(err):
	case <-ctx.Done():
		return context.Canceled
	}
	return nil
}

//------------------------------------------------------------------------------
