package transaction

import (
	"context"
	"errors"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type tag struct {
	index int
}

type tagType *tag

// Tracked is a transaction type that adds identifying tags to messages such
// that an error returned resulting from multiple transaction messages can be
// reduced.
type Tracked struct {
	msg     types.Message
	tags    []tagType
	resChan chan<- types.Response
}

// NewTracked creates a transaction from a message batch and a response channel.
// The message is tagged with an identifier for the transaction, and if an error
// is returned from a downstream component that merged messages from other
// transactions the tag can be used in order to determine whether the message
// owned by this transaction succeeded.
func NewTracked(msg types.Message, resChan chan<- types.Response) *Tracked {
	tags := make([]tagType, msg.Len())
	taggedParts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tag := &tag{
			index: i,
		}
		tags[i] = tag
		taggedParts[i] = withTag(tag, p)
		return nil
	})
	trackedMsg := message.New(nil)
	trackedMsg.SetAll(taggedParts)
	return &Tracked{
		msg:     trackedMsg,
		resChan: resChan,
		tags:    tags,
	}
}

// Message returns the message owned by this transaction.
func (t *Tracked) Message() types.Message {
	return t.msg
}

// ResponseChan returns the response channel owned by this transaction.
func (t *Tracked) ResponseChan() chan<- types.Response {
	return t.resChan
}

type walkableError interface {
	WalkParts(fn func(int, types.Part, error) bool)
	error
}

func getResFromTags(tags []tagType, walkable walkableError) types.Response {
	remainingTags := make(map[tagType]struct{}, len(tags))
	for _, tag := range tags {
		remainingTags[tag] = struct{}{}
	}

	var res types.Response
	walkable.WalkParts(func(_ int, p types.Part, err error) bool {
		for tag := range remainingTags {
			if hasTag(p, tag) {
				if err != nil {
					res = response.NewError(err)
					return false
				}
				delete(remainingTags, tag)
				if len(remainingTags) == 0 {
					return false
				}
			}
		}
		return true
	})
	if res != nil {
		return res
	}

	if len(remainingTags) > 0 {
		return response.NewError(errors.Unwrap(walkable))
	}
	return response.NewAck()
}

func getResFromTag(tag tagType, walkable walkableError) types.Response {
	var res types.Response
	walkable.WalkParts(func(_ int, p types.Part, err error) bool {
		if hasTag(p, tag) {
			if err != nil {
				res = response.NewError(err)
			} else {
				res = response.NewAck()
			}
			return false
		}
		return true
	})
	if res != nil {
		return res
	}
	return response.NewError(errors.Unwrap(walkable))
}

func (t *Tracked) resFromError(err error) types.Response {
	var res types.Response = response.NewAck()
	if err != nil {
		if walkable, ok := err.(walkableError); ok {
			if len(t.tags) == 1 {
				res = getResFromTag(t.tags[0], walkable)
			} else {
				res = getResFromTags(t.tags, walkable)
			}
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

type tagListKeyType int

const tagListKey tagListKeyType = iota

type tagChecker interface {
	HasTag(t tagType) bool
}

type tagValue struct {
	tag      tagType
	previous tagChecker
}

func (t tagValue) HasTag(tag tagType) bool {
	if t.tag == tag {
		return true
	}
	if t.previous != nil {
		return t.previous.HasTag(tag)
	}
	return false
}

func hasTag(p types.Part, tag tagType) bool {
	ctx := message.GetContext(p)

	v, ok := ctx.Value(tagListKey).(tagChecker)
	if !ok {
		return false
	}

	return v.HasTag(tag)
}

func withTag(tag tagType, p types.Part) types.Part {
	ctx := message.GetContext(p)

	var prev tagChecker
	if v, ok := ctx.Value(tagListKey).(tagChecker); ok {
		prev = v
	}

	ctx = context.WithValue(ctx, tagListKey, tagValue{
		tag:      tag,
		previous: prev,
	})

	return message.WithContext(ctx, p)
}
