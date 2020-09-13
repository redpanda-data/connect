package message

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Tag is a struct that can be appended to a message via context in order to
// identify it (and derivatives) after processing.
type Tag struct {
	Index int
}

// NewTag creates a new tag to be associated with a message.
func NewTag(index int) *Tag {
	return &Tag{
		Index: index,
	}
}

// HasTag checks whether a message part has a particular tag associated with it.
func HasTag(t *Tag, p types.Part) bool {
	ctx := message.GetContext(p)

	v, ok := ctx.Value(tagKey).(tagChecker)
	if !ok {
		return false
	}

	return v.HasTag(t)
}

// WithTag returns a message with a tag attached to it via context. A message
// can have any number of tags.
func WithTag(t *Tag, p types.Part) types.Part {
	ctx := message.GetContext(p)

	var prev tagChecker
	if v, ok := ctx.Value(tagKey).(tagChecker); ok {
		prev = v
	}

	ctx = context.WithValue(ctx, tagKey, tagValue{
		tag:      t,
		previous: prev,
	})

	return message.WithContext(ctx, p)
}

//------------------------------------------------------------------------------

type tagType *Tag

type tagKeyType int

const tagKey tagKeyType = iota

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
