package batch

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type tag struct {
	index int
}

type tagType *tag

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

func hasTag(p types.Part, tag tagType) bool {
	ctx := message.GetContext(p)

	v, ok := ctx.Value(tagKey).(tagChecker)
	if !ok {
		return false
	}

	return v.HasTag(tag)
}

func withTag(tag tagType, p types.Part) types.Part {
	ctx := message.GetContext(p)

	var prev tagChecker
	if v, ok := ctx.Value(tagKey).(tagChecker); ok {
		prev = v
	}

	ctx = context.WithValue(ctx, tagKey, tagValue{
		tag:      tag,
		previous: prev,
	})

	return message.WithContext(ctx, p)
}
