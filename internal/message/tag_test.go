package message

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestNestedTags(t *testing.T) {
	part := message.NewPart([]byte("foo"))
	tag1 := NewTag(0)
	tag2 := NewTag(0)
	tag3 := NewTag(0)

	part2 := WithTag(tag1, part)
	part2 = WithTag(tag2, part2)

	part3 := WithTag(tag1, part)
	part3 = WithTag(tag3, part3)

	part4 := WithTag(tag2, part3)

	assert.False(t, HasTag(tag1, part))
	assert.False(t, HasTag(tag2, part))
	assert.False(t, HasTag(tag3, part))

	assert.True(t, HasTag(tag1, part2))
	assert.True(t, HasTag(tag2, part2))
	assert.False(t, HasTag(tag3, part2))

	assert.True(t, HasTag(tag1, part3))
	assert.False(t, HasTag(tag2, part3))
	assert.True(t, HasTag(tag3, part3))

	assert.True(t, HasTag(tag1, part4))
	assert.True(t, HasTag(tag2, part4))
	assert.True(t, HasTag(tag3, part4))
}
