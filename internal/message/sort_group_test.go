package message

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestNestedSortGroups(t *testing.T) {
	msg := message.QuickBatch(nil)
	msg.Append(message.NewPart([]byte("first")))
	msg.Append(message.NewPart([]byte("second")))

	group1, msg1 := NewSortGroup(msg)

	assert.Equal(t, -1, group1.GetIndex(msg.Get(0)))
	assert.Equal(t, -1, group1.GetIndex(msg.Get(1)))

	assert.Equal(t, 0, group1.GetIndex(msg1.Get(0)))
	assert.Equal(t, 1, group1.GetIndex(msg1.Get(1)))

	msg1Reordered := message.QuickBatch(nil)
	msg1Reordered.Append(msg1.Get(1))
	msg1Reordered.Append(msg1.Get(0))

	group2, msg2 := NewSortGroup(msg1Reordered)

	assert.Equal(t, -1, group1.GetIndex(msg.Get(0)))
	assert.Equal(t, -1, group1.GetIndex(msg.Get(1)))

	assert.Equal(t, 0, group1.GetIndex(msg1.Get(0)))
	assert.Equal(t, 1, group1.GetIndex(msg1.Get(1)))

	assert.Equal(t, -1, group2.GetIndex(msg.Get(0)))
	assert.Equal(t, -1, group2.GetIndex(msg.Get(1)))

	assert.Equal(t, -1, group2.GetIndex(msg1.Get(0)))
	assert.Equal(t, -1, group2.GetIndex(msg1.Get(1)))

	assert.Equal(t, 0, group2.GetIndex(msg2.Get(0)))
	assert.Equal(t, 1, group2.GetIndex(msg2.Get(1)))

	assert.Equal(t, 1, group1.GetIndex(msg2.Get(0)))
	assert.Equal(t, 0, group1.GetIndex(msg2.Get(1)))
}
