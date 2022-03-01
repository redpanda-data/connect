package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNestedSortGroups(t *testing.T) {
	msg := QuickBatch(nil)
	msg.Append(NewPart([]byte("first")))
	msg.Append(NewPart([]byte("second")))

	group1, msg1 := NewSortGroup(msg)

	assert.Equal(t, -1, group1.GetIndex(msg.Get(0)))
	assert.Equal(t, -1, group1.GetIndex(msg.Get(1)))

	assert.Equal(t, 0, group1.GetIndex(msg1.Get(0)))
	assert.Equal(t, 1, group1.GetIndex(msg1.Get(1)))

	msg1Reordered := QuickBatch(nil)
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
