package batch

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestCount(t *testing.T) {
	p1 := message.NewPart([]byte("foo bar"))

	p2 := WithCollapsedCount(p1, 2)
	p3 := WithCollapsedCount(p2, 3)
	p4 := WithCollapsedCount(p1, 4)

	assert.Equal(t, 1, CollapsedCount(p1))
	assert.Equal(t, 2, CollapsedCount(p2))
	assert.Equal(t, 4, CollapsedCount(p3))
	assert.Equal(t, 4, CollapsedCount(p4))
}

func TestMessageCount(t *testing.T) {
	m := message.QuickBatch([][]byte{
		[]byte("FOO"),
		[]byte("BAR"),
		[]byte("BAZ"),
	})

	assert.Equal(t, 3, MessageCollapsedCount(m))
}
