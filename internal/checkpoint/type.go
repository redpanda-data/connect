package checkpoint

import (
	"sync"
)

// Type keeps track of a sequence of pending checkpoint payloads, and as pending
// checkpoints are resolved it retains the latest fully resolved payload in the
// sequence where all prior sequence checkpoints are also resolved.
//
// Also keeps track of the logical size of the unresolved sequence, which allows
// for limiting the number of pending checkpoints.
type Type struct {
	positionOffset int64
	checkpoint     any

	latest, earliest *node
}

// New returns a new checkpointer.
func New() *Type {
	return &Type{}
}

// Track a new unresolved payload. This payload will be cached until it is
// marked as resolved. While it is cached no more recent payload will ever be
// committed.
//
// While the returned resolve funcs can be called from any goroutine, it
// is assumed that Track is called from a single goroutine.
func (t *Type) Track(payload any, batchSize int64) func() any {
	newNode := getNode()
	newNode.payload = payload
	newNode.position = batchSize

	if t.earliest == nil {
		t.earliest = newNode
	}

	if t.latest != nil {
		newNode.prev = t.latest
		newNode.position += t.latest.position
		t.latest.next = newNode
	}

	t.latest = newNode

	return func() any {
		if newNode.prev != nil {
			newNode.prev.position = newNode.position
			newNode.prev.payload = newNode.payload
			newNode.prev.next = newNode.next
		} else {
			t.checkpoint = newNode.payload
			t.positionOffset = newNode.position
			t.earliest = newNode.next
		}

		if newNode.next != nil {
			newNode.next.prev = newNode.prev
		} else {
			t.latest = newNode.prev
			if t.latest == nil {
				t.positionOffset = 0
			}
		}

		putNode(newNode)
		return t.checkpoint
	}
}

// Pending returns the gap between the earliest and latests unresolved messages.
func (t *Type) Pending() int64 {
	if t.latest == nil {
		return 0
	}
	return t.latest.position - t.positionOffset
}

// Highest returns the payload of the highest resolved checkpoint.
func (t *Type) Highest() any {
	return t.checkpoint
}

type node struct {
	position   int64
	payload    any
	prev, next *node
}

var nodePool = &sync.Pool{
	New: func() any {
		return &node{}
	},
}

func getNode() *node {
	return nodePool.Get().(*node)
}

func putNode(node *node) {
	node.position = 0
	node.payload = nil
	node.prev = nil
	node.next = nil
	nodePool.Put(node)
}
