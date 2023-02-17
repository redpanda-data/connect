package checkpoint

import "sync"

// Uncapped keeps track of a sequence of pending checkpoint payloads, and as
// pending checkpoints are resolved it retains the latest fully resolved payload
// in the sequence where all prior sequence checkpoints are also resolved.
//
// Also keeps track of the logical size of the unresolved sequence, which allows
// for limiting the number of pending checkpoints.
type Uncapped[T any] struct {
	positionOffset int64
	checkpoint     *T

	poolOfT sync.Pool

	latest, earliest *node[T]
}

// NewUncapped returns a new check pointer implemented via a linked list.
func NewUncapped[T any]() *Uncapped[T] {
	return &Uncapped[T]{
		poolOfT: sync.Pool{
			New: func() any {
				return &node[T]{}
			},
		},
	}
}

// Track a new unresolved payload. This payload will be cached until it is
// marked as resolved. While it is cached no more recent payload will ever be
// committed.
//
// While the returned resolve funcs can be called from any goroutine, it
// is assumed that Track is called from a single goroutine.
func (t *Uncapped[T]) Track(payload T, batchSize int64) func() *T {
	newNode := t.poolOfT.Get().(*node[T])
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

	return func() *T {
		if newNode.prev != nil {
			newNode.prev.position = newNode.position
			newNode.prev.payload = newNode.payload
			newNode.prev.next = newNode.next
		} else {
			tmp := newNode.payload
			t.checkpoint = &tmp
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

		newNode.reset()
		t.poolOfT.Put(newNode)
		return t.checkpoint
	}
}

// Pending returns the gap between the earliest and latest unresolved messages.
func (t *Uncapped[T]) Pending() int64 {
	if t.latest == nil {
		return 0
	}
	return t.latest.position - t.positionOffset
}

// Highest returns the payload of the highest resolved checkpoint.
func (t *Uncapped[T]) Highest() *T {
	return t.checkpoint
}

type node[T any] struct {
	position   int64
	payload    T
	prev, next *node[T]
}

func (n *node[T]) reset() {
	n.position = 0
	var def T
	n.payload = def
	n.prev = nil
	n.next = nil
}
