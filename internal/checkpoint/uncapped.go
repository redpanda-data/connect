package checkpoint

// Uncapped keeps track of a sequence of pending checkpoint payloads, and as
// pending checkpoints are resolved it retains the latest fully resolved payload
// in the sequence where all prior sequence checkpoints are also resolved.
//
// Also keeps track of the logical size of the unresolved sequence, which allows
// for limiting the number of pending checkpoints.
type Uncapped[T any] struct {
	positionOffset int64
	checkpoint     *T

	start, end *node[T]
}

// NewUncapped returns a new check pointer implemented via a linked list.
func NewUncapped[T any]() *Uncapped[T] {
	return &Uncapped[T]{}
}

// Track a new unresolved payload. This payload will be cached until it is
// marked as resolved. While it is cached no more recent payload will ever be
// committed.
func (t *Uncapped[T]) Track(payload T, batchSize int64) func() *T {
	newNode := &node[T]{
		payload:  payload,
		position: batchSize,
	}

	if t.start == nil {
		t.start = newNode
	}

	if t.end != nil {
		newNode.prev = t.end
		newNode.position += t.end.position
		t.end.next = newNode
	}

	t.end = newNode

	return func() *T {
		if newNode.prev != nil {
			newNode.prev.position = newNode.position
			newNode.prev.payload = newNode.payload
			newNode.prev.next = newNode.next
		} else {
			tmp := newNode.payload
			t.checkpoint = &tmp
			t.positionOffset = newNode.position
			t.start = newNode.next
		}

		if newNode.next != nil {
			newNode.next.prev = newNode.prev
		} else {
			t.end = newNode.prev
			if t.end == nil {
				t.positionOffset = 0
			}
		}
		return t.checkpoint
	}
}

// Pending returns the gap between the baseline and the end of our checkpoints.
func (t *Uncapped[T]) Pending() int64 {
	if t.end == nil {
		return 0
	}
	return t.end.position - t.positionOffset
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
