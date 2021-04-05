package checkpoint

import (
	"context"
	"errors"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// ErrBatchSizeLargerThanCheckpointLimit is returned when a batch size is larger than the
// configured checkpoint_limit, indicating that the batch cannot be processed as configured.
var ErrBatchSizeLargerThanCheckpointLimit = errors.New("batch size larger than checkpoint limit")

// Type receives an ordered feed of payloads being tracked, and
// an unordered feed of payloads that are resolved, and is able to
// return the highest batch payload with no previous unresolved payloads.
//
// If the number of unresolved tracked values meets a given cap the next attempt
// to track a value will be blocked until the next value is resolved.
type Type struct {
	cap  int64
	cond *sync.Cond

	positionOffset int64
	checkpoint     interface{}

	latest, earliest *node
}

// New returns a new capped checkpointer. If the cap is 0, cap will not be enforced.
func New(cap int64) *Type {
	return &Type{
		cap:  cap,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Track a new unresolved payload. This payload will be cached until it is
// marked as resolved. While it is cached no more recent payload will ever be
// committed.
//
// While the returned resolve funcs can be called from any goroutine, it
// is assumed that Track is called from a single goroutine.
func (t *Type) Track(ctx context.Context, payload interface{}, batchSize int64) (resolve func() (highestResolved interface{}), err error) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	// Impossible for this batch to be tracked
	if t.cap > 0 && batchSize > t.cap {
		return nil, ErrBatchSizeLargerThanCheckpointLimit
	}

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		t.cond.L.Lock()
		t.cond.Broadcast()
		t.cond.L.Unlock()
	}()

	for t.cap > 0 && batchSize+t.pendingLocked() > t.cap {
		t.cond.Wait()
		select {
		case <-ctx.Done():
			return nil, types.ErrTimeout
		default:
		}
	}

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

	// resolver
	return func() (highest interface{}) {
		t.cond.L.Lock()
		defer t.cond.L.Unlock()

		if newNode.prev != nil {
			newNode.prev.position = newNode.position
			newNode.prev.payload = newNode.payload
			newNode.prev.next = newNode.next
		} else if newNode == t.earliest {
			t.checkpoint = newNode.payload
			t.earliest = newNode.next

			t.cond.Broadcast()

			if t.earliest != nil {
				t.positionOffset = newNode.position
			}
		}

		if newNode.next != nil {
			newNode.next.prev = newNode.prev
		} else if newNode == t.latest {
			t.latest = newNode.prev

			if t.latest == nil {
				t.positionOffset = 0
			}
		}

		putNode(newNode)

		return t.checkpoint
	}, nil
}

func (t *Type) Pending() int64 {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return t.pendingLocked()
}

func (t *Type) pendingLocked() int64 {
	if t.latest == nil {
		return 0
	}
	return t.latest.position - t.positionOffset
}

func (t *Type) Highest() interface{} {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return t.checkpoint
}

type node struct {
	position   int64
	payload    interface{}
	prev, next *node
}

var nodePool = &sync.Pool{
	New: func() interface{} {
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
