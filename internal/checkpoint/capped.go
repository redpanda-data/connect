package checkpoint

import (
	"context"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/component"
)

// Capped receives an ordered feed of integer based offsets being tracked, and
// an unordered feed of integer based offsets that are resolved, and is able to
// return the highest offset currently able to be committed such that an
// unresolved offset is never committed.
//
// If the number of unresolved tracked values meets a given cap the next attempt
// to track a value will be blocked until the next value is resolved.
//
// This component is safe to use concurrently across goroutines.
type Capped struct {
	t    *Type
	cap  int64
	cond *sync.Cond
}

// NewCapped returns a new capped checkpointer.
func NewCapped(capacity int64) *Capped {
	return &Capped{
		t:    New(),
		cap:  capacity,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Highest returns the current highest checkpoint.
func (c *Capped) Highest() interface{} {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	return c.t.Highest()
}

// Track a new unresolved integer offset. This offset will be cached until it is
// marked as resolved. While it is cached no higher valued offset will ever be
// committed. If the provided value is lower than an already provided value an
// error is returned.
func (c *Capped) Track(ctx context.Context, payload interface{}, batchSize int64) (func() interface{}, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		c.cond.L.Lock()
		c.cond.Broadcast()
		c.cond.L.Unlock()
	}()

	pending := c.t.Pending()
	for pending > 0 && pending+batchSize > c.cap {
		c.cond.Wait()
		select {
		case <-ctx.Done():
			return nil, component.ErrTimeout
		default:
		}
		pending = c.t.Pending()
	}

	resolveFn := c.t.Track(payload, batchSize)

	return func() interface{} {
		c.cond.L.Lock()
		defer c.cond.L.Unlock()

		highest := resolveFn()
		c.cond.Broadcast()
		return highest
	}, nil
}
