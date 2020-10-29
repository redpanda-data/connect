package checkpoint

import (
	"context"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/checkpoint"
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
	t    *checkpoint.Type
	cap  int
	cond *sync.Cond
}

// NewCapped returns a new capped checkpointer.
func NewCapped(cap int) *Capped {
	return &Capped{
		t:    checkpoint.New(0),
		cap:  cap,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Highest returns the current highest checkpoint.
func (c *Capped) Highest() int {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	return c.t.Highest()
}

// Track a new unresolved integer offset. This offset will be cached until it is
// marked as resolved. While it is cached no higher valued offset will ever be
// committed. If the provided value is lower than an already provided value an
// error is returned.
func (c *Capped) Track(ctx context.Context, i int) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		c.cond.Broadcast()
	}()

	for (i - c.t.Highest()) >= c.cap {
		c.cond.Wait()
		select {
		case <-ctx.Done():
			return types.ErrTimeout
		default:
		}
	}

	return c.t.Track(i)
}

// Resolve a tracked offset by allowing it to be committed. The highest possible
// offset to be committed is returned, or an error if the provided offset was
// not recognised.
func (c *Capped) Resolve(offset int) (int, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	n, err := c.t.Resolve(offset)
	if err == nil {
		c.cond.Broadcast()
	}
	return n, err
}
