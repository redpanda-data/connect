package batch

import (
	"context"
	"sync"
)

// AckFunc is a common function signature for acknowledging receipt of messages.
type AckFunc func(context.Context, error) error

// CombinedAcker creates a single ack func closure that aggregates one or more
// derived closures such that only once each derived closure is called the
// singular ack func will trigger. If at least one derived closure receives an
// error the singular ack func will send the first non-nil error received.
type CombinedAcker struct {
	mut           sync.Mutex
	remainingAcks int
	err           error
	root          AckFunc
}

// NewCombinedAcker creates an aggregated that derives one or more ack funcs
// that, once all of which have been called, the provided root ack func is
// called.
func NewCombinedAcker(aFn AckFunc) *CombinedAcker {
	return &CombinedAcker{
		remainingAcks: 0,
		root:          aFn,
	}
}

// Derive creates a new ack func that must be called before the origin ack func
// will be called. It is invalid to derive an ack func after any other
// previously derived funcs have been called.
func (c *CombinedAcker) Derive() AckFunc {
	c.mut.Lock()
	c.remainingAcks++
	c.mut.Unlock()

	var decrementOnce sync.Once
	return func(ctx context.Context, ackErr error) (err error) {
		decrementOnce.Do(func() {
			c.mut.Lock()
			c.remainingAcks--
			remaining := c.remainingAcks
			if ackErr != nil {
				c.err = ackErr
			}
			ackErr = c.err
			c.mut.Unlock()
			if remaining == 0 {
				err = c.root(ctx, ackErr)
			}
		})
		return
	}
}
