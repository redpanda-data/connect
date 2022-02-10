package util

import (
	"sort"
	"time"
)

// Closable defines a type that can be safely closed down and cleaned up. This
// interface is required for many components within Benthos, but if your
// implementation is stateless and does not require shutting down then this
// interface can be implemented with empty shims.
type Closable interface {
	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

//------------------------------------------------------------------------------

// ClosablePool keeps a reference to a pool of closable types and closes them in
// tiers.
type ClosablePool struct {
	closables map[int][]Closable
}

// NewClosablePool creates a fresh pool of closable types.
func NewClosablePool() *ClosablePool {
	return &ClosablePool{
		closables: make(map[int][]Closable),
	}
}

//------------------------------------------------------------------------------

// Add adds a closable type to the pool, tiers are used to partition and order
// the closing of types (starting at the lowest tier and working upwards).
// Closable types in a single tier are closed in the order that they are added.
func (c *ClosablePool) Add(tier int, closable Closable) {
	tierArray := []Closable{}
	if t, ok := c.closables[tier]; ok {
		tierArray = t
	}
	c.closables[tier] = append(tierArray, closable)
}

// Close attempts to close and clean up all stored closables in the determined
// order. If the timeout is met whilst working through the pool there is no
// indication of how far the pool has been progressed, thus this timeout should
// only be used for preventing severe blocking of a service.
func (c *ClosablePool) Close(timeout time.Duration) error {
	started := time.Now()

	tiers := []int{}
	for i := range c.closables {
		tiers = append(tiers, i)
	}
	sort.Ints(tiers)

	for _, i := range tiers {
		tier := c.closables[i]
		for j := range tier {
			tier[j].CloseAsync()
		}
		for j := range tier {
			if err := tier[j].WaitForClose(timeout - time.Since(started)); err != nil {
				return err
			}
		}
		delete(c.closables, i)
	}
	return nil
}
