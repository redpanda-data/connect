package common

import (
	"context"
	"fmt"
	"sync"
)

// Stoppable represents a resource (a Benthos stream) that can be stopped.
type Stoppable interface {
	Stop(ctx context.Context) error
}

// CombineStoppables returns a single Stoppable that will call each provided
// Stoppable in the order they are specified on a Stop. If any stoppable returns
// an error all subsequent stoppables will still be called before an error is
// returned.
func CombineStoppables(stoppables ...Stoppable) Stoppable {
	return &combinedStoppables{
		stoppables: stoppables,
	}
}

type combinedStoppables struct {
	stoppables []Stoppable
}

func (c *combinedStoppables) Stop(ctx context.Context) (stopErr error) {
	for _, s := range c.stoppables {
		if err := s.Stop(ctx); err != nil && stopErr == nil {
			stopErr = err
		}
	}
	return
}

// SwappableStopper wraps an active Stoppable resource in a mechanism that
// allows changing the resource for something else after stopping it.
type SwappableStopper struct {
	stopped bool
	current Stoppable
	mut     sync.Mutex
}

// NewSwappableStopper creates a new swappable stopper resource around an
// initial stoppable.
func NewSwappableStopper(s Stoppable) *SwappableStopper {
	return &SwappableStopper{
		current: s,
	}
}

// Stop the wrapped resource.
func (s *SwappableStopper) Stop(ctx context.Context) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		return nil
	}

	s.stopped = true
	return s.current.Stop(ctx)
}

// Replace the resource with something new only once the existing one is
// stopped. In order to avoid unnecessary start up of the swapping resource we
// accept a closure that constructs it and is only called when we're ready.
func (s *SwappableStopper) Replace(ctx context.Context, fn func() (Stoppable, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		// If the outer stream has been stopped then do not create a new one.
		return nil
	}

	if err := s.current.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop active stream: %w", err)
	}

	newStoppable, err := fn()
	if err != nil {
		return fmt.Errorf("failed to init updated stream: %w", err)
	}

	s.current = newStoppable
	return nil
}
