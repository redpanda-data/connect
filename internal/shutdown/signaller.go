package shutdown

import (
	"context"
	"sync"
	"time"
)

const longTermWait = time.Hour * 24

// MaximumShutdownWait is a magic number determining the maximum length of time
// that a component should be willing to wait for a child to finish shutting
// down before it can give up and exit.
//
// This wait time is largely symbolic, if a component blocks for anything more
// than a few minutes then it has failed in its duty to gracefully terminate.
//
// However, it's still necessary for components to provide some measure of time
// that they're willing to wait for with the current mechanism (WaitForClose),
// therefore we provide a very large duration, and since this is a magic number
// I've defined it once and exposed as a function, allowing us to more easily
// identify these cases and refactor them in the future.
func MaximumShutdownWait() time.Duration {
	return longTermWait
}

// Signaller is a mechanism owned by components that support graceful
// shut down and is used as a way to signal from outside that any goroutines
// owned by the component should begin to close.
//
// Shutting down can happen in two tiers of urgency, the first is to terminate
// "at leisure", meaning if you're in the middle of something it's okay to do
// that first before terminating, but please do not commit to new work.
//
// The second tier is immediate, where you need to clean up resources and
// terminate as soon as possible, regardless of any tasks that you are currently
// attempting to finish.
//
// Finally, there is also a signal of having closed down, which is made by the
// component and can be used from outside to determine whether the component
// has finished terminating.
type Signaller struct {
	closeAtLeisureChan chan struct{}
	closeAtLeisureOnce sync.Once

	closeNowChan chan struct{}
	closeNowOnce sync.Once

	hasClosedChan chan struct{}
	hasClosedOnce sync.Once
}

// NewSignaller creates a new signaller.
func NewSignaller() *Signaller {
	return &Signaller{
		closeAtLeisureChan: make(chan struct{}),
		closeNowChan:       make(chan struct{}),
		hasClosedChan:      make(chan struct{}),
	}
}

// CloseAtLeisure signals to the owner of this Signaller that it should
// terminate at its own leisure, meaning it's okay to complete any tasks that
// are in progress but no new work should be started.
func (s *Signaller) CloseAtLeisure() {
	s.closeAtLeisureOnce.Do(func() {
		close(s.closeAtLeisureChan)
	})
}

// CloseNow signals to the owner of this Signaller that it should terminate
// right now regardless of any in progress tasks.
func (s *Signaller) CloseNow() {
	s.CloseAtLeisure()
	s.closeNowOnce.Do(func() {
		close(s.closeNowChan)
	})
}

// ShutdownComplete is a signal made by the component that it and all of its
// owned resources have terminated.
func (s *Signaller) ShutdownComplete() {
	s.hasClosedOnce.Do(func() {
		close(s.hasClosedChan)
	})
}

//------------------------------------------------------------------------------

// ShouldCloseAtLeisure returns true if the signaller has received the signal to
// shut down at leisure or immediately.
func (s *Signaller) ShouldCloseAtLeisure() bool {
	select {
	case <-s.CloseAtLeisureChan():
		return true
	default:
	}
	return false
}

// CloseAtLeisureChan returns a channel that will be closed when the signal to
// shut down either at leisure or immediately has been made.
func (s *Signaller) CloseAtLeisureChan() <-chan struct{} {
	return s.closeAtLeisureChan
}

// CloseAtLeisureCtx returns a context.Context that will be terminated when
// either the provided context is cancelled or the signal to shut down
// either at leisure or immediately has been made.
func (s *Signaller) CloseAtLeisureCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closeAtLeisureChan:
		}
		cancel()
	}()
	return ctx, cancel
}

// ShouldCloseNow returns true if the signaller has received the signal to shut
// down immediately.
func (s *Signaller) ShouldCloseNow() bool {
	select {
	case <-s.CloseNowChan():
		return true
	default:
	}
	return false
}

// CloseNowChan returns a channel that will be closed when the signal to shut
// down immediately has been made.
func (s *Signaller) CloseNowChan() <-chan struct{} {
	return s.closeNowChan
}

// CloseNowCtx returns a context.Context that will be terminated when either the
// provided context is cancelled or the signal to shut down immediately has been
// made.
func (s *Signaller) CloseNowCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closeNowChan:
		}
		cancel()
	}()
	return ctx, cancel
}

// HasClosed returns true if the signaller has received the signal that the
// component has terminated.
func (s *Signaller) HasClosed() bool {
	select {
	case <-s.HasClosedChan():
		return true
	default:
	}
	return false
}

// HasClosedChan returns a channel that will be closed when the signal that the
// component has terminated has been made.
func (s *Signaller) HasClosedChan() <-chan struct{} {
	return s.hasClosedChan
}

// HasClosedCtx returns a context.Context that will be cancelled when either the
// provided context is cancelled or the signal that the component has shut down
// has been made.
func (s *Signaller) HasClosedCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.hasClosedChan:
		}
		cancel()
	}()
	return ctx, cancel
}
