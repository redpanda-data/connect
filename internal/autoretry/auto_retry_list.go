package autoretry

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// AckFunc is a synchronous function that matches the standard acknowledgment
// signature in Benthos.
type AckFunc func(context.Context, error) error

// MutatorFunc is an optional closure used to mutate a T about to be scheduled
// for retry based on the returned error. This is useful for reducing a batch
// based on a batch error, etc.
type MutatorFunc[T any] func(t T, err error) T

type pendingT[T any] struct {
	t        T
	aFn      AckFunc
	boff     backoff.BackOff
	attempts int
}

// List contains a slice of items that are pending an acknowledgement, once
// items are added it's required that all rejected adopted T values are
// recirculated either via TryShift (non-blocking) or Shift.
type List[T any] struct {
	pendingRetry []*pendingT[T]
	mutator      MutatorFunc[T]
	inFlight     int
	cond         sync.Cond
}

// NewList returns a new list of Ts requiring automatic retries.
func NewList[T any](mutator MutatorFunc[T]) *List[T] {
	if mutator == nil {
		mutator = func(t T, err error) T { return t }
	}
	return &List[T]{
		cond:    *sync.NewCond(&sync.Mutex{}),
		mutator: mutator,
	}
}

// Adopt a T and its acknowledgement function so that a rejected T is added to
// retry list. Returns a new acknowledgment function that should be propagated
// as it encapsulates the retry logic.
func (l *List[T]) Adopt(ctx context.Context, t T, aFn AckFunc) AckFunc {
	l.cond.L.Lock()
	l.inFlight++
	l.cond.L.Unlock()

	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond
	boff.MaxInterval = time.Second
	boff.Multiplier = 1.1
	boff.MaxElapsedTime = 0

	return l.wrapPendingAck(&pendingT[T]{
		t:        t,
		aFn:      aFn,
		attempts: 0,
		boff:     boff,
	})
}

func (l *List[T]) wrapPendingAck(t *pendingT[T]) AckFunc {
	return func(ctx context.Context, err error) error {
		l.cond.L.Lock()
		defer func() {
			// Either outcome is worth broadcasting.
			l.cond.Broadcast()
			l.cond.L.Unlock()
		}()

		if err != nil {
			t.t = l.mutator(t.t, err)
			l.pendingRetry = append(l.pendingRetry, t)
			return nil
		}
		l.inFlight--
		return t.aFn(ctx, nil)
	}
}

// TryShift only yields a rejected T when there is one ready, otherwise nothing
// is returned. This is useful as a probe before each time the input stream is
// polled as these rejected messages should have priority.
func (l *List[T]) TryShift(ctx context.Context) (t T, fn AckFunc, ok bool) {
	return l.tryShift(ctx, true)
}

func (l *List[T]) tryShift(ctx context.Context, needsLock bool) (t T, fn AckFunc, ok bool) {
	var resend *pendingT[T]
	func() {
		if needsLock {
			l.cond.L.Lock()
			defer l.cond.L.Unlock()
		}

		lPending := len(l.pendingRetry)
		if lPending == 0 {
			return
		}

		resend = l.pendingRetry[0]
		if lPending > 1 {
			l.pendingRetry = l.pendingRetry[1:]
		} else {
			l.pendingRetry = nil
		}
	}()

	if resend == nil {
		return
	}

	resend.attempts++
	if resend.attempts > 2 {
		// This sleep prevents a busy loop on permanently failed messages.
		if tout := resend.boff.NextBackOff(); tout > 0 {
			select {
			case <-time.After(tout):
			case <-ctx.Done():
				return
			}
		}
	}
	return resend.t, l.wrapPendingAck(resend), true
}

// Exhausted returns true if all adopted Ts have been acknowledged.
func (l *List[T]) Exhausted() bool {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()
	return l.inFlight == 0 && len(l.pendingRetry) == 0
}

// Shift blocks until either a rejected T is ready to be consumed, the context
// is cancelled, or all adopted T values have been acknowledged and therefore
// there is nothing to yield, in which case io.EOF is returned.
func (l *List[T]) Shift(ctx context.Context) (t T, fn AckFunc, err error) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	ctx, done := context.WithCancel(ctx)
	defer done()
	go func() {
		<-ctx.Done()
		l.cond.Broadcast()
	}()

	for {
		// If we've exhausted all in flight and pending Ts then return io.EOF.
		if l.inFlight == 0 && len(l.pendingRetry) == 0 {
			err = io.EOF
			return
		}
		// If the context is cancelled return its error.
		if err = ctx.Err(); err != nil {
			return
		}
		// If there's a retry ready to unshift then return it.
		var unshifted bool
		if t, fn, unshifted = l.tryShift(ctx, false); unshifted {
			return
		}

		// Otherwise, wait for either a context cancel or other activity.
		l.cond.Wait()
	}
}
