package autoretry

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// ErrExhausted is returned by shift calls when there are no pending messages
// and new reads are disabled.
var ErrExhausted = errors.New("retry list is exhausted")

// AckFunc is a synchronous function that matches the standard acknowledgment
// signature in Benthos.
type AckFunc func(context.Context, error) error

// ReadFunc is a closure used to obtain a new T, this is done asynchronously
// from retries but the result is given lower priority than retries. If the read
// func returns an error then it is returned as a highest priority.
type ReadFunc[T any] func(context.Context) (t T, aFn AckFunc, err error)

// MutatorFunc is an optional closure used to mutate a T about to be scheduled
// for retry based on the returned error. This is useful for reducing a batch
// based on a batch error, etc.
type MutatorFunc[T any] func(t T, err error) T

// The result of a read attempt for a new T, the result of this is lower
// priority than pendingT.
type readT[T any] struct {
	t   T
	aFn AckFunc
	err error
}

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
	// Send is a lower priority than retry
	pendingRead  *readT[T]
	pendingRetry []*pendingT[T]

	reader  ReadFunc[T]
	mutator MutatorFunc[T]

	readInFlight  int
	retryInFlight int
	cond          sync.Cond
	readCtx       context.Context
	readDone      func()
}

// NewList returns a new list of Ts requiring automatic retries.
func NewList[T any](reader ReadFunc[T], mutator MutatorFunc[T]) *List[T] {
	if mutator == nil {
		mutator = func(t T, err error) T { return t }
	}
	readCtx, readDone := context.WithCancel(context.Background())
	return &List[T]{
		cond:     *sync.NewCond(&sync.Mutex{}),
		reader:   reader,
		mutator:  mutator,
		readCtx:  readCtx,
		readDone: readDone,
	}
}

// Adopt a T and its acknowledgement function so that a rejected T is added to
// retry list. Returns a new acknowledgment function that should be propagated
// as it encapsulates the retry logic.
func (l *List[T]) adopt(t T, aFn AckFunc) AckFunc {
	l.retryInFlight++

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
		l.retryInFlight--
		return t.aFn(ctx, nil)
	}
}

func (l *List[T]) dispatchReader() {
	var next readT[T]
	next.t, next.aFn, next.err = l.reader(l.readCtx)

	l.cond.L.Lock()
	l.pendingRead = &next
	l.readInFlight--
	l.cond.Broadcast()
	l.cond.L.Unlock()
}

// Shift blocks until either a T needs retrying and returns it, enableRead is
// set true and a new T is ready, or the context is cancelled. Returns
// ErrExhausted if all messages are exhausted and enableRead is set to false.
func (l *List[T]) Shift(ctx context.Context, enableRead bool) (t T, fn AckFunc, err error) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	ctx, done := context.WithCancel(ctx)
	defer done()
	go func() {
		<-ctx.Done()
		l.cond.Broadcast()
	}()

	if enableRead && l.readInFlight == 0 && l.pendingRead == nil {
		l.readInFlight++
		go l.dispatchReader()
	}

	for {
		// If we've exhausted all in flight and pending Ts return ErrExhausted.
		if !enableRead && l.exhausted() {
			err = ErrExhausted
			return
		}

		// If the context is cancelled return its error.
		if err = ctx.Err(); err != nil {
			return
		}

		// If there's a retry ready to unshift then return it.
		var unshifted bool
		if t, fn, unshifted = l.tryShift(ctx); unshifted {
			return
		}

		// If the read attempt succeeded then return it.
		if pendingRead := l.pendingRead; pendingRead != nil {
			if pendingRead.err == nil {
				pendingRead.aFn = l.adopt(pendingRead.t, pendingRead.aFn)
			}
			l.pendingRead = nil
			return pendingRead.t, pendingRead.aFn, pendingRead.err
		}

		// Otherwise, wait for either a context cancel or other activity.
		l.cond.Wait()
	}
}

func (l *List[T]) tryShift(ctx context.Context) (t T, fn AckFunc, ok bool) {
	var resend *pendingT[T]
	func() {
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
	return l.exhausted()
}

func (l *List[T]) exhausted() bool {
	return l.readInFlight == 0 && l.retryInFlight == 0 && len(l.pendingRetry) == 0
}

// Close any pending read attempts that could be dangling from prior shifts.
func (l *List[T]) Close(ctx context.Context) error {
	l.readDone()
	return nil
}

// TODO: Ensure docs around auto retry and all implementations are okay with
// nacks on termination, otherwise we leave them.
//
//nolint:unused // Keeping this around for now.
func (l *List[T]) nackAllPending() error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	rejectAck := func(fn AckFunc) {
		_ = fn(context.Background(), errors.New("message rejected"))
	}

	// Wait for all pending activity to end.
	for {
		if l.retryInFlight <= 0 && l.readInFlight <= 0 {
			for _, r := range l.pendingRetry {
				go rejectAck(r.aFn)
				l.pendingRead = nil
			}
			l.pendingRetry = nil
			if l.pendingRead != nil && l.pendingRead.aFn != nil {
				go rejectAck(l.pendingRead.aFn)
				l.pendingRead = nil
			}
			return nil
		}
		l.cond.Wait()
	}
}
