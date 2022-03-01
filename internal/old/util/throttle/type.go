package throttle

import (
	"context"
	"sync/atomic"
	"time"
)

//------------------------------------------------------------------------------

// Type is a throttle of retries to avoid endless busy loops when a message
// fails to reach its destination.
type Type struct {
	// consecutiveRetries is the live count of consecutive retries.
	consecutiveRetries int64

	// throttlePeriod is the current throttle period, by default this is set to
	// the baseThrottlePeriod.
	throttlePeriod int64

	// unthrottledRetries is the number of concecutive retries we are
	// comfortable attempting before throttling begins.
	unthrottledRetries int64

	// maxExponentialPeriod is the maximum duration for which our throttle lasts
	// when exponentially increasing.
	maxExponentialPeriod int64

	// baseThrottlePeriod is the static duration for which our throttle lasts.
	baseThrottlePeriod int64

	// closeChan can interrupt a throttle when closed.
	closeChan <-chan struct{}
}

// New creates a new throttle, which permits a static number of consecutive
// retries before throttling subsequent retries. A success will reset the count
// of consecutive retries.
func New(options ...func(*Type)) *Type {
	t := &Type{
		unthrottledRetries:   3,
		baseThrottlePeriod:   int64(time.Second),
		maxExponentialPeriod: int64(time.Minute),
		closeChan:            nil,
	}
	t.throttlePeriod = t.baseThrottlePeriod
	for _, option := range options {
		option(t)
	}
	return t
}

//------------------------------------------------------------------------------

// OptMaxUnthrottledRetries sets the maximum number of consecutive retries that
// will be attempted before throttling will begin.
func OptMaxUnthrottledRetries(n int64) func(*Type) {
	return func(t *Type) {
		t.unthrottledRetries = n
	}
}

// OptMaxExponentPeriod sets the maximum period of time that throttles will last
// when exponentially increasing.
func OptMaxExponentPeriod(period time.Duration) func(*Type) {
	return func(t *Type) {
		t.maxExponentialPeriod = int64(period)
	}
}

// OptThrottlePeriod sets the static period of time that throttles will last.
func OptThrottlePeriod(period time.Duration) func(*Type) {
	return func(t *Type) {
		t.baseThrottlePeriod = int64(period)
		t.throttlePeriod = int64(period)
	}
}

// OptCloseChan sets a read-only channel that, if closed, will interrupt a retry
// throttle early.
func OptCloseChan(c <-chan struct{}) func(*Type) {
	return func(t *Type) {
		t.closeChan = c
	}
}

//------------------------------------------------------------------------------

// Retry indicates that a retry is about to occur and, if appropriate, will
// block until either the throttle period is over and the retry may be attempted
// (returning true) or that the close channel has closed (returning false).
func (t *Type) Retry() bool {
	return t.RetryWithContext(context.Background())
}

// RetryWithContext indicates that a retry is about to occur and, if
// appropriate, will block until either the throttle period is over and the
// retry may be attempted (returning true) or that the close channel has closed
// (returning false), or that the context was cancelled (false).
func (t *Type) RetryWithContext(ctx context.Context) bool {
	if rets := atomic.AddInt64(&t.consecutiveRetries, 1); rets <= t.unthrottledRetries {
		return true
	}
	select {
	case <-time.After(time.Duration(atomic.LoadInt64(&t.throttlePeriod))):
	case <-t.closeChan:
		return false
	case <-ctx.Done():
		return false
	}
	return true
}

// ExponentialRetry is the same as Retry except also sets the throttle period to
// exponentially increase after each consecutive retry.
func (t *Type) ExponentialRetry() bool {
	return t.ExponentialRetryWithContext(context.Background())
}

// ExponentialRetryWithContext is the same as RetryWithContext except also sets
// the throttle period to exponentially increase after each consecutive retry.
func (t *Type) ExponentialRetryWithContext(ctx context.Context) bool {
	if atomic.LoadInt64(&t.consecutiveRetries) > t.unthrottledRetries {
		if throtPrd := atomic.LoadInt64(&t.throttlePeriod); throtPrd < t.maxExponentialPeriod {
			throtPrd *= 2
			if throtPrd > t.maxExponentialPeriod {
				throtPrd = t.maxExponentialPeriod
			}
			atomic.StoreInt64(&t.throttlePeriod, throtPrd)
		}
	}
	return t.RetryWithContext(ctx)
}

// Reset clears the count of consecutive retries and resets the exponential
// backoff.
func (t *Type) Reset() {
	atomic.StoreInt64(&t.consecutiveRetries, 0)
	atomic.StoreInt64(&t.throttlePeriod, t.baseThrottlePeriod)
}

//------------------------------------------------------------------------------
