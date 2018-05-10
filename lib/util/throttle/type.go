package throttle

import "time"

//------------------------------------------------------------------------------

// Type is a throttle of retries to avoid endless busy loops when a message
// fails to reach its destination. This isn't intended to be used for hitting
// load balanced resources and therefore doesn't implement anything clever like
// exponential backoff.
type Type struct {
	// unthrottledRetries is the number of concecutive retries we are
	// comfortable attempting before throttling begins.
	unthrottledRetries int

	// maxExponentialPeriod is the maximum duration for which our throttle lasts
	// when exponentially increasing.
	maxExponentialPeriod time.Duration

	// baseThrottlePeriod is the static duration for which our throttle lasts.
	baseThrottlePeriod time.Duration

	// throttlePeriod is the current throttle period, by default this is set to
	// the baseThrottlePeriod.
	throttlePeriod time.Duration

	// closeChan can interrupt a throttle when closed.
	closeChan <-chan struct{}

	// consecutiveRetries is the live count of consecutive retries.
	consecutiveRetries int
}

// New creates a new throttle, which permits a static number of consecutive
// retries before throttling subsequent retries. A success will reset the count
// of consecutive retries.
func New(options ...func(*Type)) *Type {
	t := &Type{
		unthrottledRetries:   3,
		baseThrottlePeriod:   time.Second,
		maxExponentialPeriod: time.Minute,
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
func OptMaxUnthrottledRetries(n int) func(*Type) {
	return func(t *Type) {
		t.unthrottledRetries = n
	}
}

// OptMaxExponentPeriod sets the maximum period of time that throttles will last
// when exponentially increasing.
func OptMaxExponentPeriod(period time.Duration) func(*Type) {
	return func(t *Type) {
		t.maxExponentialPeriod = period
	}
}

// OptThrottlePeriod sets the static period of time that throttles will last.
func OptThrottlePeriod(period time.Duration) func(*Type) {
	return func(t *Type) {
		t.baseThrottlePeriod = period
		t.throttlePeriod = period
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
	t.consecutiveRetries++
	if t.consecutiveRetries <= t.unthrottledRetries {
		return true
	}
	select {
	case <-time.After(t.throttlePeriod):
	case <-t.closeChan:
		return false
	}
	return true
}

// ExponentialRetry is the same as Retry except also sets the throttle period to
// exponentially increase after each consecutive retry.
func (t *Type) ExponentialRetry() bool {
	if t.consecutiveRetries > t.unthrottledRetries {
		if t.throttlePeriod < t.maxExponentialPeriod {
			t.throttlePeriod = t.throttlePeriod * 2
			if t.throttlePeriod > t.maxExponentialPeriod {
				t.throttlePeriod = t.maxExponentialPeriod
			}
		}
	}
	return t.Retry()
}

// Reset clears the count of consecutive retries and resets the exponential
// backoff.
func (t *Type) Reset() {
	t.consecutiveRetries = 0
	t.throttlePeriod = t.baseThrottlePeriod
}

//------------------------------------------------------------------------------
