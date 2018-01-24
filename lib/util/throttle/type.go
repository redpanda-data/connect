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

	// throttlePeriod is the static duration for which our throttle lasts.
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
		unthrottledRetries: 3,
		throttlePeriod:     time.Second,
		closeChan:          nil,
	}
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

// OptThrottlePeriod sets the static period of time that throttles will last.
func OptThrottlePeriod(period time.Duration) func(*Type) {
	return func(t *Type) {
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

// Reset clears the count of consecutive retries.
func (t *Type) Reset() {
	t.consecutiveRetries = 0
}

//------------------------------------------------------------------------------
