package mock

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.RateLimit = RateLimit(nil)

// RateLimit provides a mock rate limit implementation around a closure.
type RateLimit func() (time.Duration, error)

// Access the rate limit
func (r RateLimit) Access() (time.Duration, error) {
	return r()
}

// CloseAsync does nothing
func (r RateLimit) CloseAsync() {
}

// WaitForClose does nothing
func (r RateLimit) WaitForClose(t time.Duration) error {
	return nil
}
