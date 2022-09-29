package mock

import (
	"context"
	"time"
)

// RateLimit provides a mock rate limit implementation around a closure.
type RateLimit func(context.Context) (time.Duration, error)

// Access the rate limit.
func (r RateLimit) Access(ctx context.Context) (time.Duration, error) {
	return r(ctx)
}

// Close does nothing.
func (r RateLimit) Close(ctx context.Context) error {
	return nil
}
