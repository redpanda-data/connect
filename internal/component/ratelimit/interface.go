package ratelimit

import (
	"context"
	"time"
)

// V1 is a common interface implemented by rate limits.
type V1 interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(ctx context.Context) (time.Duration, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
