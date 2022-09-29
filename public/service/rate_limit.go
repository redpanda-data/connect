package service

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
)

// RateLimit is an interface implemented by Benthos rate limits.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(context.Context) (time.Duration, error)

	Closer
}

//------------------------------------------------------------------------------

func newAirGapRateLimit(c RateLimit, stats metrics.Type) ratelimit.V1 {
	return ratelimit.MetricsForRateLimit(c, stats)
}

//------------------------------------------------------------------------------

// Implements RateLimit around a types.RateLimit.
type reverseAirGapRateLimit struct {
	r ratelimit.V1
}

func newReverseAirGapRateLimit(r ratelimit.V1) *reverseAirGapRateLimit {
	return &reverseAirGapRateLimit{r}
}

func (a *reverseAirGapRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *reverseAirGapRateLimit) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}
