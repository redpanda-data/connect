package service

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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

func newAirGapRateLimit(c RateLimit, stats metrics.Type) types.RateLimit {
	return ratelimit.MetricsForRateLimit(c, stats)
}

//------------------------------------------------------------------------------

// Implements RateLimit around a types.RateLimit
type reverseAirGapRateLimit struct {
	r types.RateLimit
}

func newReverseAirGapRateLimit(r types.RateLimit) *reverseAirGapRateLimit {
	return &reverseAirGapRateLimit{r}
}

func (a *reverseAirGapRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *reverseAirGapRateLimit) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}
