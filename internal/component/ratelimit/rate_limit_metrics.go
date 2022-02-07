package ratelimit

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type metricsRateLimit struct {
	r types.RateLimit

	mChecked metrics.StatCounter
	mLimited metrics.StatCounter
	mErr     metrics.StatCounter
}

// MetricsForRateLimit wraps a ratelimit.V2 with a struct that implements
// types.RateLimit.
func MetricsForRateLimit(r types.RateLimit, stats metrics.Type) types.RateLimit {
	return &metricsRateLimit{
		r: r,

		mChecked: stats.GetCounter("checked"),
		mLimited: stats.GetCounter("limited"),
		mErr:     stats.GetCounter("error"),
	}
}

func (r *metricsRateLimit) Access(ctx context.Context) (time.Duration, error) {
	r.mChecked.Incr(1)
	tout, err := r.r.Access(ctx)
	if err != nil {
		r.mErr.Incr(1)
	} else if tout > 0 {
		r.mLimited.Incr(1)
	}
	return tout, err
}

func (r *metricsRateLimit) Close(ctx context.Context) error {
	return r.r.Close(ctx)
}
