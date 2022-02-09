package ratelimit

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
)

type metricsRateLimit struct {
	r V1

	mChecked metrics.StatCounter
	mLimited metrics.StatCounter
	mErr     metrics.StatCounter
}

// MetricsForRateLimit wraps a ratelimit.V2 with a struct that implements
// types.RateLimit.
func MetricsForRateLimit(r V1, stats metrics.Type) V1 {
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
