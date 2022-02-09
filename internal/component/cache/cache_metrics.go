package cache

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

type metricsCache struct {
	c   V1
	sig *shutdown.Signaller

	mGetNotFound metrics.StatCounter
	mGetFailed   metrics.StatCounter
	mGetSuccess  metrics.StatCounter
	mGetLatency  metrics.StatTimer

	mSetFailed  metrics.StatCounter
	mSetSuccess metrics.StatCounter
	mSetLatency metrics.StatTimer

	mAddDupe    metrics.StatCounter
	mAddFailed  metrics.StatCounter
	mAddSuccess metrics.StatCounter
	mAddLatency metrics.StatTimer

	mDelFailed  metrics.StatCounter
	mDelSuccess metrics.StatCounter
	mDelLatency metrics.StatTimer
}

// MetricsForCache wraps a cache with a struct that adds standard metrics over
// each method.
func MetricsForCache(c V1, stats metrics.Type) V1 {
	return &metricsCache{
		c: c, sig: shutdown.NewSignaller(),

		mGetNotFound: stats.GetCounter("get.not_found"),
		mGetFailed:   stats.GetCounter("get.failed"),
		mGetSuccess:  stats.GetCounter("get.success"),
		mGetLatency:  stats.GetTimer("get.latency"),

		mSetFailed:  stats.GetCounter("set.failed"),
		mSetSuccess: stats.GetCounter("set.success"),
		mSetLatency: stats.GetTimer("set.latency"),

		mAddDupe:    stats.GetCounter("add.duplicate"),
		mAddFailed:  stats.GetCounter("add.failed"),
		mAddSuccess: stats.GetCounter("add.success"),
		mAddLatency: stats.GetTimer("add.latency"),

		mDelFailed:  stats.GetCounter("delete.failed"),
		mDelSuccess: stats.GetCounter("delete.success"),
		mDelLatency: stats.GetTimer("delete.latency"),
	}
}

func (a *metricsCache) Get(ctx context.Context, key string) ([]byte, error) {
	started := time.Now()
	b, err := a.c.Get(ctx, key)
	a.mGetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		if errors.Is(err, component.ErrKeyNotFound) {
			a.mGetNotFound.Incr(1)
		} else {
			a.mGetFailed.Incr(1)
		}
	} else {
		a.mGetSuccess.Incr(1)
	}
	return b, err
}

func (a *metricsCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	started := time.Now()
	err := a.c.Set(ctx, key, value, ttl)
	a.mSetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mSetFailed.Incr(1)
	} else {
		a.mSetSuccess.Incr(1)
	}
	return err
}

func (a *metricsCache) SetMulti(ctx context.Context, items map[string]TTLItem) error {
	started := time.Now()
	err := a.c.SetMulti(ctx, items)
	a.mSetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mSetFailed.Incr(int64(len(items)))
	} else {
		a.mSetSuccess.Incr(int64(len(items)))
	}
	return err
}

func (a *metricsCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	started := time.Now()
	err := a.c.Add(ctx, key, value, ttl)
	a.mAddLatency.Timing(int64(time.Since(started)))
	if err != nil {
		if errors.Is(err, component.ErrKeyAlreadyExists) {
			a.mAddDupe.Incr(1)
		} else {
			a.mAddFailed.Incr(1)
		}
	} else {
		a.mAddSuccess.Incr(1)
	}
	return err
}

func (a *metricsCache) Delete(ctx context.Context, key string) error {
	started := time.Now()
	err := a.c.Delete(ctx, key)
	a.mDelLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mDelFailed.Incr(1)
	} else {
		a.mDelSuccess.Incr(1)
	}
	return err
}

func (a *metricsCache) Close(ctx context.Context) error {
	return a.c.Close(ctx)
}
