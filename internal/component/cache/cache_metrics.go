package cache

import (
	"context"
	"errors"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

type metricsCache struct {
	c   V1
	sig *shutdown.Signaller

	mGetNotFound metrics.StatCounter
	mGetError    metrics.StatCounter
	mGetSuccess  metrics.StatCounter
	mGetLatency  metrics.StatTimer

	mSetError   metrics.StatCounter
	mSetSuccess metrics.StatCounter
	mSetLatency metrics.StatTimer

	mAddDupe    metrics.StatCounter
	mAddError   metrics.StatCounter
	mAddSuccess metrics.StatCounter
	mAddLatency metrics.StatTimer

	mDelError   metrics.StatCounter
	mDelSuccess metrics.StatCounter
	mDelLatency metrics.StatTimer
}

// MetricsForCache wraps a cache with a struct that adds standard metrics over
// each method.
func MetricsForCache(c V1, stats metrics.Type) V1 {
	cacheSuccess := stats.GetCounterVec("cache_success", "operation")
	cacheError := stats.GetCounterVec("cache_error", "operation")
	cacheLatency := stats.GetTimerVec("cache_latency_ns", "operation")

	return &metricsCache{
		c: c, sig: shutdown.NewSignaller(),

		mGetNotFound: stats.GetCounterVec("cache_not_found", "operation").With("get"),
		mGetError:    cacheError.With("get"),
		mGetSuccess:  cacheSuccess.With("get"),
		mGetLatency:  cacheLatency.With("get"),

		mSetError:   cacheError.With("set"),
		mSetSuccess: cacheSuccess.With("set"),
		mSetLatency: cacheLatency.With("set"),

		mAddDupe:    stats.GetCounterVec("cache_duplicate", "operation").With("add"),
		mAddError:   cacheError.With("add"),
		mAddSuccess: cacheSuccess.With("add"),
		mAddLatency: cacheLatency.With("add"),

		mDelError:   cacheError.With("delete"),
		mDelSuccess: cacheSuccess.With("delete"),
		mDelLatency: cacheLatency.With("delete"),
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
			a.mGetError.Incr(1)
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
		a.mSetError.Incr(1)
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
		a.mSetError.Incr(int64(len(items)))
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
			a.mAddError.Incr(1)
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
		a.mDelError.Incr(1)
	} else {
		a.mDelSuccess.Incr(1)
	}
	return err
}

func (a *metricsCache) Close(ctx context.Context) error {
	return a.c.Close(ctx)
}
