package cache

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// V2 is a simpler interface to implement than types.Cache.
type V2 interface {
	// Get a cache item.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set a cache item, specifying an optioal TTL. It is okay for caches to
	// ignore the ttl parameter if it isn't possible to implement.
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Add is the same operation as Set except that it returns an error if the
	// key already exists. It is okay for caches to return nil on duplicates if
	// it isn't possible to implement.
	Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Delete attempts to remove a key. If the key does not exist then it is
	// considered correct to return an error, however, for cache implementations
	// where it is difficult to determine this then it is acceptable to return
	// nil.
	Delete(ctx context.Context, key string) error

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements types.Cache
type v2ToV1Cache struct {
	c   V2
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

// NewV2ToV1Cache wraps a cache.V2 with a struct that implements types.Cache.
func NewV2ToV1Cache(c V2, stats metrics.Type) types.Cache {
	return &v2ToV1Cache{
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

func (a *v2ToV1Cache) Get(key string) ([]byte, error) {
	started := time.Now()
	b, err := a.c.Get(context.Background(), key)
	a.mGetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		if errors.Is(err, types.ErrKeyNotFound) {
			a.mGetNotFound.Incr(1)
		} else {
			a.mGetFailed.Incr(1)
		}
	} else {
		a.mGetSuccess.Incr(1)
	}
	return b, err
}

func (a *v2ToV1Cache) Set(key string, value []byte) error {
	started := time.Now()
	err := a.c.Set(context.Background(), key, value, nil)
	a.mSetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mSetFailed.Incr(1)
	} else {
		a.mSetSuccess.Incr(1)
	}
	return err
}

func (a *v2ToV1Cache) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	started := time.Now()
	err := a.c.Set(context.Background(), key, value, ttl)
	a.mSetLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mSetFailed.Incr(1)
	} else {
		a.mSetSuccess.Incr(1)
	}
	return err
}

func (a *v2ToV1Cache) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := a.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (a *v2ToV1Cache) SetMultiWithTTL(items map[string]types.CacheTTLItem) error {
	for k, v := range items {
		if err := a.SetWithTTL(k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

func (a *v2ToV1Cache) Add(key string, value []byte) error {
	started := time.Now()
	err := a.c.Add(context.Background(), key, value, nil)
	a.mAddLatency.Timing(int64(time.Since(started)))
	if err != nil {
		if errors.Is(err, types.ErrKeyAlreadyExists) {
			a.mAddDupe.Incr(1)
		} else {
			a.mAddFailed.Incr(1)
		}
	} else {
		a.mAddSuccess.Incr(1)
	}
	return err
}

func (a *v2ToV1Cache) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	started := time.Now()
	err := a.c.Add(context.Background(), key, value, ttl)
	a.mAddLatency.Timing(int64(time.Since(started)))
	if err != nil {
		if errors.Is(err, types.ErrKeyAlreadyExists) {
			a.mAddDupe.Incr(1)
		} else {
			a.mAddFailed.Incr(1)
		}
	} else {
		a.mAddSuccess.Incr(1)
	}
	return err
}

func (a *v2ToV1Cache) Delete(key string) error {
	started := time.Now()
	err := a.c.Delete(context.Background(), key)
	a.mDelLatency.Timing(int64(time.Since(started)))
	if err != nil {
		a.mDelFailed.Incr(1)
	} else {
		a.mDelSuccess.Incr(1)
	}
	return err
}

func (a *v2ToV1Cache) CloseAsync() {
	go func() {
		if err := a.c.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *v2ToV1Cache) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}
