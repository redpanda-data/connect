package dgraph

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto"

	"github.com/benthosdev/benthos/v4/public/service"
)

func ristrettoCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Second
	retriesDefaults.MaxInterval = time.Second * 5
	retriesDefaults.MaxElapsedTime = time.Second * 30

	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a map held in the memory-bound [Ristretto cache](https://github.com/dgraph-io/ristretto).`).
		Description(`This cache is more efficient and appropriate for high-volume use cases than the standard memory cache. However, the add command is non-atomic, and therefore this cache is not suitable for deduplication.`).
		Field(service.NewDurationField("default_ttl").
			Description("A default TTL to set for items, calculated from the moment the item is cached. Set to an empty string or zero duration to disable TTLs.").
			Default("").
			Example("5m").
			Example("60s")).
		Field(service.NewBackOffToggledField("get_retries", false, retriesDefaults).
			Description("Determines how and whether get attempts should be retried if the key is not found. Ristretto is a concurrent cache that does not immediately reflect writes, and so it can sometimes be useful to enable retries at the cost of speed in cases where the key is expected to exist.").
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"ristretto", ristrettoCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newRistrettoCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newRistrettoCacheFromConfig(conf *service.ParsedConfig) (*ristrettoCache, error) {
	backOff, backOffEnabled, err := conf.FieldBackOffToggled("get_retries")
	if err != nil {
		return nil, err
	}

	var defaultTTL time.Duration
	if testStr, _ := conf.FieldString("default_ttl"); testStr != "" {
		if defaultTTL, err = conf.FieldDuration("default_ttl"); err != nil {
			return nil, err
		}
	}

	return newRistrettoCache(defaultTTL, backOffEnabled, backOff)
}

//------------------------------------------------------------------------------

type ristrettoCache struct {
	defaultTTL time.Duration
	cache      *ristretto.Cache

	retriesEnabled bool
	boffPool       sync.Pool
}

func newRistrettoCache(defaultTTL time.Duration, retriesEnabled bool, backOff *backoff.ExponentialBackOff) (*ristrettoCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	r := &ristrettoCache{
		defaultTTL:     defaultTTL,
		cache:          cache,
		retriesEnabled: retriesEnabled,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}

	return r, nil
}

func (r *ristrettoCache) Get(ctx context.Context, key string) ([]byte, error) {
	var boff backoff.BackOff

	for {
		res, ok := r.cache.Get(key)
		if ok {
			return res.([]byte), nil
		}

		if r.retriesEnabled {
			if boff == nil {
				boff = r.boffPool.Get().(backoff.BackOff)
				defer func() { //nolint:gocritic
					boff.Reset()
					r.boffPool.Put(boff)
				}()
			}
		} else {
			return nil, service.ErrKeyNotFound
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return nil, service.ErrKeyNotFound
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, service.ErrKeyNotFound
		}
	}
}

func (r *ristrettoCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	var t time.Duration
	if ttl != nil {
		t = *ttl
	} else {
		t = r.defaultTTL
	}
	if !r.cache.SetWithTTL(key, value, 1, t) {
		return errors.New("set operation was dropped")
	}
	return nil
}

func (r *ristrettoCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return r.Set(ctx, key, value, ttl)
}

func (r *ristrettoCache) Delete(ctx context.Context, key string) error {
	r.cache.Del(key)
	return nil
}

func (r *ristrettoCache) Close(ctx context.Context) error {
	r.cache.Close()
	return nil
}
