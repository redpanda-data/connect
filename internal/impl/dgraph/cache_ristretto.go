package dgraph

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/dgraph-io/ristretto"
)

func ristrettoCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a map held in the memory-bound [Ristretto cache](https://github.com/dgraph-io/ristretto).`).
		Description(`This cache is more efficient and appropriate for high-volume use cases than the standard memory cache. However, the add command is non-atomic, and therefore this cache is not suitable for deduplication.`).
		Field(service.NewDurationField("default_ttl").
			Description("A default TTL to set for items, calculated from the moment the item is cached. Set to an empty string or zero duration to disable TTLs.").
			Default("").
			Example("5m").
			Example("60s")).
		Field(service.NewIntField("retries").
			Description("Optionally retries get operations when they fail because the key is not found.").
			Default(0).
			Advanced()).
		Field(service.NewDurationField("retry_period").
			Description("The duration to wait between retry attempts.").
			Default("50ms").
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
	retries, err := conf.FieldInt("retries")
	if err != nil {
		return nil, err
	}

	retryPeriod, err := conf.FieldDuration("retry_period")
	if err != nil {
		return nil, err
	}

	var defaultTTL time.Duration
	if testStr, _ := conf.FieldString("default_ttl"); testStr != "" {
		if defaultTTL, err = conf.FieldDuration("default_ttl"); err != nil {
			return nil, err
		}
	}
	return newRistrettoCache(defaultTTL, retries, retryPeriod)
}

//------------------------------------------------------------------------------
type ristrettoCache struct {
	defaultTTL time.Duration
	cache      *ristretto.Cache

	retries     int
	retryPeriod time.Duration
}

func newRistrettoCache(defaultTTL time.Duration, retries int, retryPeriod time.Duration) (*ristrettoCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	r := &ristrettoCache{
		defaultTTL:  defaultTTL,
		cache:       cache,
		retries:     retries,
		retryPeriod: retryPeriod,
	}

	return r, nil
}

func (r *ristrettoCache) Get(ctx context.Context, key string) ([]byte, error) {
	retries := r.retries
	for {
		res, ok := r.cache.Get(key)
		if ok {
			return res.([]byte), nil
		}

		if retries <= 0 {
			return nil, service.ErrKeyNotFound
		}

		retries--
		select {
		case <-time.After(r.retryPeriod):
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
