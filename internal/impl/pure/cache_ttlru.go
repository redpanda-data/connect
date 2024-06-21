package pure

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ttlruCacheFieldCapLabel        = "cap"
	ttlruCacheFieldCapDefaultValue = 1024

	ttlruCacheFieldDefaultTTLLabel        = "default_ttl"
	ttlruCacheFieldDefaultTTLDefaultValue = 5 * time.Minute

	ttlruCacheFieldInitValuesLabel = "init_values"

	ttlruCacheFieldOptimisticLabel        = "optimistic"
	ttlruCacheFieldOptimisticDefaultValue = false

	ttlruCacheFieldDeprecatedTTLLabel          = "ttl"
	ttlruCacheFieldDeprecatedWithoutResetLabel = "without_reset"
)

func ttlruCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a ttlru in-memory cache. This cache is therefore reset every time the service restarts.`).
		Description(`The cache ttlru provides a simple, goroutine safe, cache with a fixed number of entries. Each entry has a per-cache defined TTL.

This TTL is reset on both modification and access of the value. As a result, if the cache is full, and no items have expired, when adding a new item, the item with the soonest expiration will be evicted.

It uses the package ` + "[`expirable`](https://github.com/hashicorp/golang-lru/v2/expirable)" + `

The field ` + ttlruCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of key/value pairs:

` + "```yaml" + `
cache_resources:
  - label: foocache
    ttlru:
      default_ttl: '5m'
      cap: 1024
      init_values:
        foo: bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(ttlruCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(ttlruCacheFieldCapDefaultValue)).
		Field(service.NewDurationField(ttlruCacheFieldDefaultTTLLabel).
			Description("The cache ttl of each element").
			Default(ttlruCacheFieldDefaultTTLDefaultValue.String()).
			Version("4.21.0")).
		Field(service.NewDurationField(ttlruCacheFieldDeprecatedTTLLabel).
			Description("Deprecated. Please use `" + ttlruCacheFieldDefaultTTLLabel + "` field").
			Optional().Advanced()).
		Field(service.NewStringMapField(ttlruCacheFieldInitValuesLabel).
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			})).
		Field(service.NewBoolField(ttlruCacheFieldOptimisticLabel).
			Description("If true, we do not lock on read/write events. The ttlru package is thread-safe, however the ADD operation is not atomic.").
			Default(ttlruCacheFieldOptimisticDefaultValue).
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"ttlru", ttlruCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			logger := mgr.Logger().With("component", "ttlru")
			f, err := ttlruMemCacheFromConfig(conf, logger)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func ttlruMemCacheFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*ttlruCacheAdapter, error) {
	capacity, err := conf.FieldInt(ttlruCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	var ttl time.Duration
	if conf.Contains(ttlruCacheFieldDeprecatedTTLLabel) {
		ttl, err = conf.FieldDuration(ttlruCacheFieldDeprecatedTTLLabel)

		logger.Warnf("field %q is deprecated, please use %q",
			ttlruCacheFieldDeprecatedTTLLabel, ttlruCacheFieldDefaultTTLLabel)
	} else {
		ttl, err = conf.FieldDuration(ttlruCacheFieldDefaultTTLLabel)
	}
	if err != nil {
		return nil, err
	}

	if withoutReset, _ := conf.FieldBool(ttlruCacheFieldDeprecatedWithoutResetLabel); withoutReset {
		logger.Warnf("field %q is deprecated, ignoring", ttlruCacheFieldDeprecatedWithoutResetLabel)
	}

	initValues, err := conf.FieldStringMap(ttlruCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	optimistic, err := conf.FieldBool(ttlruCacheFieldOptimisticLabel)
	if err != nil {
		return nil, err
	}

	return ttlruMemCache(capacity, ttl, initValues, optimistic)
}

//------------------------------------------------------------------------------

type ttlruCacheAdapter struct {
	inner *expirable.LRU[string, []byte]

	optimistic bool

	sync.Mutex
}

var (
	errInvalidTTLRUCacheCapacityValue = errors.New("invalid ttlru cache parameter capacity: must be bigger than 0")
	errInvalidTTLRUCachetTTLValue     = errors.New("invalid ttlru cache parameter default_ttl: must be bigger than 0s")
)

func ttlruMemCache(capacity int,
	defaultTTL time.Duration,
	initValues map[string]string,
	optimistic bool,
) (*ttlruCacheAdapter, error) {
	if capacity <= 0 {
		return nil, errInvalidTTLRUCacheCapacityValue
	}

	if defaultTTL <= 0 {
		return nil, errInvalidTTLRUCachetTTLValue
	}

	c := expirable.NewLRU[string, []byte](capacity, nil, defaultTTL)

	for k, v := range initValues {
		_ = c.Add(k, []byte(v))
	}

	return &ttlruCacheAdapter{
		inner:      c,
		optimistic: optimistic,
	}, nil
}

var _ service.Cache = (*ttlruCacheAdapter)(nil)

func (ca *ttlruCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := ca.inner.Get(key)
	if !ok {
		return nil, service.ErrKeyNotFound
	}

	return value, nil
}

func (ca *ttlruCacheAdapter) Set(_ context.Context, key string, value []byte, _ *time.Duration) error {
	_ = ca.inner.Add(key, value)

	return nil
}

func (ca *ttlruCacheAdapter) unsafeAdd(key string, value []byte) error {
	_, ok := ca.inner.Peek(key)
	if ok {
		return service.ErrKeyAlreadyExists
	}

	_ = ca.inner.Add(key, value)

	return nil
}

func (ca *ttlruCacheAdapter) Add(_ context.Context, key string, value []byte, _ *time.Duration) error {
	if ca.optimistic {
		return ca.unsafeAdd(key, value)
	}

	ca.Lock()

	err := ca.unsafeAdd(key, value)

	ca.Unlock()

	return err
}

func (ca *ttlruCacheAdapter) Delete(_ context.Context, key string) error {
	_ = ca.inner.Remove(key)

	return nil
}

func (ca *ttlruCacheAdapter) Close(_ context.Context) error {
	return nil
}
