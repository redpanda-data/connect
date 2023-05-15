package pure

import (
	"context"
	"fmt"
	"sync"
	"time"

	"zvelo.io/ttlru"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ttlruCacheFieldCapLabel                 = "cap"
	ttlruCacheFieldCapDefaultValue          = 1024
	ttlruCacheFieldTTLLabel                 = "ttl"
	ttlruCacheFieldTTLDefaultValue          = 5 * time.Minute
	ttlruCacheFieldInitValuesLabel          = "init_values"
	ttlruCacheFieldWithoutResetLabel        = "without_reset"
	ttlruCacheFieldWithoutResetDefaultValue = false
	ttlruCacheFieldOptimisticLabel          = "optimistic"
	ttlruCacheFieldOptimisticDefaultValue   = false
)

func ttlruCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a ttlru in-memory cache. This cache is therefore reset every time the service restarts.`).
		Description(`The cache ttlru provides a simple, goroutine safe, cache with a fixed number of entries. Each entry has a per-cache defined TTL.

This TTL is reset on both modification and access of the value. As a result, if the cache is full, and no items have expired, when adding a new item, the item with the soonest expiration will be evicted.

It uses the package ` + "[`ttlru`](https://github.com/zvelo/ttlru)" + `

The field ` + ttlruCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of key/value pairs:

` + "```yaml" + `
cache_resources:
  - label: foocache
    ttlru:
      ttl: '5m'
      cap: 1024
      init_values:
        foo: bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(ttlruCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(ttlruCacheFieldCapDefaultValue)).
		Field(service.NewDurationField(ttlruCacheFieldTTLLabel).
			Description("The cache ttl of each element").
			Default(ttlruCacheFieldTTLDefaultValue.String())).
		Field(service.NewStringMapField(ttlruCacheFieldInitValuesLabel).
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]string{}).
			Example(map[string]string{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			})).
		Field(service.NewBoolField(ttlruCacheFieldWithoutResetLabel).
			Description("If true, we stop reset the ttl on read events.").
			Default(ttlruCacheFieldWithoutResetDefaultValue).
			Advanced()).
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
			f, err := ttlruMemCacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func ttlruMemCacheFromConfig(conf *service.ParsedConfig) (*ttlruCacheAdapter, error) {
	capacity, err := conf.FieldInt(ttlruCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	ttl, err := conf.FieldDuration(ttlruCacheFieldTTLLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringMap(ttlruCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	withoutReset, err := conf.FieldBool(ttlruCacheFieldWithoutResetLabel)
	if err != nil {
		return nil, err
	}

	optimistic, err := conf.FieldBool(ttlruCacheFieldOptimisticLabel)
	if err != nil {
		return nil, err
	}

	return ttlruMemCache(capacity, ttl, initValues, withoutReset, optimistic)
}

//------------------------------------------------------------------------------

type ttlruCacheAdapter struct {
	inner ttlru.Cache

	optimistic bool

	sync.RWMutex
}

var (
	errInvalidTTLRUCacheParameters    = fmt.Errorf("invalid ttlru cache parameters")
	errInvalidTTLRUCacheCapacityValue = fmt.Errorf("invalid ttlru cache parameter capacity: must be bigger than 0")
	errInvalidTTLRUCachetTTLValue     = fmt.Errorf("invalid ttlru cache parameter ttl: must be bigger than 0s")
)

func ttlruMemCache(capacity int,
	ttl time.Duration,
	initValues map[string]string,
	withoutReset, optimistic bool,
) (*ttlruCacheAdapter, error) {
	if capacity <= 0 {
		return nil, errInvalidTTLRUCacheCapacityValue
	}

	if ttl <= 0 {
		return nil, errInvalidTTLRUCachetTTLValue
	}

	opts := make([]ttlru.Option, 1, 2)

	opts[0] = ttlru.WithTTL(ttl)

	if withoutReset {
		opts = append(opts, ttlru.WithoutReset())
	}

	c := ttlru.New(capacity, opts...)
	if c == nil {
		return nil, errInvalidTTLRUCacheParameters
	}

	for k, v := range initValues {
		c.Set(k, []byte(v))
	}

	return &ttlruCacheAdapter{
		inner:      c,
		optimistic: optimistic,
	}, nil
}

var _ service.Cache = (*ttlruCacheAdapter)(nil)

func (ca *ttlruCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	unlock := func() {}
	if !ca.optimistic {
		ca.RWMutex.RLock()

		unlock = ca.RWMutex.RUnlock
	}

	value, ok := ca.inner.Get(key)

	unlock()

	if !ok {
		return nil, service.ErrKeyNotFound
	}

	data, _ := value.([]byte)

	return data, nil
}

func (ca *ttlruCacheAdapter) Set(_ context.Context, key string, value []byte, _ *time.Duration) error {
	unlock := func() {}
	if !ca.optimistic {
		ca.RWMutex.Lock()

		unlock = ca.RWMutex.Unlock
	}

	ca.inner.Set(key, value)

	unlock()

	return nil
}

func (ca *ttlruCacheAdapter) Add(_ context.Context, key string, value []byte, _ *time.Duration) error {
	unlock := func() {}
	if !ca.optimistic {
		ca.RWMutex.Lock()

		unlock = ca.RWMutex.Unlock
	}

	_, ok := ca.inner.Get(key)
	if ok {
		unlock()

		return service.ErrKeyAlreadyExists
	}

	_ = ca.inner.Set(key, value)

	unlock()

	return nil
}

func (ca *ttlruCacheAdapter) Delete(_ context.Context, key string) error {
	unlock := func() {}
	if !ca.optimistic {
		ca.RWMutex.Lock()

		unlock = ca.RWMutex.Unlock
	}

	ca.inner.Del(key)

	unlock()

	return nil
}

func (ca *ttlruCacheAdapter) Close(_ context.Context) error {
	return nil
}
