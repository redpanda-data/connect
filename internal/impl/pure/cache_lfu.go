package pure

import (
	"context"
	"fmt"
	"time"

	lfu "github.com/vmihailenco/go-tinylfu"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	lfuCacheFieldSizeLabel        = "size"
	lfuCacheFieldSizeDefaultValue = 1000

	lfuCacheFieldSamplesLabel        = "samples"
	lfuCacheFieldSamplesDefaultValue = 100000

	lfuCacheFieldInitValuesLabel = "init_values"

	// optimistic
	lfuCacheFieldOptimisticLabel        = "optimistic"
	lfuCacheFieldOptimisticDefaultValue = false
)

func lfuCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a lfu in-memory cache. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the lfu package which implements a fixed-size thread safe LFU cache.

It uses the package ` + "[`lfu`](github.com/vmihailenco/go-tinylfu)" + `

The field ` + lfuCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of key/value pairs:

` + "```yaml" + `
cache_resources:
  - label: foocache
    lfu:
      size: 1024
      init_values:
        foo: bar
` + "```" + `

These values can be overridden during execution, at which point the configured TTL is respected as usual.`).
		Field(service.NewIntField(lfuCacheFieldSizeLabel).
			Description("The cache maximum size (number of entries)").
			Default(lfuCacheFieldSizeDefaultValue)).
		Field(service.NewIntField(lfuCacheFieldSamplesLabel).
			Description("The cache samples").
			Default(lfuCacheFieldSamplesDefaultValue)).
		Field(service.NewStringMapField(lfuCacheFieldInitValuesLabel).
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]string{}).
			Example(map[string]string{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			})).
		Field(service.NewBoolField(lfuCacheFieldOptimisticLabel).
			Description("If true, we do not lock on read/write events. The lfu package is thread-safe, however the ADD operation is not atomic.").
			Default(lfuCacheFieldOptimisticDefaultValue).
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"lfu", lfuCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := lfuMemCacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func lfuMemCacheFromConfig(conf *service.ParsedConfig) (*lfuCacheAdapter, error) {
	size, err := conf.FieldInt(lfuCacheFieldSizeLabel)
	if err != nil {
		return nil, err
	}

	samples, err := conf.FieldInt(lfuCacheFieldSamplesLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringMap(lfuCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	optimistic, err := conf.FieldBool(lfuCacheFieldOptimisticLabel)
	if err != nil {
		return nil, err
	}

	return lfuMemCache(size, samples, initValues, optimistic)
}

//------------------------------------------------------------------------------

var (
	errInvalidLFUCacheSizeValue    = fmt.Errorf("invalid lfu cache parameter size: must be bigger than 0")
	errInvalidLFUCacheSamplesValue = fmt.Errorf("invalid lfu cache parameter samples: must be bigger than 0")
)

func lfuMemCache(size int,
	samples int,
	initValues map[string]string,
	optimistic bool) (ca *lfuCacheAdapter, err error) {
	if size <= 0 {
		return nil, errInvalidLFUCacheSizeValue
	}
	if samples <= 0 {
		return nil, errInvalidLFUCacheSamplesValue
	}

	var inner lfuCache

	if optimistic {
		inner = lfu.New(size, samples)
	} else {
		inner = lfu.NewSync(size, samples)
	}

	for k, v := range initValues {
		inner.Set(&lfu.Item{
			Key:   k,
			Value: []byte(v),
		})
	}

	ca = &lfuCacheAdapter{
		inner: inner,
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var (
	_ lfuCache = (*lfu.T)(nil)
	_ lfuCache = (*lfu.SyncT)(nil)
)

type lfuCache interface {
	Get(key string) (interface{}, bool)
	// Add(newItem *Item) error
	Set(newItem *lfu.Item)
	Del(key string)
}

//------------------------------------------------------------------------------

var _ service.Cache = (*lfuCacheAdapter)(nil)

type lfuCacheAdapter struct {
	inner lfuCache
}

func (ca *lfuCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := ca.inner.Get(key)
	if !ok {
		return nil, service.ErrKeyNotFound
	}

	data, _ := value.([]byte)

	return data, nil
}

func (ca *lfuCacheAdapter) Set(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	item := &lfu.Item{
		Key:   key,
		Value: value,
	}

	if ttl != nil {
		item.ExpireAt = time.Now().UTC().Add(*ttl)
	}

	ca.inner.Set(item)

	return nil
}

func (ca *lfuCacheAdapter) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return ca.Set(ctx, key, value, ttl)
	// return nil
}

func (ca *lfuCacheAdapter) Delete(_ context.Context, key string) error {
	ca.inner.Del(key)

	return nil
}

func (ca *lfuCacheAdapter) Close(_ context.Context) error {
	return nil
}
