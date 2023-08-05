package pure

import (
	"context"
	"errors"
	"fmt"
	"time"

	lfu "github.com/vmihailenco/go-tinylfu"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	lfuCacheFieldSizeLabel        = "size"
	lfuCacheFieldSizeDefaultValue = 1000

	lfuCacheFieldDefaultTTLLabel        = "default_ttl"
	lfuCacheFieldDefaultTTLDefaultValue = 1 * time.Hour

	lfuCacheFieldSamplesLabel        = "samples"
	lfuCacheFieldSamplesDefaultValue = 100000

	lfuCacheFieldInitValuesLabel = "init_values"
)

func lfuCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a TinyLFU in-memory cache. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the lfu package which implements a fixed-size thread safe LFU cache.

It uses the package ` + "[`go-tinyflu`](github.com/vmihailenco/go-tinylfu)" + `

This cache is described on ` + "[`TinyLFU: A Highly Efficient Cache Admission Policy`](https://arxiv.org/abs/1512.00727)" + `

The field ` + lfuCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of key/value pairs:

` + "```yaml" + `
cache_resources:
  - label: foocache
    lfu:
      size: 1024
      init_values:
        foo: bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(lfuCacheFieldSizeLabel).
			Description("The cache maximum size (number of entries)").
			Default(lfuCacheFieldSizeDefaultValue)).
		Field(service.NewIntField(lfuCacheFieldSamplesLabel).
			Description("The cache samples").
			Default(lfuCacheFieldSamplesDefaultValue)).
		Field(service.NewDurationField(lfuCacheFieldDefaultTTLLabel).
			Description("The default ttl of each element").
			Default(lfuCacheFieldDefaultTTLDefaultValue.String())).
		Field(service.NewStringMapField(lfuCacheFieldInitValuesLabel).
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]string{}).
			Example(map[string]string{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			}))

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

	ttl, err := conf.FieldDuration(lfuCacheFieldDefaultTTLLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringMap(lfuCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	return lfuMemCache(size, samples, ttl, initValues)
}

//------------------------------------------------------------------------------

var (
	errInvalidLFUCacheSizeValue    = fmt.Errorf("invalid lfu cache parameter size: must be bigger than 0")
	errInvalidLFUCacheSamplesValue = fmt.Errorf("invalid lfu cache parameter samples: must be bigger than 0")
)

func lfuMemCache(size, samples int,
	ttl time.Duration,
	initValues map[string]string) (ca *lfuCacheAdapter, err error) {
	if size <= 0 {
		return nil, errInvalidLFUCacheSizeValue
	}
	if samples <= 0 {
		return nil, errInvalidLFUCacheSamplesValue
	}

	inner := lfu.NewSync(size, samples)

	ca = &lfuCacheAdapter{
		inner:      inner,
		defaultTTL: ttl,
	}

	for k, v := range initValues {
		_ = ca.Set(context.Background(), k, []byte(v), &ttl)
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var _ service.Cache = (*lfuCacheAdapter)(nil)

type lfuCacheAdapter struct {
	inner      lfu.LFU
	defaultTTL time.Duration
}

func (ca *lfuCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := ca.inner.Get(key)
	if !ok {
		return nil, service.ErrKeyNotFound
	}

	data, _ := value.([]byte)

	return data, nil
}

func (ca *lfuCacheAdapter) addExpireAt(item *lfu.Item, ttl *time.Duration) {
	var t time.Duration

	if ttl != nil {
		t = *ttl
	} else {
		t = ca.defaultTTL
	}

	if t > 0 {
		item.ExpireAt = time.Now().Add(t)
	}
}

func (ca *lfuCacheAdapter) Set(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	item := &lfu.Item{
		Key:   key,
		Value: value,
	}

	ca.addExpireAt(item, ttl)

	ca.inner.Set(item)

	return nil
}

func (ca *lfuCacheAdapter) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	item := &lfu.Item{
		Key:   key,
		Value: value,
	}

	ca.addExpireAt(item, ttl)

	err := ca.inner.Add(item)
	if errors.Is(err, lfu.ErrKeyAlreadyExists) {
		return service.ErrKeyAlreadyExists
	} else if err != nil {
		return fmt.Errorf("unexpected error: %w", err)
	}

	return nil
}

func (ca *lfuCacheAdapter) Delete(_ context.Context, key string) error {
	ca.inner.Del(key)

	return nil
}

func (ca *lfuCacheAdapter) Close(_ context.Context) error {
	return nil
}
