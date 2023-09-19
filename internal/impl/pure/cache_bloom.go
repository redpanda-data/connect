package pure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	bloomCacheFieldCapLabel        = "cap"
	bloomCacheFieldCapDefaultValue = 10000

	bloomCacheFieldFalsePositiveRateLabel        = "fp"
	bloomCacheFieldFalsePositiveRateDefaultValue = 0.01

	bloomCacheFieldInitValuesLabel = "init_values"

	bloomCacheFieldStrictLabel        = "strict"
	bloomCacheFieldStrictDefaultValue = false
)

func bloomCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary(`Stores keys in a bloom in-memory filter, useful for deduplication. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the bloom package which implements a fixed-size thread safe filter.

A Bloom filter is a concise/compressed representation of a set, where the main requirement is to make membership queries; i.e., whether an item is a member of a set. A Bloom filter will always correctly report the presence of an element in the set when the element is indeed present. A Bloom filter can use much less storage than the original set, but it allows for some 'false positives': it may sometimes report that an element is in the set whereas it is not.

When you construct, you need to know how many elements you have (the desired capacity), and what is the desired false positive rate you are willing to tolerate. A common false-positive rate is 1%. The lower the false-positive rate, the more memory you are going to require. Similarly, the higher the capacity, the more memory you will use.

It uses the package ` + "[`bloomfilter`](github.com/bits-and-blooms/bloom/v3)" + `

The field ` + "`" + bloomCacheFieldInitValuesLabel + "`" + ` can be used to pre-populate the memory cache with any number of keys:

` + "```yml" + `
cache_resources:
  - label: foocache
    bloom:
      cap: 1024
      init_values:
        foo: t
        bar: t
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(bloomCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(bloomCacheFieldCapDefaultValue)).
		Field(service.NewFloatField(bloomCacheFieldFalsePositiveRateLabel).
			Description("false positive rate. 1% is 0.01").
			Default(bloomCacheFieldFalsePositiveRateDefaultValue)).
		Field(service.NewStringMapField(ttlruCacheFieldInitValuesLabel).
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]string{}).
			Example(map[string]string{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			})).
		Field(service.NewBoolField(bloomCacheFieldStrictLabel).
			Description("Bloom filters does not support delete operations. If strict mode is true, such operations will fail.").
			Default(bloomCacheFieldStrictDefaultValue).
			Advanced().
			Optional()).
		Footnotes(`This component implements all cache operations except *delete*, however it does not store any value, only the keys.

The main intent is to be used on deduplication.

When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

	return spec
}

func init() {
	err := service.RegisterCache(
		"bloom", bloomCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := bloomMemCacheFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func bloomMemCacheFromConfig(conf *service.ParsedConfig, log *service.Logger) (*bloomCacheAdapter, error) {
	capacity, err := conf.FieldInt(bloomCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	fp, err := conf.FieldFloat(bloomCacheFieldFalsePositiveRateLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringMap(bloomCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	var strict bool
	if conf.Contains(bloomCacheFieldStrictLabel) {
		strict, err = conf.FieldBool(bloomCacheFieldStrictLabel)
		if err != nil {
			return nil, err
		}
	}

	bloomLogger := log.With("cache", "bloom")

	return bloomMemCache(capacity, fp, initValues, bloomLogger, strict)
}

//------------------------------------------------------------------------------

var (
	errInvalidBloomCacheCapacityValue          = fmt.Errorf("invalid bloom cache parameter capacity: must be bigger than 0")
	errInvalidBloomCacheFalsePositiveRateValue = fmt.Errorf("invalid bloom cache parameter fp: must be bigger than 0")
)

func bloomMemCache(capacity int,
	fp float64,
	initValues map[string]string,
	log *service.Logger,
	strict bool,
) (ca *bloomCacheAdapter, err error) {
	if capacity <= 0 {
		return nil, errInvalidBloomCacheCapacityValue
	}

	if fp <= 0 {
		return nil, errInvalidBloomCacheFalsePositiveRateValue
	}

	inner := bloom.NewWithEstimates(uint(capacity), fp)

	for key := range initValues {
		inner.AddString(key)
	}

	ca = &bloomCacheAdapter{
		inner:  inner,
		log:    log,
		strict: strict,
	}

	for _, key := range initValues {
		_ = ca.inner.AddString(key)
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var (
	_ service.Cache = (*bloomCacheAdapter)(nil)
	_ batchedCache  = (*bloomCacheAdapter)(nil)
)

type bloomCacheAdapter struct {
	inner *bloom.BloomFilter

	log *service.Logger

	strict bool

	sync.RWMutex
}

func (ca *bloomCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	ca.RWMutex.RLock()

	ok := ca.inner.TestString(key)

	ca.RWMutex.RUnlock()

	if !ok {
		return nil, service.ErrKeyNotFound
	}

	return []byte{'t'}, nil
}

func (ca *bloomCacheAdapter) Set(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	_ = ca.inner.AddString(key)

	ca.RWMutex.Unlock()

	return nil
}

func (ca *bloomCacheAdapter) SetMulti(_ context.Context, items ...service.CacheItem) error {
	ca.RWMutex.Lock()

	for _, item := range items {
		_ = ca.inner.AddString(item.Key)
	}

	ca.RWMutex.Unlock()

	return nil
}

func (ca *bloomCacheAdapter) Add(ctx context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.TestOrAddString(key)

	ca.RWMutex.Unlock()

	if ok {
		return service.ErrKeyAlreadyExists
	}

	return nil
}

func (ca *bloomCacheAdapter) Delete(_ context.Context, key string) error {
	if ca.strict {
		return errors.ErrUnsupported
	}

	return nil
}

func (ca *bloomCacheAdapter) Close(_ context.Context) error {
	return nil
}
