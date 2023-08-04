package pure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cuckoo "github.com/seiflotfy/cuckoofilter"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cuckooCacheFieldCapLabel        = "cap"
	cuckooCacheFieldCapDefaultValue = cuckoo.DefaultCapacity
	cuckooCacheFieldInitValuesLabel = "init_values"
)

func cuckooCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores keys in a cuckoo in-memory filter, useful for deduplication. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the cuckoo package which implements a fixed-size thread safe Cuckoo filter.

It uses the package ` + "[`cuckoofilter`](github.com/seiflotfy/cuckoofilter)" + `

The field ` + cuckooCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of keys:

` + "```yml" + `
cache_resources:
  - label: foocache
    cuckoo:
      cap: 1024
      init_values:
        - foo
        - bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(cuckooCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(cuckooCacheFieldCapDefaultValue)).
		Field(service.NewStringMapField(cuckooCacheFieldInitValuesLabel).
			Description("A table of keys that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default([]string{}).
			Example([]string{
				"Nickelback",
				"Spice Girls",
				"The Human League",
			})).
		Footnotes(`This component implements all cache operations, however it does not store any value, only the keys.

The main intent is to be used on deduplication.

When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

	return spec
}

func init() {
	err := service.RegisterCache(
		"cuckoo", cuckooCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := cuckooMemCacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func cuckooMemCacheFromConfig(conf *service.ParsedConfig) (*cuckooCacheAdapter, error) {
	capacity, err := conf.FieldInt(cuckooCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringList(cuckooCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	return cuckooMemCache(capacity, initValues)
}

//------------------------------------------------------------------------------

var (
	errInvalidCuckooCacheCapacityValue = fmt.Errorf("invalid cuckoo cache parameter capacity: must be bigger than 0")
)

func cuckooMemCache(capacity int,
	initValues []string,
) (ca *cuckooCacheAdapter, err error) {
	if capacity <= 0 {
		return nil, errInvalidCuckooCacheCapacityValue
	}

	inner := cuckoo.NewFilter(uint(capacity))

	for _, key := range initValues {
		inner.Insert([]byte(key))
	}

	ca = &cuckooCacheAdapter{
		inner: inner,
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var _ cuckooCache = (*cuckoo.Filter)(nil)

type cuckooCache interface {
	Lookup([]byte) bool
	Insert([]byte) bool
	InsertUnique([]byte) bool
	Delete([]byte) bool
}

//------------------------------------------------------------------------------

var _ service.Cache = (*cuckooCacheAdapter)(nil)

type cuckooCacheAdapter struct {
	inner cuckooCache

	sync.RWMutex
}

func (ca *cuckooCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	ca.RWMutex.RLock()

	ok := ca.inner.Lookup([]byte(key))

	ca.RWMutex.RUnlock()

	if !ok {
		return nil, service.ErrKeyNotFound
	}

	return []byte{'t'}, nil
}

var errUnableToInsertKeyIntoCuckooFilter = errors.New("unable to insert key into cuckoo filter")

func (ca *cuckooCacheAdapter) Set(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.Insert([]byte(key))

	ca.RWMutex.Unlock()

	if !ok {
		return errUnableToInsertKeyIntoCuckooFilter
	}

	return nil
}

func (ca *cuckooCacheAdapter) Add(ctx context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.InsertUnique([]byte(key))

	ca.RWMutex.Unlock()

	if !ok {
		return service.ErrKeyAlreadyExists
	}

	return nil
}

func (ca *cuckooCacheAdapter) Delete(_ context.Context, key string) error {
	ca.RWMutex.Lock()

	ca.inner.Delete([]byte(key))

	ca.RWMutex.Unlock()

	return nil
}

func (ca *cuckooCacheAdapter) Close(_ context.Context) error {
	return nil
}
