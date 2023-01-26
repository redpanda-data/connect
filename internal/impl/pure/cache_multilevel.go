package pure

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

func multilevelCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Combines multiple caches as levels, performing read-through and write-through operations across them.`).
		Field(service.NewStringListField("")).
		Example(
			"Hot and cold cache",
			"The multilevel cache is useful for reducing traffic against a remote cache by routing it through a local cache. In the following example requests will only go through to the memcached server if the local memory cache is missing the key.",
			`
pipeline:
  processors:
    - branch:
        processors:
          - cache:
              resource: leveled
              operator: get
              key: ${! json("key") }
          - catch:
            - mapping: 'root = {"err":error()}'
        result_map: 'root.result = this'

cache_resources:
  - label: leveled
    multilevel: [ hot, cold ]

  - label: hot
    memory:
      default_ttl: 60s

  - label: cold
    memcached:
      addresses: [ TODO:11211 ]
      default_ttl: 60s
`)
	return spec
}

func init() {
	err := service.RegisterCache(
		"multilevel", multilevelCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			levels, err := conf.FieldStringList()
			if err != nil {
				return nil, err
			}
			return newMultilevelCache(levels, mgr, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type cacheProvider interface {
	AccessCache(ctx context.Context, name string, fn func(c service.Cache)) error
}

type multilevelCache struct {
	mgr    cacheProvider
	log    *service.Logger
	caches []string
}

func newMultilevelCache(levels []string, mgr cacheProvider, log *service.Logger) (service.Cache, error) {
	if len(levels) < 2 {
		return nil, fmt.Errorf("expected at least two cache levels, found %v", len(levels))
	}
	// TODO: Probe caches
	// for _, name := range levels {
	// }
	return &multilevelCache{
		mgr:    mgr,
		log:    log,
		caches: levels,
	}, nil
}

//------------------------------------------------------------------------------

func (l *multilevelCache) setUpToLevelPassive(ctx context.Context, i int, key string, value []byte, caches []string) {
	for _, name := range caches {
		// key already exists in this cache, skip it
		if name == l.caches[i] {
			continue
		}
		var setErr error
		if err := l.mgr.AccessCache(ctx, name, func(c service.Cache) {
			setErr = c.Set(ctx, key, value, nil)
		}); err != nil {
			l.log.Errorf("Unable to passively set key '%v' for cache '%v': %v", key, name, err)
		}
		if setErr != nil {
			l.log.Errorf("Unable to passively set key '%v' for cache '%v': %v", key, name, setErr)
		} else {
			fmt.Printf("Adding Key '%s' to '%s'\n", key, name)
		}
	}
}

func (l *multilevelCache) Get(ctx context.Context, key string) ([]byte, error) {
	var misses []string
	for i, name := range l.caches {
		var data []byte
		var err error
		if cerr := l.mgr.AccessCache(ctx, name, func(c service.Cache) {
			data, err = c.Get(ctx, key)
		}); cerr != nil {
			return nil, fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil {
			if err != service.ErrKeyNotFound {
				return nil, err
			}
			misses = append(misses, name)
		} else {
			// propogate key/data to caches where there was a miss
			go l.setUpToLevelPassive(ctx, i, key, data, misses)
			return data, nil
		}
	}
	return nil, service.ErrKeyNotFound
}

func (l *multilevelCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	for _, name := range l.caches {
		var err error
		if cerr := l.mgr.AccessCache(ctx, name, func(c service.Cache) {
			err = c.Set(ctx, key, value, ttl)
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *multilevelCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	// Check the hot cache for the key
	var err error
	if cerr := l.mgr.AccessCache(ctx, l.caches[0], func(c service.Cache) {
		_, err = c.Get(ctx, key)
	}); cerr != nil {
		return fmt.Errorf("unable to access cache '%v': %v", l.caches[0], cerr)
	}
	if err != nil {
		if err != service.ErrKeyNotFound {
			return err
		}
	} else {
		return service.ErrKeyAlreadyExists
	}

	// Add the key to the hot-cache
	if cerr := l.mgr.AccessCache(ctx, l.caches[0], func(c service.Cache) {
		err = c.Add(ctx, key, value, ttl)
	}); cerr != nil {
		return fmt.Errorf("unable to access cache '%v': %v", l.caches[0], cerr)
	}
	if err != nil {
		return err
	}

	// spawn a go routine to add the key to all of the other caches
	go func(caches []string) {
		for _, name := range caches {
			l.mgr.AccessCache(ctx, name, func(c service.Cache) {
				c.Add(ctx, key, value, ttl)
			})
		}
	}(l.caches[1:])

	return nil
}

func (l *multilevelCache) Delete(ctx context.Context, key string) error {
	for _, name := range l.caches {
		var err error
		if cerr := l.mgr.AccessCache(ctx, name, func(c service.Cache) {
			err = c.Delete(ctx, key)
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil && err != service.ErrKeyNotFound {
			return err
		}
	}
	return nil
}

func (l *multilevelCache) Close(ctx context.Context) error {
	return nil
}
