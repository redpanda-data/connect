package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMultilevel] = TypeSpec{
		constructor: NewMultilevel,
		Summary: `
Combines multiple caches as levels, performing read-through and write-through
operations across them.`,
		Footnotes: `
For the Add command this cache first checks all levels except the last for the
key. If the key is not found it is added to the final cache level, if that
succeeds all higher cache levels have the key set.

## Examples

It's possible to use multilevel to create a warm cache in memory above a cold
remote cache:

` + "```yaml" + `
pipeline:
  processors:
    - branch:
        processors:
          - cache:
              resource: leveled
              operator: get
              key: ${! json("key") }
          - catch:
            - bloblang: 'root = {"err":error()}'
        result_map: 'root.result = this'

cache_resources:
  - label: leveled
    multilevel: [ hot, cold ]

  - label: hot
    memory:
      ttl: 300

  - label: cold
    memcached:
      addresses: [ TODO:11211 ]
      ttl: 3600
` + "```" + `

Using this config when a target key already exists in our local memory cache we
won't bother hitting the remote memcached instance.`,
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeString).HasDefault([]string{}),
	}
}

//------------------------------------------------------------------------------

// MultilevelConfig contains config fields for the Multilevel cache type.
type MultilevelConfig []string

// NewMultilevelConfig creates a MultilevelConfig populated with default values.
func NewMultilevelConfig() MultilevelConfig {
	return []string{}
}

//------------------------------------------------------------------------------

// Multilevel is a file system based cache implementation.
type Multilevel struct {
	mgr    types.Manager
	log    log.Modular
	caches []string
}

// NewMultilevel creates a new Multilevel cache type.
func NewMultilevel(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	if len(conf.Multilevel) < 2 {
		return nil, fmt.Errorf("expected at least two cache levels, found %v", len(conf.Multilevel))
	}
	for _, name := range conf.Multilevel {
		if err := interop.ProbeCache(context.Background(), mgr, name); err != nil {
			return nil, err
		}
	}
	return &Multilevel{
		mgr:    mgr,
		log:    log,
		caches: conf.Multilevel,
	}, nil
}

//------------------------------------------------------------------------------

func (l *Multilevel) setUpToLevelPassive(i int, key string, value []byte) {
	for j, name := range l.caches {
		if j == i {
			break
		}
		var setErr error
		err := interop.AccessCache(context.Background(), l.mgr, name, func(c types.Cache) {
			setErr = c.Set(key, value)
		})
		if err != nil {
			l.log.Errorf("Unable to passively set key '%v' for cache '%v': %v\n", key, name, err)
		}
		if setErr != nil {
			l.log.Errorf("Unable to passively set key '%v' for cache '%v': %v\n", key, name, setErr)
		}
	}
}

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (l *Multilevel) Get(key string) ([]byte, error) {
	for i, name := range l.caches {
		var data []byte
		var err error
		if cerr := interop.AccessCache(context.Background(), l.mgr, name, func(c types.Cache) {
			data, err = c.Get(key)
		}); cerr != nil {
			return nil, fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil {
			if err != types.ErrKeyNotFound {
				return nil, err
			}
		} else {
			l.setUpToLevelPassive(i, key, data)
			return data, nil
		}
	}
	return nil, types.ErrKeyNotFound
}

// SetWithTTL attempts to set the value of a key.
func (l *Multilevel) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	for _, name := range l.caches {
		var err error
		if cerr := interop.AccessCache(context.Background(), l.mgr, name, func(c types.Cache) {
			if cttl, ok := c.(types.CacheWithTTL); ok {
				err = cttl.SetWithTTL(key, value, ttl)
			} else {
				err = c.Set(key, value)
			}
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Set attempts to set the value of a key.
func (l *Multilevel) Set(key string, value []byte) error {
	return l.SetWithTTL(key, value, nil)
}

// SetMultiWithTTL attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (l *Multilevel) SetMultiWithTTL(items map[string]types.CacheTTLItem) error {
	for _, name := range l.caches {
		var err error
		if cerr := interop.AccessCache(context.Background(), l.mgr, name, func(c types.Cache) {
			if cttl, ok := c.(types.CacheWithTTL); ok {
				err = cttl.SetMultiWithTTL(items)
			} else {
				sitems := make(map[string][]byte, len(items))
				for k, v := range items {
					sitems[k] = v.Value
				}
				err = c.SetMulti(sitems)
			}
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (l *Multilevel) SetMulti(items map[string][]byte) error {
	sitems := make(map[string]types.CacheTTLItem, len(items))
	for k, v := range items {
		sitems[k] = types.CacheTTLItem{
			Value: v,
		}
	}
	return l.SetMultiWithTTL(sitems)
}

// AddWithTTL attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (l *Multilevel) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	for i := 0; i < len(l.caches)-1; i++ {
		var err error
		if cerr := interop.AccessCache(context.Background(), l.mgr, l.caches[i], func(c types.Cache) {
			_, err = c.Get(key)
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", l.caches[i], cerr)
		}
		if err != nil {
			if err != types.ErrKeyNotFound {
				return err
			}
		} else {
			return types.ErrKeyAlreadyExists
		}
	}

	var err error
	if cerr := interop.AccessCache(context.Background(), l.mgr, l.caches[len(l.caches)-1], func(c types.Cache) {
		if cttl, ok := c.(types.CacheWithTTL); ok {
			err = cttl.AddWithTTL(key, value, ttl)
		} else {
			err = c.Add(key, value)
		}
	}); cerr != nil {
		return fmt.Errorf("unable to access cache '%v': %v", l.caches[len(l.caches)-1], cerr)
	}
	if err != nil {
		return err
	}

	for i := len(l.caches) - 2; i >= 0; i-- {
		if cerr := interop.AccessCache(context.Background(), l.mgr, l.caches[i], func(c types.Cache) {
			if cttl, ok := c.(types.CacheWithTTL); ok {
				err = cttl.AddWithTTL(key, value, ttl)
			} else {
				err = c.Add(key, value)
			}
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", l.caches[i], cerr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (l *Multilevel) Add(key string, value []byte) error {
	return l.AddWithTTL(key, value, nil)
}

// Delete attempts to remove a key.
func (l *Multilevel) Delete(key string) error {
	for _, name := range l.caches {
		var err error
		if cerr := interop.AccessCache(context.Background(), l.mgr, name, func(c types.Cache) {
			err = c.Delete(key)
		}); cerr != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, cerr)
		}
		if err != nil && err != types.ErrKeyNotFound {
			return err
		}
	}
	return nil
}

// CloseAsync shuts down the cache.
func (l *Multilevel) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (l *Multilevel) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
