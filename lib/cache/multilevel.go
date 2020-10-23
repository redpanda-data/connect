package cache

import (
	"fmt"
	"time"

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

resources:
  caches:
    leveled:
      multilevel: [ hot, cold ]

    hot:
      memory:
        ttl: 300

    cold:
      memcached:
        addresses: [ TODO:11211 ]
        ttl: 3600
` + "```" + `

Using this config when a target key already exists in our local memory cache we
won't bother hitting the remote memcached instance.`,
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
	missing := []string{}
	for _, c := range conf.Multilevel {
		if _, err := mgr.GetCache(c); err != nil {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("caches %v not found", missing)
	}
	return &Multilevel{
		mgr:    mgr,
		log:    log,
		caches: conf.Multilevel,
	}, nil
}

//------------------------------------------------------------------------------

func (l *Multilevel) setUpToLevelPassive(i int, key string, value []byte, ttl *time.Duration) {
	for j, name := range l.caches {
		if j == i {
			break
		}
		c, err := l.mgr.GetCache(name)
		if err == nil {
			err = c.Set(key, value, ttl)
		}
		if err != nil {
			l.log.Errorf("Unable to passively set key '%v' for cache '%v': %v\n", key, name, err)
		}
	}
}

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (l *Multilevel) Get(key string) ([]byte, error) {
	for i, name := range l.caches {
		c, err := l.mgr.GetCache(name)
		if err != nil {
			return nil, fmt.Errorf("unable to access cache '%v': %v", name, err)
		}
		data, err := c.Get(key)
		if err != nil {
			if err != types.ErrKeyNotFound {
				return nil, err
			}
		} else {
			l.setUpToLevelPassive(i, key, data, nil)
			return data, nil
		}
	}
	return nil, types.ErrKeyNotFound
}

// Set attempts to set the value of a key.
func (l *Multilevel) Set(key string, value []byte, ttl *time.Duration) error {
	for _, name := range l.caches {
		c, err := l.mgr.GetCache(name)
		if err != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, err)
		}
		if err = c.Set(key, value, ttl); err != nil {
			return err
		}
	}
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (l *Multilevel) SetMulti(items map[string][]byte, t *time.Duration) error {
	for _, name := range l.caches {
		c, err := l.mgr.GetCache(name)
		if err != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, err)
		}
		if err = c.SetMulti(items, t); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (l *Multilevel) Add(key string, value []byte, ttl *time.Duration) error {
	for i := 0; i < len(l.caches)-1; i++ {
		c, err := l.mgr.GetCache(l.caches[i])
		if err != nil {
			return fmt.Errorf("unable to access cache '%v': %v", l.caches[i], err)
		}
		if _, err = c.Get(key); err != nil {
			if err != types.ErrKeyNotFound {
				return err
			}
		} else {
			return types.ErrKeyAlreadyExists
		}
	}
	c, err := l.mgr.GetCache(l.caches[len(l.caches)-1])
	if err != nil {
		return fmt.Errorf("unable to access cache '%v': %v", l.caches[len(l.caches)-1], err)
	}
	if err = c.Add(key, value, ttl); err != nil {
		return err
	}
	for i := len(l.caches) - 2; i >= 0; i-- {
		if c, err = l.mgr.GetCache(l.caches[i]); err != nil {
			return fmt.Errorf("unable to access cache '%v': %v", l.caches[i], err)
		}
		if err = c.Set(key, value, ttl); err != nil {
			return err
		}
	}
	return nil
}

// Delete attempts to remove a key.
func (l *Multilevel) Delete(key string) error {
	for _, name := range l.caches {
		c, err := l.mgr.GetCache(name)
		if err != nil {
			return fmt.Errorf("unable to access cache '%v': %v", name, err)
		}
		if err = c.Delete(key); err != nil && err != types.ErrKeyNotFound {
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
