package pure

import (
	"context"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"

	"github.com/benthosdev/benthos/v4/public/service"
)

func memCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a map held in memory. This cache is therefore reset every time the service restarts. Each item in the cache has a TTL set from the moment it was last edited, after which it will be removed during the next compaction.`).
		Description(`The compaction interval determines how often the cache is cleared of expired items, and this process is only triggered on writes to the cache. Access to the cache is blocked during this process.

Item expiry can be disabled entirely by setting the ` + "`compaction_interval`" + ` to an empty string.

The field ` + "`init_values`" + ` can be used to prepopulate the memory cache with any number of key/value pairs which are exempt from TTLs:

` + "```yaml" + `
cache_resources:
  - label: foocache
    memory:
      default_ttl: 60s
      init_values:
        foo: bar
` + "```" + `

These values can be overridden during execution, at which point the configured TTL is respected as usual.`).
		Field(service.NewDurationField("default_ttl").
			Description("The default TTL of each item. After this period an item will be eligible for removal during the next compaction.").
			Default("5m")).
		Field(service.NewDurationField("compaction_interval").
			Description("The period of time to wait before each compaction, at which point expired items are removed. This field can be set to an empty string in order to disable compactions/expiry entirely.").
			Default("60s")).
		Field(service.NewStringMapField("init_values").
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			})).
		Field(service.NewIntField("shards").
			Description("A number of logical shards to spread keys across, increasing the shards can have a performance benefit when processing a large number of keys.").
			Default(1).
			Advanced())
	return spec
}

func init() {
	err := service.RegisterCache(
		"memory", memCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := newMemCacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func newMemCacheFromConfig(conf *service.ParsedConfig) (*memoryCache, error) {
	ttl, err := conf.FieldDuration("default_ttl")
	if err != nil {
		return nil, err
	}

	var compInterval time.Duration
	if test, _ := conf.FieldString("compaction_interval"); test != "" {
		if compInterval, err = conf.FieldDuration("compaction_interval"); err != nil {
			return nil, err
		}
	}

	nShards, err := conf.FieldInt("shards")
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringMap("init_values")
	if err != nil {
		return nil, err
	}

	return newMemCache(ttl, compInterval, nShards, initValues), nil
}

//------------------------------------------------------------------------------

type item struct {
	value   []byte
	expires time.Time
}

type shard struct {
	items map[string]item

	compInterval   time.Duration
	lastCompaction time.Time

	sync.RWMutex
}

func (s *shard) isExpired(i item) bool {
	if s.compInterval == 0 {
		return false
	}
	if i.expires.IsZero() {
		return false
	}
	return i.expires.Before(time.Now())
}

func (s *shard) compaction() {
	if s.compInterval == 0 {
		return
	}
	if time.Since(s.lastCompaction) < s.compInterval {
		return
	}
	for k, v := range s.items {
		if s.isExpired(v) {
			delete(s.items, k)
		}
	}
	s.lastCompaction = time.Now()
}

//------------------------------------------------------------------------------

func newMemCache(ttl, compInterval time.Duration, nShards int, initValues map[string]string) *memoryCache {
	m := &memoryCache{
		defaultTTL: ttl,
	}

	if nShards <= 1 {
		m.shards = []*shard{
			{
				items:          map[string]item{},
				compInterval:   compInterval,
				lastCompaction: time.Now(),
			},
		}
	} else {
		for i := 0; i < nShards; i++ {
			m.shards = append(m.shards, &shard{
				items:          map[string]item{},
				compInterval:   compInterval,
				lastCompaction: time.Now(),
			})
		}
	}

	for k, v := range initValues {
		m.getShard(k).items[k] = item{
			value:   []byte(v),
			expires: time.Time{},
		}
	}

	return m
}

type memoryCache struct {
	shards     []*shard
	defaultTTL time.Duration
}

func (m *memoryCache) getShard(key string) *shard {
	if len(m.shards) == 1 {
		return m.shards[0]
	}
	h := xxhash.New64()
	_, _ = h.WriteString(key)
	return m.shards[h.Sum64()%uint64(len(m.shards))]
}

func (m *memoryCache) Get(_ context.Context, key string) ([]byte, error) {
	shard := m.getShard(key)
	shard.RLock()
	k, exists := shard.items[key]
	shard.RUnlock()
	if !exists {
		return nil, service.ErrKeyNotFound
	}
	// Simulate compaction by returning ErrKeyNotFound if ttl expired.
	if shard.isExpired(k) {
		return nil, service.ErrKeyNotFound
	}
	return k.value, nil
}

func (m *memoryCache) Set(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	var expires time.Time
	if ttl != nil {
		expires = time.Now().Add(*ttl)
	} else {
		expires = time.Now().Add(m.defaultTTL)
	}
	shard := m.getShard(key)
	shard.Lock()
	shard.compaction()
	shard.items[key] = item{value: value, expires: expires}
	shard.Unlock()
	return nil
}

func (m *memoryCache) Add(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	var expires time.Time
	if ttl != nil {
		expires = time.Now().Add(*ttl)
	} else {
		expires = time.Now().Add(m.defaultTTL)
	}
	shard := m.getShard(key)
	shard.Lock()
	if _, exists := shard.items[key]; exists {
		shard.Unlock()
		return service.ErrKeyAlreadyExists
	}
	shard.compaction()
	shard.items[key] = item{value: value, expires: expires}
	shard.Unlock()
	return nil
}

func (m *memoryCache) Delete(_ context.Context, key string) error {
	shard := m.getShard(key)
	shard.Lock()
	shard.compaction()
	delete(shard.items, key)
	shard.Unlock()
	return nil
}

func (m *memoryCache) Close(context.Context) error {
	return nil
}
