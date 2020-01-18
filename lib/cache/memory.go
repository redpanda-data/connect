package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMemory] = TypeSpec{
		constructor: NewMemory,
		Description: `
The memory cache simply stores key/value pairs in a map held in memory. This
cache is therefore reset every time the service restarts. Each item in the cache
has a TTL set from the moment it was last edited, after which it will be removed
during the next compaction.

A compaction only occurs during a write where the time since the last compaction
is above the compaction interval. It is therefore possible to obtain values of
keys that have expired between compactions.

The field ` + "`init_values`" + ` can be used to prepopulate the memory cache
with any number of key/value pairs which are exempt from TTLs:

` + "```yaml" + `
type: memory
memory:
  ttl: 60
  init_values:
    foo: bar
` + "```" + `

These values can be overridden during execution, at which point the configured
TTL is respected as usual.`,
	}
}

//------------------------------------------------------------------------------

// MemoryConfig contains config fields for the Memory cache type.
type MemoryConfig struct {
	TTL                int               `json:"ttl" yaml:"ttl"`
	CompactionInterval string            `json:"compaction_interval" yaml:"compaction_interval"`
	InitValues         map[string]string `json:"init_values" yaml:"init_values"`
}

// NewMemoryConfig creates a MemoryConfig populated with default values.
func NewMemoryConfig() MemoryConfig {
	return MemoryConfig{
		TTL:                300, // 5 Mins
		CompactionInterval: "60s",
		InitValues:         map[string]string{},
	}
}

//------------------------------------------------------------------------------

type item struct {
	value []byte
	ts    time.Time
}

// Memory is a memory based cache implementation.
type Memory struct {
	items          map[string]item
	ttl            time.Duration
	compInterval   time.Duration
	lastCompaction time.Time

	stats        metrics.Type
	mCompactions metrics.StatCounter
	mKeys        metrics.StatGauge

	sync.RWMutex
}

// NewMemory creates a new Memory cache type.
func NewMemory(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	var interval time.Duration
	if tout := conf.Memory.CompactionInterval; len(tout) > 0 {
		var err error
		if interval, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse compaction interval string: %v", err)
		}
	}
	items := map[string]item{}
	for k, v := range conf.Memory.InitValues {
		items[k] = item{
			value: []byte(v),
			ts:    time.Time{},
		}
	}
	return &Memory{
		items:          items,
		ttl:            time.Second * time.Duration(conf.Memory.TTL),
		compInterval:   interval,
		lastCompaction: time.Now(),
		stats:          stats,
		mCompactions:   stats.GetCounter("compaction"),
		mKeys:          stats.GetGauge("keys"),
	}, nil
}

//------------------------------------------------------------------------------

func (m *Memory) compaction() {
	if time.Since(m.lastCompaction) < m.compInterval {
		return
	}
	m.mCompactions.Incr(1)
	for k, v := range m.items {
		if v.ts.IsZero() {
			continue
		}
		if time.Since(v.ts) >= m.ttl {
			delete(m.items, k)
		}
	}
	m.lastCompaction = time.Now()
}

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (m *Memory) Get(key string) ([]byte, error) {
	m.RLock()
	k, exists := m.items[key]
	m.RUnlock()
	if !exists {
		return nil, types.ErrKeyNotFound
	}
	return k.value, nil
}

// Set attempts to set the value of a key.
func (m *Memory) Set(key string, value []byte) error {
	m.Lock()
	m.compaction()
	m.items[key] = item{value: value, ts: time.Now()}
	m.mKeys.Set(int64(len(m.items)))
	m.Unlock()
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (m *Memory) SetMulti(items map[string][]byte) error {
	m.Lock()
	m.compaction()
	for k, v := range items {
		m.items[k] = item{value: v, ts: time.Now()}
	}
	m.mKeys.Set(int64(len(m.items)))
	m.Unlock()
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (m *Memory) Add(key string, value []byte) error {
	m.Lock()
	if _, exists := m.items[key]; exists {
		m.Unlock()
		return types.ErrKeyAlreadyExists
	}
	m.compaction()
	m.items[key] = item{value: value, ts: time.Now()}
	m.mKeys.Set(int64(len(m.items)))
	m.Unlock()
	return nil
}

// Delete attempts to remove a key.
func (m *Memory) Delete(key string) error {
	m.Lock()
	m.compaction()
	delete(m.items, key)
	m.mKeys.Set(int64(len(m.items)))
	m.Unlock()
	return nil
}

// CloseAsync shuts down the cache.
func (m *Memory) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (m *Memory) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
