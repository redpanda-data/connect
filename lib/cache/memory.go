// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cache

import (
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["memory"] = TypeSpec{
		constructor: NewMemory,
		description: `
The memory cache simply stores key/value pairs in a map held in memory. This
cache is therefore reset every time the service restarts. Each item in the cache
has a TTL set from the moment it was last edited, after which it will be removed
during the next compaction.

A compaction only occurs during a write where the time since the last compaction
is above the compaction interval. It is therefore possible to obtain values of
keys that have expired between compactions.`,
	}
}

//------------------------------------------------------------------------------

// MemoryConfig contains config fields for the Memory cache type.
type MemoryConfig struct {
	TTL                 int `json:"ttl" yaml:"ttl"`
	CompactionIntervalS int `json:"compaction_interval_s" yaml:"compaction_interval_s"`
}

// NewMemoryConfig creates a MemoryConfig populated with default values.
func NewMemoryConfig() MemoryConfig {
	return MemoryConfig{
		TTL:                 300, // 5 Mins
		CompactionIntervalS: 60,
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
	sync.RWMutex
}

// NewMemory creates a new Memory cache type.
func NewMemory(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return &Memory{
		items:          map[string]item{},
		ttl:            time.Second * time.Duration(conf.Memory.TTL),
		compInterval:   time.Second * time.Duration(conf.Memory.CompactionIntervalS),
		lastCompaction: time.Now(),
	}, nil
}

//------------------------------------------------------------------------------

func (m *Memory) compaction() {
	if time.Since(m.lastCompaction) < m.compInterval {
		return
	}
	for k, v := range m.items {
		if time.Since(v.ts) >= m.ttl {
			delete(m.items, k)
		}
	}
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
	m.Unlock()
	return nil
}

// Delete attempts to remove a key.
func (m *Memory) Delete(key string) error {
	m.Lock()
	m.compaction()
	delete(m.items, key)
	m.Unlock()
	return nil
}

//------------------------------------------------------------------------------
