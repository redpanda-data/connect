// Copyright (c) 2018 Lorenzo Alberton
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
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["memcached"] = TypeSpec{
		constructor: NewMemcached,
		description: `
Connects to a cluster of memcached services, a prefix can be specified to allow
multiple cache types to share a memcached cluster under different namespaces.`,
	}
}

//------------------------------------------------------------------------------

// MemcachedConfig is a config struct for a memcached connection.
type MemcachedConfig struct {
	Addresses     []string `json:"addresses" yaml:"addresses"`
	Prefix        string   `json:"prefix" yaml:"prefix"`
	TTL           int32    `json:"ttl" yaml:"ttl"`
	Retries       int      `json:"retries" yaml:"retries"`
	RetryPeriodMS int      `json:"retry_period_ms" yaml:"retry_period_ms"`
}

// NewMemcachedConfig returns a MemcachedConfig with default values.
func NewMemcachedConfig() MemcachedConfig {
	return MemcachedConfig{
		Addresses:     []string{"localhost:11211"},
		Prefix:        "",
		TTL:           300,
		Retries:       3,
		RetryPeriodMS: 500,
	}
}

//------------------------------------------------------------------------------

// Memcached is a cache that connects to memcached servers.
type Memcached struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mc          *memcache.Client
	retryPeriod time.Duration
}

// NewMemcached returns a Memcached processor.
func NewMemcached(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (types.Cache, error) {
	addresses := []string{}
	for _, addr := range conf.Memcached.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				addresses = append(addresses, splitAddr)
			}
		}
	}
	return &Memcached{
		conf:  conf,
		log:   log.NewModule(".cache.memcached"),
		stats: stats,

		retryPeriod: time.Duration(conf.Memcached.RetryPeriodMS) * time.Millisecond,
		mc:          memcache.New(addresses...),
	}, nil
}

//------------------------------------------------------------------------------

// getItemFor returns a memcache.Item object ready to be stored in memcache
func (m *Memcached) getItemFor(key string, value []byte) *memcache.Item {
	return &memcache.Item{
		Key:        m.conf.Memcached.Prefix + key,
		Value:      value,
		Expiration: m.conf.Memcached.TTL,
	}
}

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist or if the operation failed.
func (m *Memcached) Get(key string) ([]byte, error) {
	m.stats.Incr("cache.memcached.get.count", 1)

	item, err := m.mc.Get(m.conf.Memcached.Prefix + key)
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.stats.Incr("cache.memcached.get.retry", 1)
		item, err = m.mc.Get(m.conf.Memcached.Prefix + key)
	}
	if err != nil {
		m.stats.Incr("cache.memcached.get.failed.error", 1)
		return nil, err
	}

	m.stats.Incr("cache.memcached.get.success", 1)
	return item.Value, err
}

// Set attempts to set the value of a key.
func (m *Memcached) Set(key string, value []byte) error {
	m.stats.Incr("cache.memcached.set.count", 1)

	err := m.mc.Set(m.getItemFor(key, value))
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.stats.Incr("cache.memcached.set.retry", 1)
		err = m.mc.Set(m.getItemFor(key, value))
	}
	if err != nil {
		m.stats.Incr("cache.memcached.set.failed.error", 1)
	} else {
		m.stats.Incr("cache.memcached.set.success", 1)
	}

	return err
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (m *Memcached) Add(key string, value []byte) error {
	m.stats.Incr("cache.memcached.add.count", 1)

	err := m.mc.Add(m.getItemFor(key, value))
	if memcache.ErrNotStored == err {
		m.stats.Incr("cache.memcached.add.failed.duplicate", 1)
		return types.ErrKeyAlreadyExists
	}
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.stats.Incr("cache.memcached.add.retry", 1)
		if err := m.mc.Add(m.getItemFor(key, value)); memcache.ErrNotStored == err {
			m.stats.Incr("cache.memcached.add.failed.duplicate", 1)
			return types.ErrKeyAlreadyExists
		}
	}
	if err != nil {
		m.stats.Incr("cache.memcached.add.failed.error", 1)
	} else {
		m.stats.Incr("cache.memcached.add.success", 1)
	}
	return err
}

// Delete attempts to remove a key.
func (m *Memcached) Delete(key string) error {
	m.stats.Incr("cache.memcached.delete.count", 1)

	err := m.mc.Delete(m.conf.Memcached.Prefix + key)
	if err == memcache.ErrCacheMiss {
		err = nil
	}
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.stats.Incr("cache.memcached.delete.retry", 1)
		if err = m.mc.Delete(m.conf.Memcached.Prefix + key); err == memcache.ErrCacheMiss {
			err = nil
		}
	}
	if err != nil {
		m.stats.Incr("cache.memcached.delete.failed.error", 1)
	} else {
		m.stats.Incr("cache.memcached.delete.success", 1)
	}
	return err
}

//-----------------------------------------------------------------------------
