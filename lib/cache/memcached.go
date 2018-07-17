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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
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

	mLatency       metrics.StatTimer
	mGetCount      metrics.StatCounter
	mGetRetry      metrics.StatCounter
	mGetFailed     metrics.StatCounter
	mGetSuccess    metrics.StatCounter
	mGetLatency    metrics.StatTimer
	mSetCount      metrics.StatCounter
	mSetRetry      metrics.StatCounter
	mSetFailed     metrics.StatCounter
	mSetSuccess    metrics.StatCounter
	mSetLatency    metrics.StatTimer
	mAddCount      metrics.StatCounter
	mAddDupe       metrics.StatCounter
	mAddRetry      metrics.StatCounter
	mAddFailedDupe metrics.StatCounter
	mAddFailedErr  metrics.StatCounter
	mAddSuccess    metrics.StatCounter
	mAddLatency    metrics.StatTimer
	mDelCount      metrics.StatCounter
	mDelRetry      metrics.StatCounter
	mDelFailedErr  metrics.StatCounter
	mDelSuccess    metrics.StatCounter
	mDelLatency    metrics.StatTimer

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

		mLatency:       stats.GetTimer("cache.memcached.latency"),
		mGetCount:      stats.GetCounter("cache.memcached.get.count"),
		mGetRetry:      stats.GetCounter("cache.memcached.get.retry"),
		mGetFailed:     stats.GetCounter("cache.memcached.get.failed.error"),
		mGetSuccess:    stats.GetCounter("cache.memcached.get.success"),
		mGetLatency:    stats.GetTimer("cache.memcached.get.latency"),
		mSetCount:      stats.GetCounter("cache.memcached.set.count"),
		mSetRetry:      stats.GetCounter("cache.memcached.set.retry"),
		mSetFailed:     stats.GetCounter("cache.memcached.set.failed.error"),
		mSetSuccess:    stats.GetCounter("cache.memcached.set.success"),
		mSetLatency:    stats.GetTimer("cache.memcached.set.latency"),
		mAddCount:      stats.GetCounter("cache.memcached.add.count"),
		mAddDupe:       stats.GetCounter("cache.memcached.add.failed.duplicate"),
		mAddRetry:      stats.GetCounter("cache.memcached.add.retry"),
		mAddFailedDupe: stats.GetCounter("cache.memcached.add.failed.duplicate"),
		mAddFailedErr:  stats.GetCounter("cache.memcached.add.failed.error"),
		mAddSuccess:    stats.GetCounter("cache.memcached.add.success"),
		mAddLatency:    stats.GetTimer("cache.memcached.add.latency"),
		mDelCount:      stats.GetCounter("cache.memcached.delete.count"),
		mDelRetry:      stats.GetCounter("cache.memcached.delete.retry"),
		mDelFailedErr:  stats.GetCounter("cache.memcached.delete.failed.error"),
		mDelSuccess:    stats.GetCounter("cache.memcached.delete.success"),
		mDelLatency:    stats.GetTimer("cache.memcached.del.latency"),

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
	m.mGetCount.Incr(1)
	tStarted := time.Now()

	item, err := m.mc.Get(m.conf.Memcached.Prefix + key)
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.mGetRetry.Incr(1)
		item, err = m.mc.Get(m.conf.Memcached.Prefix + key)
	}

	latency := int64(time.Since(tStarted))
	m.mGetLatency.Timing(latency)
	m.mLatency.Timing(latency)

	if err != nil {
		m.mGetFailed.Incr(1)
		return nil, err
	}

	m.mGetSuccess.Incr(1)
	return item.Value, err
}

// Set attempts to set the value of a key.
func (m *Memcached) Set(key string, value []byte) error {
	m.mSetCount.Incr(1)
	tStarted := time.Now()

	err := m.mc.Set(m.getItemFor(key, value))
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.mSetRetry.Incr(1)
		err = m.mc.Set(m.getItemFor(key, value))
	}
	if err != nil {
		m.mSetFailed.Incr(1)
	} else {
		m.mSetSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	m.mSetLatency.Timing(latency)
	m.mLatency.Timing(latency)

	return err
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (m *Memcached) Add(key string, value []byte) error {
	m.mAddCount.Incr(1)
	tStarted := time.Now()

	err := m.mc.Add(m.getItemFor(key, value))
	if memcache.ErrNotStored == err {
		m.mAddFailedDupe.Incr(1)

		latency := int64(time.Since(tStarted))
		m.mAddLatency.Timing(latency)
		m.mLatency.Timing(latency)

		return types.ErrKeyAlreadyExists
	}
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.mAddRetry.Incr(1)
		if err := m.mc.Add(m.getItemFor(key, value)); memcache.ErrNotStored == err {
			m.mAddFailedDupe.Incr(1)

			latency := int64(time.Since(tStarted))
			m.mAddLatency.Timing(latency)
			m.mLatency.Timing(latency)

			return types.ErrKeyAlreadyExists
		}
	}
	if err != nil {
		m.mAddFailedErr.Incr(1)
	} else {
		m.mAddSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	m.mAddLatency.Timing(latency)
	m.mLatency.Timing(latency)

	return err
}

// Delete attempts to remove a key.
func (m *Memcached) Delete(key string) error {
	m.mDelCount.Incr(1)
	tStarted := time.Now()

	err := m.mc.Delete(m.conf.Memcached.Prefix + key)
	if err == memcache.ErrCacheMiss {
		err = nil
	}
	for i := 0; i < m.conf.Memcached.Retries && err != nil; i++ {
		<-time.After(m.retryPeriod)
		m.mDelRetry.Incr(1)
		if err = m.mc.Delete(m.conf.Memcached.Prefix + key); err == memcache.ErrCacheMiss {
			err = nil
		}
	}
	if err != nil {
		m.mDelFailedErr.Incr(1)
	} else {
		m.mDelSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	m.mDelLatency.Timing(latency)
	m.mLatency.Timing(latency)

	return err
}

//-----------------------------------------------------------------------------
