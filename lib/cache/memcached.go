package cache

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/bradfitz/gomemcache/memcache"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMemcached] = TypeSpec{
		constructor: NewMemcached,
		Description: `
Connects to a cluster of memcached services, a prefix can be specified to allow
multiple cache types to share a memcached cluster under different namespaces.`,
	}
}

//------------------------------------------------------------------------------

// MemcachedConfig is a config struct for a memcached connection.
type MemcachedConfig struct {
	Addresses   []string `json:"addresses" yaml:"addresses"`
	Prefix      string   `json:"prefix" yaml:"prefix"`
	TTL         int32    `json:"ttl" yaml:"ttl"`
	Retries     int      `json:"retries" yaml:"retries"`
	RetryPeriod string   `json:"retry_period" yaml:"retry_period"`
}

// NewMemcachedConfig returns a MemcachedConfig with default values.
func NewMemcachedConfig() MemcachedConfig {
	return MemcachedConfig{
		Addresses:   []string{"localhost:11211"},
		Prefix:      "",
		TTL:         300,
		Retries:     3,
		RetryPeriod: "500ms",
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
	var retryPeriod time.Duration
	if tout := conf.Memcached.RetryPeriod; len(tout) > 0 {
		var err error
		if retryPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry period string: %v", err)
		}
	}
	return &Memcached{
		conf:  conf,
		log:   log,
		stats: stats,

		mLatency:       stats.GetTimer("latency"),
		mGetCount:      stats.GetCounter("get.count"),
		mGetRetry:      stats.GetCounter("get.retry"),
		mGetFailed:     stats.GetCounter("get.failed.error"),
		mGetSuccess:    stats.GetCounter("get.success"),
		mGetLatency:    stats.GetTimer("get.latency"),
		mSetCount:      stats.GetCounter("set.count"),
		mSetRetry:      stats.GetCounter("set.retry"),
		mSetFailed:     stats.GetCounter("set.failed.error"),
		mSetSuccess:    stats.GetCounter("set.success"),
		mSetLatency:    stats.GetTimer("set.latency"),
		mAddCount:      stats.GetCounter("add.count"),
		mAddDupe:       stats.GetCounter("add.failed.duplicate"),
		mAddRetry:      stats.GetCounter("add.retry"),
		mAddFailedDupe: stats.GetCounter("add.failed.duplicate"),
		mAddFailedErr:  stats.GetCounter("add.failed.error"),
		mAddSuccess:    stats.GetCounter("add.success"),
		mAddLatency:    stats.GetTimer("add.latency"),
		mDelCount:      stats.GetCounter("delete.count"),
		mDelRetry:      stats.GetCounter("delete.retry"),
		mDelFailedErr:  stats.GetCounter("delete.failed.error"),
		mDelSuccess:    stats.GetCounter("delete.success"),
		mDelLatency:    stats.GetTimer("delete.latency"),

		retryPeriod: retryPeriod,
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
		m.log.Errorf("Get command failed: %v\n", err)
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
		m.log.Errorf("Set command failed: %v\n", err)
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

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (m *Memcached) SetMulti(items map[string][]byte) error {
	// TODO: Come back and optimise this.
	for k, v := range items {
		if err := m.Set(k, v); err != nil {
			return err
		}
	}
	return nil
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
		m.log.Errorf("Add command failed: %v\n", err)
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
		m.log.Errorf("Delete command failed: %v\n", err)
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

// CloseAsync shuts down the cache.
func (m *Memcached) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (m *Memcached) WaitForClose(timeout time.Duration) error {
	return nil
}

//-----------------------------------------------------------------------------
