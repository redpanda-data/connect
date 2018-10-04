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
	"fmt"
	"net/url"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedis] = TypeSpec{
		constructor: NewRedis,
		description: `
Use a Redis instance as a cache. The expiration can be set to zero or an empty
string in order to set no expiration.`,
	}
}

//------------------------------------------------------------------------------

// RedisConfig is a config struct for a redis connection.
type RedisConfig struct {
	URL           string `json:"url" yaml:"url"`
	Prefix        string `json:"prefix" yaml:"prefix"`
	Expiration    string `json:"expiration" yaml:"expiration"`
	Retries       int    `json:"retries" yaml:"retries"`
	RetryPeriodMS int    `json:"retry_period_ms" yaml:"retry_period_ms"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		URL:           "tcp://localhost:6379",
		Prefix:        "",
		Expiration:    "24h",
		Retries:       3,
		RetryPeriodMS: 500,
	}
}

//------------------------------------------------------------------------------

// Redis is a cache that connects to redis servers.
type Redis struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mLatency       metrics.StatTimer
	mGetCount      metrics.StatCounter
	mGetRetry      metrics.StatCounter
	mGetFailed     metrics.StatCounter
	mGetSuccess    metrics.StatCounter
	mGetLatency    metrics.StatTimer
	mGetNotFound   metrics.StatCounter
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
	mDelNotFound   metrics.StatCounter
	mDelSuccess    metrics.StatCounter
	mDelLatency    metrics.StatTimer

	client      *redis.Client
	ttl         time.Duration
	prefix      string
	retryPeriod time.Duration
}

// NewRedis returns a Redis processor.
func NewRedis(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (types.Cache, error) {
	var ttl time.Duration
	if len(conf.Redis.Expiration) > 0 {
		var err error
		if ttl, err = time.ParseDuration(conf.Redis.Expiration); err != nil {
			return nil, fmt.Errorf("failed to parse expiration: %v", err)
		}
	}

	url, err := url.Parse(conf.Redis.URL)
	if err != nil {
		return nil, err
	}

	var pass string
	if url.User != nil {
		pass, _ = url.User.Password()
	}
	client := redis.NewClient(&redis.Options{
		Addr:     url.Host,
		Network:  url.Scheme,
		Password: pass,
	})

	return &Redis{
		conf:  conf,
		log:   log.NewModule(".cache.redis"),
		stats: stats,

		mLatency:       stats.GetTimer("cache.redis.latency"),
		mGetCount:      stats.GetCounter("cache.redis.get.count"),
		mGetRetry:      stats.GetCounter("cache.redis.get.retry"),
		mGetFailed:     stats.GetCounter("cache.redis.get.failed.error"),
		mGetNotFound:   stats.GetCounter("cache.redis.get.failed.not_found"),
		mGetSuccess:    stats.GetCounter("cache.redis.get.success"),
		mGetLatency:    stats.GetTimer("cache.redis.get.latency"),
		mSetCount:      stats.GetCounter("cache.redis.set.count"),
		mSetRetry:      stats.GetCounter("cache.redis.set.retry"),
		mSetFailed:     stats.GetCounter("cache.redis.set.failed.error"),
		mSetSuccess:    stats.GetCounter("cache.redis.set.success"),
		mSetLatency:    stats.GetTimer("cache.redis.set.latency"),
		mAddCount:      stats.GetCounter("cache.redis.add.count"),
		mAddDupe:       stats.GetCounter("cache.redis.add.failed.duplicate"),
		mAddRetry:      stats.GetCounter("cache.redis.add.retry"),
		mAddFailedDupe: stats.GetCounter("cache.redis.add.failed.duplicate"),
		mAddFailedErr:  stats.GetCounter("cache.redis.add.failed.error"),
		mAddSuccess:    stats.GetCounter("cache.redis.add.success"),
		mAddLatency:    stats.GetTimer("cache.redis.add.latency"),
		mDelCount:      stats.GetCounter("cache.redis.delete.count"),
		mDelRetry:      stats.GetCounter("cache.redis.delete.retry"),
		mDelFailedErr:  stats.GetCounter("cache.redis.delete.failed.error"),
		mDelNotFound:   stats.GetCounter("cache.redis.delete.failed.not_found"),
		mDelSuccess:    stats.GetCounter("cache.redis.delete.success"),
		mDelLatency:    stats.GetTimer("cache.redis.del.latency"),

		retryPeriod: time.Duration(conf.Redis.RetryPeriodMS) * time.Millisecond,
		ttl:         ttl,
		prefix:      conf.Redis.Prefix,
		client:      client,
	}, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist or if the operation failed.
func (r *Redis) Get(key string) ([]byte, error) {
	r.mGetCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	res, err := r.client.Get(key).Result()
	if err == redis.Nil {
		r.mGetNotFound.Incr(1)
		return nil, types.ErrKeyNotFound
	}

	for i := 0; i < r.conf.Redis.Retries && err != nil; i++ {
		r.log.Errorf("Get command failed: %v\n", err)
		<-time.After(r.retryPeriod)
		r.mGetRetry.Incr(1)
		res, err = r.client.Get(key).Result()
		if err == redis.Nil {
			r.mGetNotFound.Incr(1)
			return nil, types.ErrKeyNotFound
		}
	}

	latency := int64(time.Since(tStarted))
	r.mGetLatency.Timing(latency)
	r.mLatency.Timing(latency)

	if err != nil {
		r.mGetFailed.Incr(1)
		return nil, err
	}

	r.mGetSuccess.Incr(1)
	return []byte(res), nil
}

// Set attempts to set the value of a key.
func (r *Redis) Set(key string, value []byte) error {
	r.mSetCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	err := r.client.Set(key, value, r.ttl).Err()
	for i := 0; i < r.conf.Redis.Retries && err != nil; i++ {
		r.log.Errorf("Set command failed: %v\n", err)
		<-time.After(r.retryPeriod)
		r.mSetRetry.Incr(1)
		err = r.client.Set(key, value, r.ttl).Err()
	}
	if err != nil {
		r.mSetFailed.Incr(1)
	} else {
		r.mSetSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	r.mSetLatency.Timing(latency)
	r.mLatency.Timing(latency)

	return err
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (r *Redis) Add(key string, value []byte) error {
	r.mAddCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	set, err := r.client.SetNX(key, value, r.ttl).Result()
	if !set {
		r.mAddFailedDupe.Incr(1)

		latency := int64(time.Since(tStarted))
		r.mAddLatency.Timing(latency)
		r.mLatency.Timing(latency)

		return types.ErrKeyAlreadyExists
	}
	for i := 0; i < r.conf.Redis.Retries && err != nil; i++ {
		r.log.Errorf("Add command failed: %v\n", err)
		<-time.After(r.retryPeriod)
		r.mAddRetry.Incr(1)
		if set, err = r.client.SetNX(key, value, r.ttl).Result(); !set {
			r.mAddFailedDupe.Incr(1)

			latency := int64(time.Since(tStarted))
			r.mAddLatency.Timing(latency)
			r.mLatency.Timing(latency)

			return types.ErrKeyAlreadyExists
		}
	}
	if err != nil {
		r.mAddFailedErr.Incr(1)
	} else {
		r.mAddSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	r.mAddLatency.Timing(latency)
	r.mLatency.Timing(latency)

	return err
}

// Delete attempts to remove a key.
func (r *Redis) Delete(key string) error {
	r.mDelCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	deleted, err := r.client.Del(key).Result()
	if deleted == 0 {
		r.mDelNotFound.Incr(1)
		err = nil
	}

	for i := 0; i < r.conf.Redis.Retries && err != nil; i++ {
		r.log.Errorf("Delete command failed: %v\n", err)
		<-time.After(r.retryPeriod)
		r.mDelRetry.Incr(1)
		if deleted, err = r.client.Del(key).Result(); deleted == 0 {
			r.mDelNotFound.Incr(1)
			err = nil
		}
	}
	if err != nil {
		r.mDelFailedErr.Incr(1)
	} else {
		r.mDelSuccess.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	r.mDelLatency.Timing(latency)
	r.mLatency.Timing(latency)

	return err
}

//-----------------------------------------------------------------------------
