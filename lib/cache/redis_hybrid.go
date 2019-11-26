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
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/dgraph-io/ristretto"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisHybrid] = TypeSpec{
		constructor: NewRedisHybrid,
		description: `
Use a in-memory cache that acts as a read-through cache onto Redis. Any changes
will cause distributed invalidation via a Redis pubsub channel, which can be
configured.`,
	}
}

//------------------------------------------------------------------------------

// RedisHybridConfig is a config struct for a redis connection and local cache.
type RedisHybridConfig struct {
	URL                 string `json:"url" yaml:"url"`
	Prefix              string `json:"prefix" yaml:"prefix"`
	Expiration          string `json:"expiration" yaml:"expiration"`
	Retries             int    `json:"retries" yaml:"retries"`
	RetryPeriod         string `json:"retry_period" yaml:"retry_period"`
	InvalidationChannel string `json:"invalidation_channel" yaml:"invalidation_channel"`
	LocalCacheSize      int    `json:"local_cache_size" yaml:"local_cache_size"`
}

// NewRedisHybridConfig returns a RedisHybridConfig with default values.
func NewRedisHybridConfig() RedisHybridConfig {
	return RedisHybridConfig{
		URL:                 "tcp://localhost:6379",
		Prefix:              "",
		Expiration:          "24h",
		Retries:             3,
		RetryPeriod:         "500ms",
		InvalidationChannel: "benthos_redis_hybrid",
		LocalCacheSize:      1 << 30,
	}
}

//------------------------------------------------------------------------------

// RedisHybrid is a cache that connects to redis servers.
type RedisHybrid struct {
	log      log.Modular
	redis    *Redis
	mem      *ristretto.Cache
	invalKey string
}

// NewRedisHybrid returns a RedisHybrid processor.
func NewRedisHybrid(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (types.Cache, error) {
	mem, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build in-memory cache: %w", err)
	}
	conf.Redis = RedisConfig{
		URL:         conf.RedisHybrid.URL,
		Prefix:      conf.RedisHybrid.Prefix,
		Expiration:  conf.RedisHybrid.Expiration,
		Retries:     conf.RedisHybrid.Retries,
		RetryPeriod: conf.RedisHybrid.RetryPeriod,
	}
	redisCache, err := NewRedis(conf, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to build redis cache: %w", err)
	}

	redis, ok := redisCache.(*Redis)
	if !ok {
		return nil, fmt.Errorf("failed to assert redis cache type")
	}

	r := &RedisHybrid{
		log:      log,
		redis:    redis,
		invalKey: conf.RedisHybrid.InvalidationChannel,
		mem:      mem,
	}
	go r.invalidationListener()
	return r, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist or if the operation failed.
func (r *RedisHybrid) Get(key string) ([]byte, error) {
	// first check in local cache
	if val, found := r.mem.Get(key); found {
		valBytes, ok := val.([]byte)
		if !ok {
			return nil, errors.New("bad type in cache")
		}
		return valBytes, nil
	}

	// get from remote cache
	val, err := r.redis.Get(key)
	if err != nil {
		return nil, err
	}

	// set in local cache if found remotely
	r.mem.Set(key, val, 0)

	return val, nil
}

func (r *RedisHybrid) invalidationListener() {
	for msg := range r.redis.client.Subscribe(r.invalKey).Channel() {
		r.mem.Del(string(msg.Payload))
	}
}

// Set attempts to set the value of a key.
func (r *RedisHybrid) Set(key string, value []byte) error {
	// set in redis
	if err := r.redis.Set(key, value); err != nil {
		return err
	}

	// send invalidation message
	if err := r.redis.client.Publish(r.invalKey, []byte(key)).Err(); err != nil {
		r.log.Errorf("redis PUBLISH error %v\n", err)
	}

	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (r *RedisHybrid) SetMulti(items map[string][]byte) error {
	// TODO: Come back and optimise this.
	for k, v := range items {
		if err := r.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (r *RedisHybrid) Add(key string, value []byte) error {
	// check local cache
	if _, found := r.mem.Get(key); found {
		return types.ErrKeyAlreadyExists
	}

	// run add on underlying redis cache
	if err := r.redis.Add(key, value); err != nil {
		return err
	}

	return nil
}

// Delete attempts to remove a key.
func (r *RedisHybrid) Delete(key string) error {
	// remove from local cache.
	//   the invalidation message would do this,
	//   but it will be idempotent and more accurate to do it twice.
	r.mem.Del(key)

	// remove from remote cache
	r.redis.Delete(key)

	// send invalidation message for key
	if err := r.redis.client.Publish(r.invalKey, []byte(key)).Err(); err != nil {
		r.log.Errorf("redis PUBLISH error %v\n", err)
	}

	return nil
}

// CloseAsync shuts down the cache.
func (r *RedisHybrid) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (r *RedisHybrid) WaitForClose(timeout time.Duration) error {
	r.redis.client.Close()
	return nil
}

//-----------------------------------------------------------------------------
