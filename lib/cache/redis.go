package cache

import (
	"fmt"
	"net/url"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedis] = TypeSpec{
		constructor: NewRedis,
		Description: `
Use a Redis instance as a cache. The expiration can be set to zero or an empty
string in order to set no expiration.`,
	}
}

//------------------------------------------------------------------------------

// RedisConfig is a config struct for a redis connection.
type RedisConfig struct {
	URL         string `json:"url" yaml:"url"`
	Prefix      string `json:"prefix" yaml:"prefix"`
	Expiration  string `json:"expiration" yaml:"expiration"`
	Retries     int    `json:"retries" yaml:"retries"`
	RetryPeriod string `json:"retry_period" yaml:"retry_period"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		URL:         "tcp://localhost:6379",
		Prefix:      "",
		Expiration:  "24h",
		Retries:     3,
		RetryPeriod: "500ms",
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

	var retryPeriod time.Duration
	if tout := conf.Redis.RetryPeriod; len(tout) > 0 {
		var err error
		if retryPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry period string: %v", err)
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
		log:   log,
		stats: stats,

		mLatency:       stats.GetTimer("latency"),
		mGetCount:      stats.GetCounter("get.count"),
		mGetRetry:      stats.GetCounter("get.retry"),
		mGetFailed:     stats.GetCounter("get.failed.error"),
		mGetNotFound:   stats.GetCounter("get.failed.not_found"),
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
		mDelNotFound:   stats.GetCounter("delete.failed.not_found"),
		mDelSuccess:    stats.GetCounter("delete.success"),
		mDelLatency:    stats.GetTimer("delete.latency"),

		retryPeriod: retryPeriod,
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

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (r *Redis) SetMulti(items map[string][]byte) error {
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
func (r *Redis) Add(key string, value []byte) error {
	r.mAddCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	set, err := r.client.SetNX(key, value, r.ttl).Result()
	if err == nil && !set {
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
		if set, err = r.client.SetNX(key, value, r.ttl).Result(); err == nil && !set {
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

// CloseAsync shuts down the cache.
func (r *Redis) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (r *Redis) WaitForClose(timeout time.Duration) error {
	r.client.Close()
	return nil
}

//-----------------------------------------------------------------------------
