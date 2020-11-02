package cache

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	bredis "github.com/Jeffail/benthos/v3/internal/service/redis"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedis] = TypeSpec{
		constructor:       NewRedis,
		SupportsPerKeyTTL: true,
		Summary: `
Use a Redis instance as a cache. The expiration can be set to zero or an empty
string in order to set no expiration.`,
		FieldSpecs: bredis.ConfigDocs().Add(
			docs.FieldCommon("prefix", "An optional string to prefix item keys with in order to prevent collisions with similar services."),
			docs.FieldCommon("expiration", "An optional period after which cached items will expire."),
			docs.FieldAdvanced("retries", "The maximum number of retry attempts to make before abandoning a request."),
			docs.FieldAdvanced("retry_period", "The duration to wait between retry attempts."),
		),
	}
}

//------------------------------------------------------------------------------

// RedisConfig is a config struct for a redis connection.
type RedisConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Prefix        string `json:"prefix" yaml:"prefix"`
	Expiration    string `json:"expiration" yaml:"expiration"`
	Retries       int    `json:"retries" yaml:"retries"`
	RetryPeriod   string `json:"retry_period" yaml:"retry_period"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		Config:      bredis.NewConfig(),
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

	client, err := conf.Redis.Config.Client()
	if err != nil {
		return nil, err
	}

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

// SetWithTTL attempts to set the value of a key.
func (r *Redis) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	r.mSetCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	var t time.Duration
	if ttl != nil {
		t = *ttl
	} else {
		t = r.ttl
	}
	err := r.client.Set(key, value, t).Err()
	for i := 0; i < r.conf.Redis.Retries && err != nil; i++ {
		r.log.Errorf("Set command failed: %v\n", err)
		<-time.After(r.retryPeriod)
		r.mSetRetry.Incr(1)
		err = r.client.Set(key, value, t).Err()
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

// Set attempts to set the value of a key.
func (r *Redis) Set(key string, value []byte) error {
	return r.SetWithTTL(key, value, nil)
}

// SetMultiWithTTL attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (r *Redis) SetMultiWithTTL(items map[string]types.CacheTTLItem) error {
	// TODO: Come back and optimise this.
	for k, v := range items {
		if err := r.SetWithTTL(k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (r *Redis) SetMulti(items map[string][]byte) error {
	sitems := make(map[string]types.CacheTTLItem, len(items))
	for k, v := range items {
		sitems[k] = types.CacheTTLItem{
			Value: v,
		}
	}
	return r.SetMultiWithTTL(sitems)
}

// AddWithTTL attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (r *Redis) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	r.mAddCount.Incr(1)
	tStarted := time.Now()

	key = r.prefix + key

	var t time.Duration
	if ttl != nil {
		t = *ttl
	} else {
		t = r.ttl
	}
	set, err := r.client.SetNX(key, value, t).Result()
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
		if set, err = r.client.SetNX(key, value, t).Result(); err == nil && !set {
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

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (r *Redis) Add(key string, value []byte) error {
	return r.AddWithTTL(key, value, nil)
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
