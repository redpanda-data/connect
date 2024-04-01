package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/public/service"
)

func redisCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Millisecond * 500
	retriesDefaults.MaxInterval = time.Second
	retriesDefaults.MaxElapsedTime = time.Second * 5

	spec := service.NewConfigSpec().
		Stable().
		Summary(`Use a Redis instance as a cache. The expiration can be set to zero or an empty string in order to set no expiration.`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("prefix").
			Description("An optional string to prefix item keys with in order to prevent collisions with similar services.").
			Optional()).
		Field(service.NewDurationField("default_ttl").
			Description("An optional default TTL to set for items, calculated from the moment the item is cached.").
			Optional().
			Advanced()).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"redis", redisCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newRedisCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newRedisCacheFromConfig(conf *service.ParsedConfig) (*redisCache, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	var prefix string
	if conf.Contains("prefix") {
		if prefix, err = conf.FieldString("prefix"); err != nil {
			return nil, err
		}
	}

	var ttl time.Duration
	if conf.Contains("default_ttl") {
		ttlTmp, err := conf.FieldDuration("default_ttl")
		if err != nil {
			return nil, err
		}
		ttl = ttlTmp
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}
	return newRedisCache(ttl, prefix, client, backOff)
}

//------------------------------------------------------------------------------

type redisCache struct {
	client     redis.UniversalClient
	defaultTTL time.Duration
	prefix     string

	boffPool sync.Pool
}

func newRedisCache(
	defaultTTL time.Duration,
	prefix string,
	client redis.UniversalClient,
	backOff *backoff.ExponentialBackOff,
) (*redisCache, error) {
	return &redisCache{
		defaultTTL: defaultTTL,
		prefix:     prefix,
		client:     client,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}, nil
}

func (r *redisCache) Get(ctx context.Context, key string) ([]byte, error) {
	boff := r.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		r.boffPool.Put(boff)
	}()

	if r.prefix != "" {
		key = r.prefix + key
	}

	for {
		res, err := r.client.Get(ctx, key).Result()
		if err == nil {
			return []byte(res), nil
		}

		if errors.Is(err, redis.Nil) {
			return nil, service.ErrKeyNotFound
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return nil, err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, err
		}
	}
}

func (r *redisCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := r.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		r.boffPool.Put(boff)
	}()

	if r.prefix != "" {
		key = r.prefix + key
	}

	var t time.Duration
	if ttl != nil {
		t = *ttl
	} else {
		t = r.defaultTTL
	}

	for {
		err := r.client.Set(ctx, key, value, t).Err()
		if err == nil {
			return nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
	}
}

func (r *redisCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := r.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		r.boffPool.Put(boff)
	}()

	if r.prefix != "" {
		key = r.prefix + key
	}

	var t time.Duration

	if ttl != nil {
		t = *ttl
	} else {
		t = r.defaultTTL
	}

	for {
		set, err := r.client.SetNX(ctx, key, value, t).Result()
		if err == nil {
			if !set {
				return service.ErrKeyAlreadyExists
			}
			return nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
	}
}

func (r *redisCache) Delete(ctx context.Context, key string) error {
	boff := r.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		r.boffPool.Put(boff)
	}()

	if r.prefix != "" {
		key = r.prefix + key
	}

	for {
		_, err := r.client.Del(ctx, key).Result()
		if err == nil {
			return nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
	}
}

func (r *redisCache) Close(ctx context.Context) error {
	return r.client.Close()
}
