package redis

import (
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/cenkalti/backoff/v4"
)

func redisBloomCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Millisecond * 500
	retriesDefaults.MaxInterval = time.Second
	retriesDefaults.MaxElapsedTime = time.Second * 5

	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Redis instance as a probabilistic cache using bloom filters.`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("filter_key").
			Description(`change the key used by the probabilistic filter`).
			Examples(
				"bf:benthos",
				"bloom-filter:benthos",
			).
			Default("bf:benthos").
			Optional()). // add prefix and suffix
		Field(service.NewBoolField("strict").
			Description("if true, bloom filter will fail on delete operations").
			Default(false).
			Advanced()).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"redis_bloom", redisBloomCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newRedisBloomCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newRedisBloomCacheFromConfig(conf *service.ParsedConfig) (*redisCache, error) {
	var (
		ttl    time.Duration
		prefix string
	)

	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	var opts []AdaptorOption

	if conf.Contains("filter_key") {
		filterKey, err := conf.FieldString("filter_key")
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithFilterKey(filterKey))
	}

	if conf.Contains("strict") {
		strict, err := conf.FieldBool("strict")
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithStrict(strict))
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}

	cacheAdaptor := NewBloomFilterRedisCacheAdaptor(client, opts...)

	return newRedisCache(ttl, prefix, cacheAdaptor, backOff), nil
}
