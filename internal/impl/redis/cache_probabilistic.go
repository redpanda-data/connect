package redis

import (
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/cenkalti/backoff/v4"
)

func redisProbabilisticCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Millisecond * 500
	retriesDefaults.MaxInterval = time.Second
	retriesDefaults.MaxElapsedTime = time.Second * 5

	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Redis instance as a probabilistic cache.`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringAnnotatedEnumField("backend", map[string]string{
			"bloom":  "uses bloom filters, does not support delete operation",
			"cuckoo": "uses cuckoo filters, supports delete operation",
		}).Description("choose the backend")).
		Field(service.NewStringField("filter_key").
			Description(`change the key used by the probabilistic filter. support strftime notation`).
			Examples(
				"bf-benthos-%Y%m%d",
				"cf-benthos-%Y%m%d",
				"my-personal-hourly-bloom-filter-key-%Y%m%d%H",
				"my-personal-hourly-cuckoo-filter-key-%Y%m%d%H",
			).
			Optional()).
		Field(service.NewStringField("location").
			Description("change the `time.Location` used to generate the filter key").
			Default("UTC").
			Advanced()).
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
		"redis_probabilistic", redisProbabilisticCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newRedisProbabilisticCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newRedisProbabilisticCacheFromConfig(conf *service.ParsedConfig) (*redisCache, error) {
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
		var filterKey string
		if filterKey, err = conf.FieldString("filter_key"); err != nil {
			return nil, err
		}

		opts = append(opts, WithFilterKey(filterKey))
	}

	if conf.Contains("location") {
		var locationStr string
		if locationStr, err = conf.FieldString("location"); err != nil {
			return nil, err
		}

		var location *time.Location
		if location, err = time.LoadLocation(locationStr); err != nil {
			return nil, err
		}

		opts = append(opts, WithLocation(location))
	}

	if conf.Contains("strict") {
		var strict bool
		if strict, err = conf.FieldBool("strict"); err != nil {
			return nil, err
		}

		opts = append(opts, WithStrict(strict))
	}

	backend, err := conf.FieldString("backend")
	if err != nil {
		return nil, err
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}

	var cacheAdaptor RedisCacheAdaptor

	switch backend {
	case "bloom":
		cacheAdaptor = NewBloomFilterRedisCacheAdaptor(client, opts...)
	case "cuckoo":
		cacheAdaptor = NewCuckooFilterRedisCacheAdaptor(client, opts...)
	}

	return newRedisCache(ttl, prefix, cacheAdaptor, backOff)
}
