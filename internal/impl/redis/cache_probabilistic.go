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
		Field(service.NewStringField("filter_key_template").
			Description(`change the template for the key used by the probabilistic filter. support strftime notation`).
			Examples(
				"bf-benthos-%Y%m%d",
				"cf-benthos-%Y%m%d",
				"my-personal-hourly-bloom-filter-key-%Y%m%d%H",
				"my-personal-hourly-cuckoo-filter-key-%Y%m%d%H",
			).
			Optional()).
		Field(service.NewDurationField("interval").
			Description("change the interval where we generate a new filter key from a filter key template").
			Default("600s")).
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

		// TODO add bf insert options and cf insert options

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

	if conf.Contains("filter_key_template") {
		filterKey, err := conf.FieldString("filter_key_template")
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithFilterKeyTemplate(filterKey))
	}

	if conf.Contains("interval") {
		interval, err := conf.FieldDuration("interval")
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithInterval(interval))
	}

	if conf.Contains("location") {
		locationStr, err := conf.FieldString("location")
		if err != nil {
			return nil, err
		}

		location, err := time.LoadLocation(locationStr)
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithLocation(location))
	}

	if conf.Contains("strict") {
		strict, err := conf.FieldBool("strict")
		if err != nil {
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

	return newRedisCache(ttl, prefix, cacheAdaptor, backOff), nil
}
