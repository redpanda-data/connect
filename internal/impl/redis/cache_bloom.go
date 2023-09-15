package redis

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/public/service"
)

func redisBloomCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Millisecond * 500
	retriesDefaults.MaxInterval = time.Second
	retriesDefaults.MaxElapsedTime = time.Second * 5

	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Redis instance as a probabilistic cache using bloom filters`).
		Description(`Bloom filters are a probabilistic data structure that checks for presence of an element in a set.

A Bloom filter is a probabilistic data structure in Redis Stack that enables you to check if an element is present in a set using a very small memory space of a fixed size.

Instead of storing all of the elements in the set, Bloom Filters store only the elements' hashed representation, thus sacrificing some precision. The trade-off is that Bloom Filters are very space-efficient and fast.

A Bloom filter can guarantee the absence of an element from a set, but it can only give an estimation about its presence. So when it responds that an element is not present in a set (a negative answer), you can be sure that indeed is the case. But one out of every N positive answers will be wrong. Even though it looks unusual at a first glance, this kind of uncertainty still has its place in computer science. There are many cases out there where a negative answer will prevent more costly operations, for example checking if a username has been taken, if a credit card has been reported as stolen, if a user has already seen an ad and much more.

See more [here](https://redis.io/docs/data-types/probabilistic/bloom-filter/).`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("filter_key").
			Description(`Specify the key used by the probabilistic bloom filter. 

If the key does not exists, we will create one using the default capacity.`).
			Examples(
				"bf:benthos",
				"cache:bf:benthos",
				"bloom-filer:benthos:20230919",
				"dedupe:bf:benthos:1694774600",
				"anything-descriptive",
			)).
		Field(service.NewBoolField("strict").
			Description("if `true`, bloom filter will fail on delete operations").
			Default(false).
			Advanced()).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced()).
		Footnotes(`This component implements all cache operations, however it does not store any value, only the keys.

			The main intent is to be used on deduplication.
			
			When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

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

	filterKey, err := conf.FieldString("filter_key")
	if err != nil {
		return nil, err
	}

	var strict bool
	if conf.Contains("strict") {
		strict, err = conf.FieldBool("strict")
		if err != nil {
			return nil, err
		}
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}

	var insertOpts *redis.BFInsertOptions
	cacheAdaptor, err := NewBloomFilterRedisCacheAdaptor(client, filterKey, strict, insertOpts)
	if err != nil {
		return nil, err
	}

	return newRedisCache(ttl, prefix, cacheAdaptor, backOff), nil
}
