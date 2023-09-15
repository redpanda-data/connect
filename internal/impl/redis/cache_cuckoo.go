package redis

import (
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/public/service"
)

func redisCuckooCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Millisecond * 500
	retriesDefaults.MaxInterval = time.Second
	retriesDefaults.MaxElapsedTime = time.Second * 5

	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Redis instance as a probabilistic cache using cuckoo filters.`).
		Description(`Cuckoo filters are a probabilistic data structure that checks for presence of an element in a set.

A Cuckoo filter, just like a Bloom filter, is a probabilistic data structure in Redis Stack that enables you to check if an element is present in a set in a very fast and space efficient way, while also allowing for deletions and showing better performance than Bloom in some scenarios.

While the Bloom filter is a bit array with flipped bits at positions decided by the hash function, a Cuckoo filter is an array of buckets, storing fingerprints of the values in one of the buckets at positions decided by the two hash functions. A membership query for item x searches the possible buckets for the fingerprint of x, and returns true if an identical fingerprint is found. A cuckoo filter's fingerprint size will directly determine the false positive rate.

See more [here](https://redis.io/docs/data-types/probabilistic/cuckoo-filter/).`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("filter_key").
			Description(`Specify the key used by the probabilistic cuckoo filter. 

If the key does not exists, we will create one using the default capacity.`).
			Examples(
				"cf:benthos",
				"cache:cf:benthos",
				"cuckoo-filter:benthos:20230919",
				"dedupe:cf:benthos:1694774600",
				"anything-descriptive",
			)).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced()).
		Footnotes(`This component implements all cache operations, however it does not store any value, only the keys.

			The main intent is to be used on deduplication.
			
			When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

	return spec
}

func init() {
	err := service.RegisterCache(
		"redis_cuckoo", redisCuckooCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newRedisCuckooCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newRedisCuckooCacheFromConfig(conf *service.ParsedConfig) (*redisCache, error) {
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

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}

	cacheAdaptor, err := NewCuckooFilterRedisCacheAdaptor(client, filterKey)
	if err != nil {
		return nil, err
	}

	return newRedisCache(ttl, prefix, cacheAdaptor, backOff), nil
}
