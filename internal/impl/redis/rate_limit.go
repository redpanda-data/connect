package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/public/service"
)

func redisRatelimitConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Summary(`A rate limit implementation using Redis. It works by using a simple token bucket algorithm to limit the number of requests to a given count within a given time period. The rate limit is shared across all instances of Benthos that use the same Redis instance, which must all have a consistent count and interval.`).
		Version("4.12.0")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec.Field(service.NewIntField("count").
		Description("The maximum number of messages to allow for a given period of time.").
		Default(1000).LintRule(`root = if this <= 0 { [ "count must be larger than zero" ] }`)).
		Field(service.NewDurationField("interval").
			Description("The time window to limit requests by.").
			Default("1s")).
		Field(service.NewStringField("key").
			Description("The key to use for the rate limit."))

	return spec
}

func init() {
	err := service.RegisterRateLimit(
		"redis", redisRatelimitConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return newRedisRatelimitFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type redisRatelimit struct {
	size   int
	key    string
	period time.Duration

	client redis.UniversalClient

	accessScript *redis.Script
}

func newRedisRatelimitFromConfig(conf *service.ParsedConfig) (*redisRatelimit, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	count, err := conf.FieldInt("count")
	if err != nil {
		return nil, err
	}

	interval, err := conf.FieldDuration("interval")
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldString("key")
	if err != nil {
		return nil, err
	}

	if count <= 0 {
		return nil, fmt.Errorf("count must be larger than zero")
	}

	return &redisRatelimit{
		size:   count,
		period: interval,
		client: client,
		key:    key,
		accessScript: redis.NewScript(`
local current = redis.call("INCR",KEYS[1])

if current == 1 then
    redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
end

if current > tonumber(ARGV[1]) then
	return redis.call("PTTL", KEYS[1])
end

return 0
`),
	}, nil
}

//------------------------------------------------------------------------------

func (r *redisRatelimit) Access(ctx context.Context) (time.Duration, error) {
	result := r.accessScript.Run(ctx, r.client, []string{r.key}, r.size, int(r.period.Milliseconds()))

	if result.Err() != nil {
		return 0, fmt.Errorf("accessing redis rate limit: %w", result.Err())
	}

	if result.Val() == 0 {
		return 0, nil
	}

	return time.Duration((result.Val().(int64)) * int64(time.Millisecond)), nil
}

func (r *redisRatelimit) Close(ctx context.Context) error {
	return nil
}
