package pure

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

func localRatelimitConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`The local rate limit is a simple X every Y type rate limit that can be shared across any number of components within the pipeline but does not support distributed rate limits across multiple running instances of Benthos.`).
		Field(service.NewIntField("count").
			Description("The maximum number of requests to allow for a given period of time.").
			Default(1000)).
		Field(service.NewDurationField("interval").
			Description("The time window to limit requests by.").
			Default("1s"))

	return spec
}

func init() {
	err := service.RegisterRateLimit(
		"local", localRatelimitConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return newLocalRatelimitFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newLocalRatelimitFromConfig(conf *service.ParsedConfig) (*localRatelimit, error) {
	count, err := conf.FieldInt("count")
	if err != nil {
		return nil, err
	}
	interval, err := conf.FieldDuration("interval")
	if err != nil {
		return nil, err
	}
	return newLocalRatelimit(count, interval)
}

//------------------------------------------------------------------------------

type localRatelimit struct {
	mut         sync.Mutex
	bucket      int
	lastRefresh time.Time

	size   int
	period time.Duration
}

func newLocalRatelimit(count int, interval time.Duration) (*localRatelimit, error) {
	if count <= 0 {
		return nil, errors.New("count must be larger than zero")
	}
	return &localRatelimit{
		bucket:      count,
		lastRefresh: time.Now(),
		size:        count,
		period:      interval,
	}, nil
}

func (r *localRatelimit) Access(ctx context.Context) (time.Duration, error) {
	r.mut.Lock()
	r.bucket--

	if r.bucket < 0 {
		r.bucket = 0
		remaining := r.period - time.Since(r.lastRefresh)

		if remaining > 0 {
			r.mut.Unlock()
			return remaining, nil
		}
		r.bucket = r.size - 1
		r.lastRefresh = time.Now()
	}
	r.mut.Unlock()
	return 0, nil
}

func (r *localRatelimit) Close(ctx context.Context) error {
	return nil
}
