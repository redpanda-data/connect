package ratelimit

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLocal] = TypeSpec{
		constructor: NewLocal,
		Description: `
The local rate limit is a simple X every Y type rate limit that can be shared
across any number of components within the pipeline.`,
	}
}

//------------------------------------------------------------------------------

// LocalConfig is a config struct containing rate limit fields for a local rate
// limit.
type LocalConfig struct {
	Count    int    `json:"count" yaml:"count"`
	Interval string `json:"interval" yaml:"interval"`
}

// NewLocalConfig returns a local rate limit configuration struct with default
// values.
func NewLocalConfig() LocalConfig {
	return LocalConfig{
		Count:    1000,
		Interval: "1s",
	}
}

//------------------------------------------------------------------------------

// Local is a structure that tracks a rate limit, it can be shared across
// parallel processes in order to maintain a maximum rate of a protected
// resource.
type Local struct {
	mut         sync.Mutex
	bucket      int
	lastRefresh time.Time

	size   int
	period time.Duration
}

// NewLocal creates a local rate limit from a configuration struct. This type is
// safe to share and call from parallel goroutines.
func NewLocal(
	conf Config,
	mgr types.Manager,
	logger log.Modular,
	stats metrics.Type,
) (types.RateLimit, error) {
	if conf.Local.Count <= 0 {
		return nil, errors.New("count must be larger than zero")
	}
	period, err := time.ParseDuration(conf.Local.Interval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interval: %v", err)
	}
	return &Local{
		bucket:      conf.Local.Count,
		lastRefresh: time.Now(),
		size:        conf.Local.Count,
		period:      period,
	}, nil
}

//------------------------------------------------------------------------------

// Access the rate limited resource. Returns a duration or an error if the rate
// limit check fails. The returned duration is either zero (meaning the resource
// can be accessed) or a reasonable length of time to wait before requesting
// again.
func (r *Local) Access() (time.Duration, error) {
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

// CloseAsync shuts down the rate limit.
func (r *Local) CloseAsync() {
}

// WaitForClose blocks until the rate limit has closed down.
func (r *Local) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
