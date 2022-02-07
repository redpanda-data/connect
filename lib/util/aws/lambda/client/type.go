package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

//------------------------------------------------------------------------------

// Config contains configuration fields for the Lambda client.
type Config struct {
	session.Config `json:",inline" yaml:",inline"`
	Function       string `json:"function" yaml:"function"`
	Timeout        string `json:"timeout" yaml:"timeout"`
	NumRetries     int    `json:"retries" yaml:"retries"`
	RateLimit      string `json:"rate_limit" yaml:"rate_limit"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Config:     session.NewConfig(),
		Function:   "",
		Timeout:    "5s",
		NumRetries: 3,
		RateLimit:  "",
	}
}

//------------------------------------------------------------------------------

// Type is a client that performs lambda invocations.
type Type struct {
	lambda *lambda.Lambda

	conf  Config
	log   log.Modular
	stats metrics.Type
	mgr   types.Manager

	timeout time.Duration

	mCount    metrics.StatCounter
	mErr      metrics.StatCounter
	mSucc     metrics.StatCounter
	mLimited  metrics.StatCounter
	mLimitFor metrics.StatCounter
	mLimitErr metrics.StatCounter
	mLatency  metrics.StatTimer
}

// New returns a Lambda client.
func New(conf Config, opts ...func(*Type)) (*Type, error) {
	l := Type{
		conf:  conf,
		log:   log.Noop(),
		stats: metrics.Noop(),
		mgr:   types.NoopMgr(),
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if l.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if conf.Function == "" {
		return nil, errors.New("lambda function must not be empty")
	}

	for _, opt := range opts {
		opt(&l)
	}

	l.mCount = l.stats.GetCounter("count")
	l.mSucc = l.stats.GetCounter("success")
	l.mErr = l.stats.GetCounter("error")
	l.mLimited = l.stats.GetCounter("rate_limit.count")
	l.mLimitFor = l.stats.GetCounter("rate_limit.total_ms")
	l.mLimitErr = l.stats.GetCounter("rate_limit.error")
	l.mLatency = l.stats.GetTimer("latency")

	sess, err := l.conf.GetSession()
	if err != nil {
		return nil, err
	}

	if conf.RateLimit != "" {
		if err := interop.ProbeRateLimit(context.Background(), l.mgr, conf.RateLimit); err != nil {
			return nil, err
		}
	}

	l.lambda = lambda.New(sess)
	return &l, nil
}

//------------------------------------------------------------------------------

// OptSetLogger sets the logger to use.
func OptSetLogger(log log.Modular) func(*Type) {
	return func(t *Type) {
		t.log = log
	}
}

// OptSetStats sets the metrics aggregator to use.
func OptSetStats(stats metrics.Type) func(*Type) {
	return func(t *Type) {
		t.stats = stats
	}
}

// OptSetManager sets the manager to use.
func OptSetManager(mgr types.Manager) func(*Type) {
	return func(t *Type) {
		t.mgr = mgr
	}
}

//------------------------------------------------------------------------------

func (l *Type) waitForAccess(ctx context.Context) bool {
	if l.conf.RateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := interop.AccessRateLimit(ctx, l.mgr, l.conf.RateLimit, func(rl types.RateLimit) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			l.log.Errorf("Rate limit error: %v\n", err)
			l.mLimitErr.Incr(1)
			period = time.Second
		}
		if period > 0 {
			if err == nil {
				l.mLimited.Incr(1)
				l.mLimitFor.Incr(period.Nanoseconds() / 1000000)
			}
			<-time.After(period)
		} else {
			return true
		}
	}
}

// InvokeV2 attempts to invoke a lambda function with a message and replaces
// its contents with the result on success, or returns an error.
func (l *Type) InvokeV2(p types.Part) error {
	l.mCount.Incr(1)

	remainingRetries := l.conf.NumRetries
	for {
		l.waitForAccess(context.Background())

		ctx, done := context.WithTimeout(context.Background(), l.timeout)
		result, err := l.lambda.InvokeWithContext(ctx, &lambda.InvokeInput{
			FunctionName: aws.String(l.conf.Function),
			Payload:      p.Get(),
		})
		done()
		if err == nil {
			if result.FunctionError != nil {
				p.Metadata().Set("lambda_function_error", *result.FunctionError)
			}
			l.mSucc.Incr(1)
			p.Set(result.Payload)
			return nil
		}

		l.mErr.Incr(1)
		remainingRetries--
		if remainingRetries < 0 {
			return err
		}
	}
}

//------------------------------------------------------------------------------
