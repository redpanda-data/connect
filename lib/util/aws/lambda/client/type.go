// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

//------------------------------------------------------------------------------

// Config contains configuration fields for the Lambda client.
type Config struct {
	session.Config `json:",inline" yaml:",inline"`
	Function       string `json:"function" yaml:"function"`
	TimeoutMS      int64  `json:"timeout_ms" yaml:"timeout_ms"`
	NumRetries     int    `json:"retries" yaml:"retries"`
	RateLimit      string `json:"rate_limit" yaml:"rate_limit"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Config:     session.NewConfig(),
		Function:   "",
		TimeoutMS:  5000,
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

	timeout   time.Duration
	rateLimit types.RateLimit

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
		conf:    conf,
		log:     log.Noop(),
		stats:   metrics.Noop(),
		mgr:     types.NoopMgr(),
		timeout: time.Duration(conf.TimeoutMS) * time.Millisecond,
	}

	if len(conf.Function) == 0 {
		return nil, errors.New("lambda function must not be empty")
	}

	for _, opt := range opts {
		opt(&l)
	}

	l.mCount = l.stats.GetCounter("client.lambda.count")
	l.mSucc = l.stats.GetCounter("client.lambda.success")
	l.mErr = l.stats.GetCounter("client.lambda.error")
	l.mLimited = l.stats.GetCounter("client.lambda.rate_limit.count")
	l.mLimitFor = l.stats.GetCounter("client.lambda.rate_limit.total_ms")
	l.mLimitErr = l.stats.GetCounter("client.lambda.rate_limit.error")
	l.mLatency = l.stats.GetTimer("client.lambda.latency")

	if len(l.conf.RateLimit) > 0 {
		var err error
		if l.rateLimit, err = l.mgr.GetRateLimit(l.conf.RateLimit); err != nil {
			return nil, fmt.Errorf("failed to obtain rate limit resource: %v", err)
		}
	}

	sess, err := l.conf.GetSession()
	if err != nil {
		return nil, err
	}

	l.lambda = lambda.New(sess)
	return &l, nil
}

//------------------------------------------------------------------------------

// OptSetLogger sets the logger to use.
func OptSetLogger(log log.Modular) func(*Type) {
	return func(t *Type) {
		t.log = log.NewModule(".client.lambda")
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

func (l *Type) waitForAccess() bool {
	if l.rateLimit == nil {
		return true
	}
	for {
		period, err := l.rateLimit.Access()
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

// Invoke attempts to invoke lambda function with a message as its payload.
func (l *Type) Invoke(msg types.Message) (types.Message, error) {
	l.mCount.Incr(1)
	response := msg.Copy()

	if err := msg.Iter(func(i int, p types.Part) error {
		remainingRetries := l.conf.NumRetries
		for {
			l.waitForAccess()

			ctx, done := context.WithTimeout(context.Background(), l.timeout)
			result, err := l.lambda.InvokeWithContext(ctx, &lambda.InvokeInput{
				FunctionName: aws.String(l.conf.Function),
				Payload:      p.Get(),
			})
			done()

			if err == nil {
				l.mSucc.Incr(1)
				response.Get(i).Set(result.Payload)
				return nil
			}
			l.mErr.Incr(1)
			remainingRetries--
			if remainingRetries < 0 {
				return err
			}
		}
	}); err != nil {
		return nil, err
	}
	return response, nil
}

//------------------------------------------------------------------------------
