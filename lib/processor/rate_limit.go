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

package processor

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRateLimit] = TypeSpec{
		constructor: NewRateLimit,
		description: `
Throttles the throughput of a pipeline according to a specified
` + "[`rate_limit`](../rate_limits/README.md)" + ` resource. Rate limits are
shared across components and therefore apply globally to all processing
pipelines.`,
	}
}

//------------------------------------------------------------------------------

// RateLimitConfig contains configuration fields for the RateLimit processor.
type RateLimitConfig struct {
	Resource string `json:"resource" yaml:"resource"`
}

// NewRateLimitConfig returns a RateLimitConfig with default values.
func NewRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		Resource: "",
	}
}

//------------------------------------------------------------------------------

// RateLimit is a processor that performs an RateLimit request using the message as the
// request body, and returns the response.
type RateLimit struct {
	rl types.RateLimit

	log log.Modular

	mCount       metrics.StatCounter
	mRateLimited metrics.StatCounter
	mErr         metrics.StatCounter
	mSent        metrics.StatCounter
	mBatchSent   metrics.StatCounter

	closeChan chan struct{}
	closeOnce sync.Once
}

// NewRateLimit returns a RateLimit processor.
func NewRateLimit(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	rl, err := mgr.GetRateLimit(conf.RateLimit.Resource)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain rate limit resource '%v': %v", conf.RateLimit.Resource, err)
	}
	r := &RateLimit{
		rl:           rl,
		log:          log,
		mCount:       stats.GetCounter("count"),
		mRateLimited: stats.GetCounter("rate.limited"),
		mErr:         stats.GetCounter("error"),
		mSent:        stats.GetCounter("sent"),
		mBatchSent:   stats.GetCounter("batch.sent"),
		closeChan:    make(chan struct{}),
	}
	return r, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *RateLimit) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	r.mCount.Incr(1)

	msg.Iter(func(i int, p types.Part) error {
		waitFor, err := r.rl.Access()
		for err != nil || waitFor > 0 {
			if err == types.ErrTypeClosed {
				return err
			}
			if err != nil {
				r.mErr.Incr(1)
				r.log.Errorf("Failed to access rate limit: %v\n", err)
				waitFor = time.Second
			} else {
				r.mRateLimited.Incr(1)
			}
			select {
			case <-time.After(waitFor):
			case <-r.closeChan:
				return types.ErrTypeClosed
			}
			waitFor, err = r.rl.Access()
		}
		return err
	})

	r.mBatchSent.Incr(1)
	r.mSent.Incr(int64(msg.Len()))
	return []types.Message{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *RateLimit) CloseAsync() {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
}

// WaitForClose blocks until the processor has closed down.
func (r *RateLimit) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
