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

package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRetry] = TypeSpec{
		constructor: NewRetry,
		description: `
Attempts to write messages to a child output and if the write fails for any
reason the message is retried either until success or, if the retries or max
elapsed time fields are non-zero, either is reached.

All messages in Benthos are always retried on an output error, but this would
usually involve propagating the error back to the source of the message, whereby
it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on
the event of a failed send. We might, for example, have a dedupe processor that
we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a
different output target (a dead letter queue). In which case you should instead
use the ` + "[`broker`](#broker)" + ` output type with the pattern 'try'.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			confBytes, err := json.Marshal(conf.Retry)
			if err != nil {
				return nil, err
			}

			confMap := map[string]interface{}{}
			if err = json.Unmarshal(confBytes, &confMap); err != nil {
				return nil, err
			}

			var outputSanit interface{} = struct{}{}
			if conf.Retry.Output != nil {
				if outputSanit, err = SanitiseConfig(*conf.Retry.Output); err != nil {
					return nil, err
				}
			}
			confMap["output"] = outputSanit
			return confMap, nil
		},
	}
}

//------------------------------------------------------------------------------

// RetryConfig contains configuration values for the Retry output type.
type RetryConfig struct {
	Output         *Config `json:"output" yaml:"output"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewRetryConfig creates a new RetryConfig with default values.
func NewRetryConfig() RetryConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 0
	rConf.Backoff.InitialInterval = "100ms"
	rConf.Backoff.MaxInterval = "1s"
	rConf.Backoff.MaxElapsedTime = "0s"
	return RetryConfig{
		Output: nil,
		Config: retries.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type dummyRetryConfig struct {
	Output         interface{} `json:"output" yaml:"output"`
	retries.Config `json:",inline" yaml:",inline"`
}

// MarshalJSON prints an empty object instead of nil.
func (r RetryConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyRetryConfig{
		Output: r.Output,
		Config: r.Config,
	}
	if r.Output == nil {
		dummy.Output = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (r RetryConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyRetryConfig{
		Output: r.Output,
		Config: r.Config,
	}
	if r.Output == nil {
		dummy.Output = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// Retry is an output type that continuously writes a message to a child output
// until the send is successful.
type Retry struct {
	running int32
	conf    RetryConfig

	wrapped Type
	backoff backoff.BackOff

	stats metrics.Type
	log   log.Modular

	transactionsIn  <-chan types.Transaction
	transactionsOut chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewRetry creates a new Retry input type.
func NewRetry(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if conf.Retry.Output == nil {
		return nil, errors.New("cannot create retry output without a child")
	}

	wrapped, err := New(*conf.Retry.Output, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create output '%v': %v", conf.Retry.Output.Type, err)
	}

	var boff backoff.BackOff
	if boff, err = conf.Retry.Get(); err != nil {
		return nil, err
	}

	return &Retry{
		running: 1,
		conf:    conf.Retry,

		log:             log,
		stats:           stats,
		wrapped:         wrapped,
		backoff:         boff,
		transactionsOut: make(chan types.Transaction),

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

func (r *Retry) loop() {
	// Metrics paths
	var (
		mRunning      = r.stats.GetGauge("retry.running")
		mCount        = r.stats.GetCounter("retry.count")
		mSuccess      = r.stats.GetCounter("retry.send.success")
		mPartsSuccess = r.stats.GetCounter("retry.parts.send.success")
		mError        = r.stats.GetCounter("retry.send.error")
		mEndOfRetries = r.stats.GetCounter("retry.end_of_retries")
	)

	defer func() {
		close(r.transactionsOut)
		r.wrapped.CloseAsync()
		err := r.wrapped.WaitForClose(time.Second)
		for ; err != nil; err = r.wrapped.WaitForClose(time.Second) {
		}
		mRunning.Decr(1)
		close(r.closedChan)
	}()
	mRunning.Incr(1)

	resChan := make(chan types.Response)

	for atomic.LoadInt32(&r.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-r.transactionsIn:
			if !open {
				return
			}
			mCount.Incr(1)
		case <-r.closeChan:
			return
		}

		var resOut types.Response

	retryLoop:
		for atomic.LoadInt32(&r.running) == 1 {
			select {
			case r.transactionsOut <- types.NewTransaction(ts.Payload, resChan):
			case <-r.closeChan:
				return
			}

			var res types.Response
			select {
			case res = <-resChan:
			case <-r.closeChan:
				return
			}

			if res.Error() != nil {
				mError.Incr(1)
				r.log.Errorf("Failed to send message: %v\n", res.Error())

				nextBackoff := r.backoff.NextBackOff()
				if nextBackoff == backoff.Stop {
					mEndOfRetries.Incr(1)
					r.backoff.Reset()
					resOut = response.NewNoack()
					break retryLoop
				}
				select {
				case <-time.After(nextBackoff):
				case <-r.closeChan:
					// TODO: log failed message?
					return
				}
			} else {
				mSuccess.Incr(1)
				mPartsSuccess.Incr(int64(ts.Payload.Len()))
				r.backoff.Reset()
				resOut = response.NewAck()
				break retryLoop
			}
		}

		select {
		case ts.ResponseChan <- resOut:
		case <-r.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (r *Retry) Consume(ts <-chan types.Transaction) error {
	if r.transactionsIn != nil {
		return types.ErrAlreadyStarted
	}
	if err := r.wrapped.Consume(r.transactionsOut); err != nil {
		return err
	}
	r.transactionsIn = ts
	go r.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (r *Retry) Connected() bool {
	return r.wrapped.Connected()
}

// CloseAsync shuts down the Retry input and stops processing requests.
func (r *Retry) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the Retry input has closed down.
func (r *Retry) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
