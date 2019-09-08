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
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeThrottle] = TypeSpec{
		constructor: NewThrottle,
		description: `
Throttles the throughput of a pipeline to a maximum of one message batch per
period. This throttle is per processing pipeline, and therefore four threads
each with a throttle would result in four times the rate specified.

The period should be specified as a time duration string. For example, '1s'
would be 1 second, '10ms' would be 10 milliseconds, etc.`,
	}
}

//------------------------------------------------------------------------------

// ThrottleConfig contains configuration fields for the Throttle processor.
type ThrottleConfig struct {
	Period string `json:"period" yaml:"period"`
}

// NewThrottleConfig returns a ThrottleConfig with default values.
func NewThrottleConfig() ThrottleConfig {
	return ThrottleConfig{
		Period: "100us",
	}
}

//------------------------------------------------------------------------------

// Throttle is a processor that limits the stream of a pipeline to one message
// batch per period specified.
type Throttle struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	duration  time.Duration
	lastBatch time.Time

	mut sync.Mutex

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewThrottle returns a Throttle processor.
func NewThrottle(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	t := &Throttle{
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	if t.duration, err = time.ParseDuration(conf.Throttle.Period); err != nil {
		return nil, fmt.Errorf("failed to parse period: %v", err)
	}

	return t, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *Throttle) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.mCount.Incr(1)
	m.mut.Lock()
	defer m.mut.Unlock()

	spans := tracing.CreateChildSpans(TypeThrottle, msg)

	var throttleFor time.Duration
	if since := time.Since(m.lastBatch); m.duration > since {
		throttleFor = m.duration - since
		time.Sleep(throttleFor)
	}

	for _, s := range spans {
		s.SetTag("throttled_for", throttleFor.String())
		s.Finish()
	}

	m.lastBatch = time.Now()

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *Throttle) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *Throttle) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
