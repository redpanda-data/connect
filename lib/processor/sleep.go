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
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSleep] = TypeSpec{
		constructor: NewSleep,
		description: `
Sleep for a period of time specified as a duration string. This processor will
interpolate functions within the ` + "`duration`" + ` field, you can find a list
of functions [here](../config_interpolation.md#functions).

This processor executes once per message batch. In order to execute once for
each message of a batch place it within a
` + "[`for_each`](#for_each)" + ` processor:

` + "``` yaml" + `
for_each:
- sleep:
    duration: ${!metadata:sleep_for}
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// SleepConfig contains configuration fields for the Sleep processor.
type SleepConfig struct {
	Duration string `json:"duration" yaml:"duration"`
}

// NewSleepConfig returns a SleepConfig with default values.
func NewSleepConfig() SleepConfig {
	return SleepConfig{
		Duration: "100us",
	}
}

//------------------------------------------------------------------------------

// Sleep is a processor that limits the stream of a pipeline to one message
// batch per period specified.
type Sleep struct {
	closed    int32
	closeChan chan struct{}

	conf  Config
	log   log.Modular
	stats metrics.Type

	duration       time.Duration
	isInterpolated bool
	durationStr    *text.InterpolatedString

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSleep returns a Sleep processor.
func NewSleep(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	t := &Sleep{
		closeChan: make(chan struct{}),
		conf:      conf,
		log:       log,
		stats:     stats,

		durationStr:    text.NewInterpolatedString(conf.Sleep.Duration),
		isInterpolated: text.ContainsFunctionVariables([]byte(conf.Sleep.Duration)),

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	if !t.isInterpolated {
		var err error
		if t.duration, err = time.ParseDuration(conf.Sleep.Duration); err != nil {
			return nil, fmt.Errorf("failed to parse duration: %v", err)
		}
	}
	return t, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Sleep) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeSleep, msg)
	defer func() {
		for _, span := range spans {
			span.Finish()
		}
	}()

	period := s.duration
	if s.isInterpolated {
		var err error
		if period, err = time.ParseDuration(s.durationStr.Get(msg)); err != nil {
			s.log.Errorf("Failed to parse duration: %v\n", err)
			s.mErr.Incr(1)
		}
	}
	select {
	case <-time.After(period):
	case <-s.closeChan:
	}

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Sleep) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.closeChan)
	}
}

// WaitForClose blocks until the processor has closed down.
func (s *Sleep) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
