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
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFilter] = TypeSpec{
		constructor: NewFilter,
		description: `
Tests each message batch against a condition, if the condition fails then the
batch is dropped. You can find a [full list of conditions here](../conditions).

In order to filter individual messages of a batch use the
` + "[`filter_parts`](#filter_parts)" + ` processor.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return condition.SanitiseConfig(conf.Filter.Config)
		},
	}
}

//------------------------------------------------------------------------------

// FilterConfig contains configuration fields for the Filter processor.
type FilterConfig struct {
	condition.Config `json:",inline" yaml:",inline"`
}

// NewFilterConfig returns a FilterConfig with default values.
func NewFilterConfig() FilterConfig {
	return FilterConfig{
		Config: condition.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// MarshalYAML prints the child condition instead of {}.
func (f FilterConfig) MarshalYAML() (interface{}, error) {
	return f.Config, nil
}

//------------------------------------------------------------------------------

// Filter is a processor that checks each message against a condition and
// rejects the message if a condition returns false.
type Filter struct {
	log   log.Modular
	stats metrics.Type

	condition condition.Type

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewFilter returns a Filter processor.
func NewFilter(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.Filter.Config, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to construct condition '%v': %v",
			conf.Filter.Config.Type, err,
		)
	}
	return &Filter{
		log:       log,
		stats:     stats,
		condition: cond,

		mCount:     stats.GetCounter("count"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Filter) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeFilter, msg)

	filterRes := c.condition.Check(msg)
	for _, s := range spans {
		if !filterRes {
			s.LogFields(
				olog.String("event", "dropped"),
				olog.String("type", "filtered"),
			)
		}
		s.SetTag("result", filterRes)
		s.Finish()
	}
	if !filterRes {
		c.mDropped.Incr(int64(msg.Len()))
		return nil, response.NewAck()
	}

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Filter) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Filter) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
