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
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFilterParts] = TypeSpec{
		constructor: NewFilterParts,
		description: `
Tests each individual message of a batch against a condition, if the condition
fails then the message is dropped. If the resulting batch is empty it will be
dropped. You can find a [full list of conditions here](../conditions), in this
case each condition will be applied to a message as if it were a single message
batch.

This processor is useful if you are combining messages into batches using the
` + "[`batch`](#batch)" + ` processor and wish to remove specific parts.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return condition.SanitiseConfig(conf.FilterParts.Config)
		},
	}
}

//------------------------------------------------------------------------------

// FilterPartsConfig contains configuration fields for the FilterParts
// processor.
type FilterPartsConfig struct {
	condition.Config `json:",inline" yaml:",inline"`
}

// NewFilterPartsConfig returns a FilterPartsConfig with default values.
func NewFilterPartsConfig() FilterPartsConfig {
	return FilterPartsConfig{
		Config: condition.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// MarshalYAML prints the child condition instead of {}.
func (f FilterPartsConfig) MarshalYAML() (interface{}, error) {
	return f.Config, nil
}

//------------------------------------------------------------------------------

// FilterParts is a processor that checks each part from a message against a
// condition and removes the part if the condition returns false.
type FilterParts struct {
	log   log.Modular
	stats metrics.Type

	condition condition.Type

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewFilterParts returns a FilterParts processor.
func NewFilterParts(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.FilterParts.Config, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to construct condition '%v': %v",
			conf.FilterParts.Config.Type, err,
		)
	}
	return &FilterParts{
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
func (c *FilterParts) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	newMsg := message.New(nil)

	spans := tracing.CreateChildSpans(TypeFilterParts, msg)
	defer func() {
		for _, s := range spans {
			s.Finish()
		}
	}()

	for i := 0; i < msg.Len(); i++ {
		if c.condition.Check(message.Lock(msg, i)) {
			newMsg.Append(msg.Get(i).Copy())
			spans[i].SetTag("result", true)
		} else {
			spans[i].LogFields(
				olog.String("event", "dropped"),
				olog.String("type", "filtered"),
			)
			spans[i].SetTag("result", false)
			c.mDropped.Incr(1)
		}
	}
	if newMsg.Len() > 0 {
		c.mBatchSent.Incr(1)
		c.mSent.Incr(int64(newMsg.Len()))
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	return nil, response.NewAck()
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *FilterParts) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *FilterParts) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
