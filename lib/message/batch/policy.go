// Copyright (c) 2019 Ashley Jeffs
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

package batch

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// SanitisePolicyConfig returns a policy config structure ready to be marshalled
// with irrelevant fields omitted.
func SanitisePolicyConfig(policy PolicyConfig) (interface{}, error) {
	condSanit, err := condition.SanitiseConfig(policy.Condition)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"byte_size": policy.ByteSize,
		"count":     policy.Count,
		"condition": condSanit,
		"period":    policy.Period,
	}, nil
}

//------------------------------------------------------------------------------

// PolicyConfig contains configuration parameters for a batch policy.
type PolicyConfig struct {
	ByteSize  int              `json:"byte_size" yaml:"byte_size"`
	Count     int              `json:"count" yaml:"count"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	Period    string           `json:"period" yaml:"period"`
}

// NewPolicyConfig creates a default PolicyConfig.
func NewPolicyConfig() PolicyConfig {
	cond := condition.NewConfig()
	cond.Type = "static"
	cond.Static = false
	return PolicyConfig{
		ByteSize:  0,
		Count:     0,
		Condition: cond,
		Period:    "",
	}
}

// IsNoop returns true if this batch policy configuration does nothing.
func (p PolicyConfig) IsNoop() bool {
	if p.ByteSize > 0 {
		return false
	}
	if p.Count > 1 {
		return false
	}
	if p.Condition.Type != condition.TypeStatic {
		return false
	}
	if len(p.Period) > 0 {
		return false
	}
	return true
}

//------------------------------------------------------------------------------

// Policy implements a batching policy by buffering messages until, based on a
// set of rules, the buffered messages are ready to be sent onwards as a batch.
type Policy struct {
	log log.Modular

	byteSize  int
	count     int
	period    time.Duration
	cond      condition.Type
	sizeTally int
	parts     []types.Part

	triggered bool
	lastBatch time.Time
	mut       sync.Mutex

	mSizeBatch   metrics.StatCounter
	mCountBatch  metrics.StatCounter
	mPeriodBatch metrics.StatCounter
	mCondBatch   metrics.StatCounter
}

// NewPolicy creates an empty policy with default rules.
func NewPolicy(
	conf PolicyConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Policy, error) {
	cond, err := condition.New(conf.Condition, mgr, log.NewModule(".condition"), metrics.Namespaced(stats, "condition"))
	if err != nil {
		return nil, err
	}
	var period time.Duration
	if len(conf.Period) > 0 {
		if period, err = time.ParseDuration(conf.Period); err != nil {
			return nil, fmt.Errorf("failed to parse duration string: %v", err)
		}
	}
	return &Policy{
		log: log,

		byteSize: conf.ByteSize,
		count:    conf.Count,
		period:   period,
		cond:     cond,

		lastBatch: time.Now(),

		mSizeBatch:   stats.GetCounter("on_size"),
		mCountBatch:  stats.GetCounter("on_count"),
		mPeriodBatch: stats.GetCounter("on_period"),
		mCondBatch:   stats.GetCounter("on_condition"),
	}, nil
}

//------------------------------------------------------------------------------

// Add a new message part to this batch policy. Returns true if this part
// triggers the conditions of the policy.
func (p *Policy) Add(part types.Part) bool {
	p.sizeTally += len(part.Get())
	p.parts = append(p.parts, part)

	if !p.triggered && p.count > 0 && len(p.parts) >= p.count {
		p.triggered = true
		p.mCountBatch.Incr(1)
		p.log.Traceln("Batching based on count")
	}
	if !p.triggered && p.byteSize > 0 && p.sizeTally >= p.byteSize {
		p.triggered = true
		p.mSizeBatch.Incr(1)
		p.log.Traceln("Batching based on byte_size")
	}
	tmpMsg := message.New(nil)
	tmpMsg.Append(part)
	if !p.triggered && p.cond.Check(tmpMsg) {
		p.triggered = true
		p.mCondBatch.Incr(1)
		p.log.Traceln("Batching based on condition")
	}

	return p.triggered || (p.period > 0 && time.Since(p.lastBatch) > p.period)
}

// Flush clears all messages stored by this batch policy. Returns nil if the
// policy is currently empty.
func (p *Policy) Flush() types.Message {
	var newMsg types.Message
	if len(p.parts) > 0 {
		if !p.triggered && p.period > 0 && time.Since(p.lastBatch) > p.period {
			p.mPeriodBatch.Incr(1)
			p.log.Traceln("Batching based on period")
		}
		newMsg = message.New(nil)
		newMsg.Append(p.parts...)
	}
	p.parts = nil
	p.sizeTally = 0
	p.lastBatch = time.Now()
	p.triggered = false
	return newMsg
}

// Count returns the number of currently buffered message parts within this
// policy.
func (p *Policy) Count() int {
	return len(p.parts)
}

// UntilNext returns a duration indicating how long until the current batch
// should be flushed due to a configured period. A negative duration indicates
// a period has not been set.
func (p *Policy) UntilNext() time.Duration {
	if p.period <= 0 {
		return -1
	}
	return time.Until(p.lastBatch.Add(p.period))
}

//------------------------------------------------------------------------------
