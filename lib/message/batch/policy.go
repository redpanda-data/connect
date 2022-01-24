package batch

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// PolicyConfig contains configuration parameters for a batch policy.
type PolicyConfig struct {
	ByteSize   int                `json:"byte_size" yaml:"byte_size"`
	Count      int                `json:"count" yaml:"count"`
	Check      string             `json:"check" yaml:"check"`
	Period     string             `json:"period" yaml:"period"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewPolicyConfig creates a default PolicyConfig.
func NewPolicyConfig() PolicyConfig {
	return PolicyConfig{
		ByteSize:   0,
		Count:      0,
		Check:      "",
		Period:     "",
		Processors: []processor.Config{},
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
	if len(p.Check) > 0 {
		return false
	}
	if len(p.Period) > 0 {
		return false
	}
	if len(p.Processors) > 0 {
		return false
	}
	return true
}

func (p PolicyConfig) isLimited() bool {
	if p.ByteSize > 0 {
		return true
	}
	if p.Count > 0 {
		return true
	}
	if len(p.Period) > 0 {
		return true
	}
	if len(p.Check) > 0 {
		return true
	}
	return false
}

func (p PolicyConfig) isHardLimited() bool {
	if p.ByteSize > 0 {
		return true
	}
	if p.Count > 0 {
		return true
	}
	if len(p.Period) > 0 {
		return true
	}
	return false
}

//------------------------------------------------------------------------------

// Policy implements a batching policy by buffering messages until, based on a
// set of rules, the buffered messages are ready to be sent onwards as a batch.
type Policy struct {
	log log.Modular

	byteSize  int
	count     int
	period    time.Duration
	check     *mapping.Executor
	procs     []types.Processor
	sizeTally int
	parts     []types.Part

	triggered bool
	lastBatch time.Time

	mSizeBatch   metrics.StatCounter
	mCountBatch  metrics.StatCounter
	mPeriodBatch metrics.StatCounter
	mCheckBatch  metrics.StatCounter
	mCondBatch   metrics.StatCounter
}

// NewPolicy creates an empty policy with default rules.
func NewPolicy(
	conf PolicyConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Policy, error) {
	if !conf.isLimited() {
		return nil, errors.New("batch policy must have at least one active trigger")
	}
	if !conf.isHardLimited() {
		log.Warnln("Batch policy should have at least one of count, period or byte_size set in order to provide a hard batch ceiling.")
	}
	var err error
	var check *mapping.Executor
	if len(conf.Check) > 0 {
		if check, err = interop.NewBloblangMapping(mgr, conf.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check: %v", err)
		}
	}
	var period time.Duration
	if len(conf.Period) > 0 {
		if period, err = time.ParseDuration(conf.Period); err != nil {
			return nil, fmt.Errorf("failed to parse duration string: %v", err)
		}
	}
	var procs []types.Processor
	for i, pconf := range conf.Processors {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := processor.New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create processor '%v': %v", i, err)
		}
		procs = append(procs, proc)
	}
	return &Policy{
		log: log,

		byteSize: conf.ByteSize,
		count:    conf.Count,
		period:   period,
		check:    check,
		procs:    procs,

		lastBatch: time.Now(),

		mSizeBatch:   stats.GetCounter("on_size"),
		mCountBatch:  stats.GetCounter("on_count"),
		mPeriodBatch: stats.GetCounter("on_period"),
		mCheckBatch:  stats.GetCounter("on_check"),
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
	if p.check != nil && !p.triggered {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll(p.parts)

		test, err := p.check.QueryPart(tmpMsg.Len()-1, tmpMsg)
		if err != nil {
			test = false
			p.log.Errorf("Failed to execute batch check query: %v\n", err)
		}
		if test {
			p.triggered = true
			p.mCheckBatch.Incr(1)
			p.log.Traceln("Batching based on check query")
		}
	}
	return p.triggered || (p.period > 0 && time.Since(p.lastBatch) > p.period)
}

// Flush clears all messages stored by this batch policy. Returns nil if the
// policy is currently empty.
func (p *Policy) Flush() types.Message {
	var newMsg types.Message

	resultMsgs := p.FlushAny()
	if len(resultMsgs) == 1 {
		newMsg = resultMsgs[0]
	} else if len(resultMsgs) > 1 {
		newMsg = message.New(nil)
		var parts []types.Part
		for _, m := range resultMsgs {
			m.Iter(func(_ int, p types.Part) error {
				parts = append(parts, p)
				return nil
			})
		}
		newMsg.SetAll(parts)
	}
	return newMsg
}

// FlushAny clears all messages stored by this batch policy and returns any
// number of discrete message batches. Returns nil if the policy is currently
// empty.
func (p *Policy) FlushAny() []types.Message {
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

	if newMsg == nil {
		return nil
	}

	if len(p.procs) > 0 {
		resultMsgs, res := processor.ExecuteAll(p.procs, newMsg)
		if res != nil {
			if err := res.Error(); err != nil {
				p.log.Errorf("Batch processors resulted in error: %v, the batch has been dropped.", err)
			}
			return nil
		}
		return resultMsgs
	}

	return []types.Message{newMsg}
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

// CloseAsync shuts down the policy resources.
func (p *Policy) CloseAsync() {
	for _, c := range p.procs {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *Policy) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.procs {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}
