package policy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Batcher implements a batching policy by buffering messages until, based on a
// set of rules, the buffered messages are ready to be sent onwards as a batch.
type Batcher struct {
	log log.Modular

	byteSize  int
	count     int
	period    time.Duration
	check     *mapping.Executor
	procs     []iprocessor.V1
	sizeTally int
	parts     []*message.Part

	triggered bool
	lastBatch time.Time

	mSizeBatch   metrics.StatCounter
	mCountBatch  metrics.StatCounter
	mPeriodBatch metrics.StatCounter
	mCheckBatch  metrics.StatCounter
}

// New creates an empty policy with default rules.
func New(conf batchconfig.Config, mgr bundle.NewManagement) (*Batcher, error) {
	if !conf.IsLimited() {
		return nil, errors.New("batch policy must have at least one active trigger")
	}
	if !conf.IsHardLimited() {
		mgr.Logger().Warnln("Batch policy should have at least one of count, period or byte_size set in order to provide a hard batch ceiling.")
	}
	var err error
	var check *mapping.Executor
	if len(conf.Check) > 0 {
		if check, err = mgr.BloblEnvironment().NewMapping(conf.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check: %v", err)
		}
	}
	var period time.Duration
	if len(conf.Period) > 0 {
		if period, err = time.ParseDuration(conf.Period); err != nil {
			return nil, fmt.Errorf("failed to parse duration string: %v", err)
		}
	}
	var procs []iprocessor.V1
	for i, pconf := range conf.Processors {
		pMgr := mgr.IntoPath("processors", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, err
		}
		procs = append(procs, proc)
	}

	batchOn := mgr.Metrics().GetCounterVec("batch_created", "mechanism")
	return &Batcher{
		log: mgr.Logger(),

		byteSize: conf.ByteSize,
		count:    conf.Count,
		period:   period,
		check:    check,
		procs:    procs,

		lastBatch: time.Now(),

		mSizeBatch:   batchOn.With("size"),
		mCountBatch:  batchOn.With("count"),
		mPeriodBatch: batchOn.With("period"),
		mCheckBatch:  batchOn.With("check"),
	}, nil
}

//------------------------------------------------------------------------------

// Add a new message part to this batch policy. Returns true if this part
// triggers the conditions of the policy.
func (p *Batcher) Add(part *message.Part) bool {
	if p.byteSize > 0 {
		// This calculation (serialisation into bytes) is potentially expensive
		// so we only do it when there's a byte size based trigger.
		p.sizeTally += len(part.AsBytes())
	}
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
		tmpMsg := message.Batch(p.parts)
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
func (p *Batcher) Flush(ctx context.Context) message.Batch {
	var newMsg message.Batch

	resultMsgs := p.flushAny(ctx)
	if len(resultMsgs) == 1 {
		newMsg = resultMsgs[0]
	} else if len(resultMsgs) > 1 {
		for _, m := range resultMsgs {
			_ = m.Iter(func(_ int, p *message.Part) error {
				newMsg = append(newMsg, p)
				return nil
			})
		}
	}
	return newMsg
}

func (p *Batcher) flushAny(ctx context.Context) []message.Batch {
	var newMsg message.Batch
	if len(p.parts) > 0 {
		if !p.triggered && p.period > 0 && time.Since(p.lastBatch) > p.period {
			p.mPeriodBatch.Incr(1)
			p.log.Traceln("Batching based on period")
		}
		newMsg = message.Batch(p.parts)
	}
	p.parts = nil
	p.sizeTally = 0
	p.lastBatch = time.Now()
	p.triggered = false

	if newMsg == nil {
		return nil
	}

	if len(p.procs) > 0 {
		resultMsgs, err := iprocessor.ExecuteAll(ctx, p.procs, newMsg)
		if err != nil {
			p.log.Errorf("Batch processors resulted in error: %v, the batch has been dropped.", err)
			return nil
		}
		return resultMsgs
	}

	return []message.Batch{newMsg}
}

// Count returns the number of currently buffered message parts within this
// policy.
func (p *Batcher) Count() int {
	return len(p.parts)
}

// UntilNext returns a duration indicating how long until the current batch
// should be flushed due to a configured period. A negative duration indicates
// a period has not been set.
func (p *Batcher) UntilNext() time.Duration {
	if p.period <= 0 {
		return -1
	}
	return time.Until(p.lastBatch.Add(p.period))
}

//------------------------------------------------------------------------------

// Close shuts down the policy resources.
func (p *Batcher) Close(ctx context.Context) error {
	for _, c := range p.procs {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
