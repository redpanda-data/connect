package processor

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBatch] = TypeSpec{
		constructor: NewBatch,
		Description: `
DEPRECATED: This processor is no longer supported and has been replaced with
improved batching mechanisms. For more information about batching in Benthos
please check out [this document](/docs/configuration/batching).

This processor is scheduled to be removed in Benthos V4`,
		Status: docs.StatusDeprecated,
		config: docs.FieldComponent().WithChildren(
			docs.FieldDeprecated("byte_size"),
			docs.FieldDeprecated("count"),
			docs.FieldDeprecated("condition").HasType(docs.FieldTypeCondition),
			docs.FieldDeprecated("period"),
		),
	}
}

//------------------------------------------------------------------------------

// BatchConfig contains configuration fields for the Batch processor.
type BatchConfig struct {
	ByteSize  int              `json:"byte_size" yaml:"byte_size"`
	Count     int              `json:"count" yaml:"count"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	Period    string           `json:"period" yaml:"period"`
}

// NewBatchConfig returns a BatchConfig with default values.
func NewBatchConfig() BatchConfig {
	cond := condition.NewConfig()
	cond.Type = "static"
	cond.Static = false
	return BatchConfig{
		ByteSize:  0,
		Count:     0,
		Condition: cond,
		Period:    "",
	}
}

//------------------------------------------------------------------------------

// Batch is a processor that combines messages into a batch until a size limit
// or other condition is reached, at which point the batch is sent out. When a
// message is combined without yet producing a batch a NoAck response is
// returned, which is interpretted as source types as an instruction to send
// another message through but hold off on acknowledging this one.
//
// Eventually, when the batch reaches its target size, the batch is sent through
// the pipeline as a single message and an acknowledgement for that message
// determines whether the whole batch of messages are acknowledged.
//
// TODO: V4 Remove me.
type Batch struct {
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

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
	mDropped   metrics.StatCounter
}

// NewBatch returns a Batch processor.
func NewBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	log.Warnln("The batch processor is deprecated and is scheduled for removal in Benthos V4. For more information about batching in Benthos check out https://benthos.dev/docs/configuration/batching")

	cMgr, cLog, cStats := interop.LabelChild("condition", mgr, log, stats)
	cond, err := condition.New(conf.Batch.Condition, cMgr, cLog, cStats)
	if err != nil {
		return nil, fmt.Errorf("failed to create condition: %v", err)
	}
	var period time.Duration
	if len(conf.Batch.Period) > 0 {
		if period, err = time.ParseDuration(conf.Batch.Period); err != nil {
			return nil, fmt.Errorf("failed to parse duration string: %v", err)
		}
	}
	return &Batch{
		log:   log,
		stats: stats,

		byteSize: conf.Batch.ByteSize,
		count:    conf.Batch.Count,
		period:   period,
		cond:     cond,

		lastBatch: time.Now(),

		mSizeBatch:   stats.GetCounter("on_size"),
		mCountBatch:  stats.GetCounter("on_count"),
		mPeriodBatch: stats.GetCounter("on_period"),
		mCondBatch:   stats.GetCounter("on_condition"),

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
		mDropped:   stats.GetCounter("dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (b *Batch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	b.mCount.Incr(1)
	b.mut.Lock()
	defer b.mut.Unlock()

	var batch bool

	// Add new parts to the buffer.
	msg.Iter(func(i int, p types.Part) error {
		if b.add(p.Copy()) {
			batch = true
		}
		return nil
	})

	// If we have reached our target count of parts in the buffer.
	if batch {
		if newMsg := b.flush(); newMsg != nil {
			b.mSent.Incr(int64(newMsg.Len()))
			b.mBatchSent.Incr(1)
			return []types.Message{newMsg}, nil
		}
	}

	b.log.Traceln("Added message to pending batch")
	b.mDropped.Incr(1)
	return nil, response.NewUnack()
}

// CloseAsync shuts down the processor and stops processing requests.
func (b *Batch) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (b *Batch) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

func (b *Batch) add(part types.Part) bool {
	b.sizeTally += len(part.Get())
	b.parts = append(b.parts, part)

	if !b.triggered && b.count > 0 && len(b.parts) >= b.count {
		b.triggered = true
		b.mCountBatch.Incr(1)
		b.log.Traceln("Batching based on count")
	}
	if !b.triggered && b.byteSize > 0 && b.sizeTally >= b.byteSize {
		b.triggered = true
		b.mSizeBatch.Incr(1)
		b.log.Traceln("Batching based on byte_size")
	}
	tmpMsg := message.New(nil)
	tmpMsg.Append(part)
	if !b.triggered && b.cond.Check(tmpMsg) {
		b.triggered = true
		b.mCondBatch.Incr(1)
		b.log.Traceln("Batching based on condition")
	}

	return b.triggered || (b.period > 0 && time.Since(b.lastBatch) > b.period)
}

func (b *Batch) flush() types.Message {
	var newMsg types.Message
	if len(b.parts) > 0 {
		if !b.triggered && b.period > 0 && time.Since(b.lastBatch) > b.period {
			b.mPeriodBatch.Incr(1)
			b.log.Traceln("Batching based on period")
		}
		newMsg = message.New(nil)
		newMsg.Append(b.parts...)
	}
	b.parts = nil
	b.sizeTally = 0
	b.lastBatch = time.Now()
	b.triggered = false

	return newMsg
}

//------------------------------------------------------------------------------
