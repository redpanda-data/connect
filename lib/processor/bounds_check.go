package processor

import (
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBoundsCheck] = TypeSpec{
		constructor: NewBoundsCheck,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Removes messages (and batches) that do not fit within certain size boundaries.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("max_part_size", "The maximum size of a message to allow (in bytes)"),
			docs.FieldCommon("min_part_size", "The minimum size of a message to allow (in bytes)"),
			docs.FieldAdvanced("max_parts", "The maximum size of message batches to allow (in message count)"),
			docs.FieldAdvanced("min_parts", "The minimum size of message batches to allow (in message count)"),
		},
	}
}

//------------------------------------------------------------------------------

// BoundsCheckConfig contains configuration fields for the BoundsCheck
// processor.
type BoundsCheckConfig struct {
	MaxParts    int `json:"max_parts" yaml:"max_parts"`
	MinParts    int `json:"min_parts" yaml:"min_parts"`
	MaxPartSize int `json:"max_part_size" yaml:"max_part_size"`
	MinPartSize int `json:"min_part_size" yaml:"min_part_size"`
}

// NewBoundsCheckConfig returns a BoundsCheckConfig with default values.
func NewBoundsCheckConfig() BoundsCheckConfig {
	return BoundsCheckConfig{
		MaxParts:    100,
		MinParts:    1,
		MaxPartSize: 1 * 1024 * 1024 * 1024, // 1GB
		MinPartSize: 1,
	}
}

//------------------------------------------------------------------------------

// BoundsCheck is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type BoundsCheck struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount           metrics.StatCounter
	mDropped         metrics.StatCounter
	mDroppedEmpty    metrics.StatCounter
	mDroppedNumParts metrics.StatCounter
	mDroppedPartSize metrics.StatCounter
	mSent            metrics.StatCounter
	mBatchSent       metrics.StatCounter
}

// NewBoundsCheck returns a BoundsCheck processor.
func NewBoundsCheck(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &BoundsCheck{
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:           stats.GetCounter("count"),
		mDropped:         stats.GetCounter("dropped"),
		mDroppedEmpty:    stats.GetCounter("dropped_empty"),
		mDroppedNumParts: stats.GetCounter("dropped_num_parts"),
		mDroppedPartSize: stats.GetCounter("dropped_part_size"),
		mSent:            stats.GetCounter("sent"),
		mBatchSent:       stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *BoundsCheck) ProcessMessage(msg *message.Batch) ([]*message.Batch, types.Response) {
	m.mCount.Incr(1)

	lParts := msg.Len()
	if lParts < m.conf.BoundsCheck.MinParts {
		m.log.Debugf(
			"Rejecting message due to message parts below minimum (%v): %v\n",
			m.conf.BoundsCheck.MinParts, lParts,
		)
		m.mDropped.Incr(1)
		m.mDroppedEmpty.Incr(1)
		return nil, response.NewAck()
	} else if lParts > m.conf.BoundsCheck.MaxParts {
		m.log.Debugf(
			"Rejecting message due to message parts exceeding limit (%v): %v\n",
			m.conf.BoundsCheck.MaxParts, lParts,
		)
		m.mDropped.Incr(1)
		m.mDroppedNumParts.Incr(1)
		return nil, response.NewAck()
	}

	var reject bool
	_ = msg.Iter(func(i int, p *message.Part) error {
		if size := len(p.Get()); size > m.conf.BoundsCheck.MaxPartSize ||
			size < m.conf.BoundsCheck.MinPartSize {
			m.log.Debugf(
				"Rejecting message due to message part size (%v -> %v): %v\n",
				m.conf.BoundsCheck.MinPartSize,
				m.conf.BoundsCheck.MaxPartSize,
				size,
			)
			reject = true
			return errors.New("exit")
		}
		return nil
	})

	if reject {
		m.mDropped.Incr(1)
		m.mDroppedPartSize.Incr(1)
		return nil, response.NewAck()
	}

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(msg.Len()))
	msgs := [1]*message.Batch{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *BoundsCheck) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *BoundsCheck) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
