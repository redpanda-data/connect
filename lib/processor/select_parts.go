package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSelectParts] = TypeSpec{
		constructor: NewSelectParts,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Cherry pick a set of messages from a batch by their index. Indexes larger than
the number of messages are simply ignored.`,
		Description: `
The selected parts are added to the new message batch in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].

If none of the selected parts exist in the input batch (resulting in an empty
output message) the batch is dropped entirely.

Message indexes can be negative, and if so the part will be selected from the
end counting backwards starting from -1. E.g. if index = -1 then the selected
part will be the last part of the message, if index = -2 then the part before
the last element with be selected, and so on.`,
		UsesBatches: true,
		FieldSpecs: docs.FieldSpecs{
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// SelectPartsConfig contains configuration fields for the SelectParts
// processor.
type SelectPartsConfig struct {
	Parts []int `json:"parts" yaml:"parts"`
}

// NewSelectPartsConfig returns a SelectPartsConfig with default values.
func NewSelectPartsConfig() SelectPartsConfig {
	return SelectPartsConfig{
		Parts: []int{0},
	}
}

//------------------------------------------------------------------------------

// SelectParts is a processor that selects parts from a message to append to a
// new message.
type SelectParts struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSelected  metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSelectParts returns a SelectParts processor.
func NewSelectParts(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &SelectParts{
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mSkipped:   stats.GetCounter("skipped"),
		mSelected:  stats.GetCounter("selected"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *SelectParts) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	m.mCount.Incr(1)

	newMsg := message.QuickBatch(nil)

	lParts := msg.Len()
	for _, index := range m.conf.SelectParts.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			m.mSkipped.Incr(1)
		} else {
			m.mSelected.Incr(1)
			newMsg.Append(msg.Get(index).Copy())
		}
	}

	if newMsg.Len() == 0 {
		m.mDropped.Incr(1)
		return nil, nil
	}

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]*message.Batch{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *SelectParts) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *SelectParts) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
