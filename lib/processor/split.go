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
	Constructors[TypeSplit] = TypeSpec{
		constructor: NewSplit,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Breaks message batches (synonymous with multiple part messages) into smaller batches. The size of the resulting batches are determined either by a discrete size or, if the field ` + "`byte_size`" + ` is non-zero, then by total size in bytes (which ever limit is reached first).`,
		Description: `
This processor is for breaking batches down into smaller ones. In order to break a single message out into multiple messages use the ` + "[`unarchive` processor](/docs/components/processors/unarchive)" + `.

If there is a remainder of messages after splitting a batch the remainder is also sent as a single batch. For example, if your target size was 10, and the processor received a batch of 95 message parts, the result would be 9 batches of 10 messages followed by a batch of 5 messages.`,
		UsesBatches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("size", "The target number of messages."),
			docs.FieldCommon("byte_size", "An optional target of total message bytes."),
		},
	}
}

//------------------------------------------------------------------------------

// SplitConfig is a configuration struct containing fields for the Split
// processor, which breaks message batches down into batches of a smaller size.
type SplitConfig struct {
	Size     int `json:"size" yaml:"size"`
	ByteSize int `json:"byte_size" yaml:"byte_size"`
}

// NewSplitConfig returns a SplitConfig with default values.
func NewSplitConfig() SplitConfig {
	return SplitConfig{
		Size:     1,
		ByteSize: 0,
	}
}

//------------------------------------------------------------------------------

// Split is a processor that splits messages into a message per part.
type Split struct {
	log   log.Modular
	stats metrics.Type

	size     int
	byteSize int

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSplit returns a Split processor.
func NewSplit(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Split{
		log:   log,
		stats: stats,

		size:     conf.Split.Size,
		byteSize: conf.Split.ByteSize,

		mCount:     stats.GetCounter("count"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Split) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	s.mCount.Incr(1)

	if msg.Len() == 0 {
		s.mDropped.Incr(1)
		return nil, nil
	}

	msgs := []*message.Batch{}

	nextMsg := message.QuickBatch(nil)
	byteSize := 0

	_ = msg.Iter(func(i int, p *message.Part) error {
		if (s.size > 0 && nextMsg.Len() >= s.size) ||
			(s.byteSize > 0 && (byteSize+len(p.Get())) > s.byteSize) {
			if nextMsg.Len() > 0 {
				msgs = append(msgs, nextMsg)
				nextMsg = message.QuickBatch(nil)
				byteSize = 0
			} else {
				s.log.Warnf("A single message exceeds the target batch byte size of '%v', actual size: '%v'", s.byteSize, len(p.Get()))
			}
		}
		nextMsg.Append(p)
		byteSize += len(p.Get())
		return nil
	})

	if nextMsg.Len() > 0 {
		msgs = append(msgs, nextMsg)
	}

	s.mBatchSent.Incr(int64(len(msgs)))
	s.mSent.Incr(int64(msg.Len()))
	return msgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Split) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *Split) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
