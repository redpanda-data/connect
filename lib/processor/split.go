package processor

import (
	"context"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func init() {
	Constructors[TypeSplit] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newSplit(conf.Split, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("split", p, mgr.Metrics()), nil
		},
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

type splitProc struct {
	log log.Modular

	size     int
	byteSize int
}

func newSplit(conf SplitConfig, mgr interop.Manager) (*splitProc, error) {
	return &splitProc{
		log:      mgr.Logger(),
		size:     conf.Size,
		byteSize: conf.ByteSize,
	}, nil
}

func (s *splitProc) ProcessBatch(ctx context.Context, _ []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	if msg.Len() == 0 {
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
	return msgs, nil
}

func (s *splitProc) Close(ctx context.Context) error {
	return nil
}
