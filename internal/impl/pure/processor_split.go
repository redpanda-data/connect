package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newSplit(conf.Split, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("split", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "split",
		Categories: []string{
			"Utility",
		},
		Summary: `
Breaks message batches (synonymous with multiple part messages) into smaller batches. The size of the resulting batches are determined either by a discrete size or, if the field ` + "`byte_size`" + ` is non-zero, then by total size in bytes (which ever limit is reached first).`,
		Description: `
This processor is for breaking batches down into smaller ones. In order to break a single message out into multiple messages use the ` + "[`unarchive` processor](/docs/components/processors/unarchive)" + `.

If there is a remainder of messages after splitting a batch the remainder is also sent as a single batch. For example, if your target size was 10, and the processor received a batch of 95 message parts, the result would be 9 batches of 10 messages followed by a batch of 5 messages.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("size", "The target number of messages.").HasDefault(1),
			docs.FieldInt("byte_size", "An optional target of total message bytes.").HasDefault(0),
		),
	})
	if err != nil {
		panic(err)
	}
}

type splitProc struct {
	log log.Modular

	size     int
	byteSize int
}

func newSplit(conf processor.SplitConfig, mgr bundle.NewManagement) (*splitProc, error) {
	return &splitProc{
		log:      mgr.Logger(),
		size:     conf.Size,
		byteSize: conf.ByteSize,
	}, nil
}

func (s *splitProc) ProcessBatch(ctx context.Context, _ []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	if msg.Len() == 0 {
		return nil, nil
	}

	msgs := []message.Batch{}

	nextMsg := message.QuickBatch(nil)
	byteSize := 0

	_ = msg.Iter(func(i int, p *message.Part) error {
		if (s.size > 0 && nextMsg.Len() >= s.size) ||
			(s.byteSize > 0 && (byteSize+len(p.AsBytes())) > s.byteSize) {
			if nextMsg.Len() > 0 {
				msgs = append(msgs, nextMsg)
				nextMsg = message.QuickBatch(nil)
				byteSize = 0
			} else {
				s.log.Warnf("A single message exceeds the target batch byte size of '%v', actual size: '%v'", s.byteSize, len(p.AsBytes()))
			}
		}
		nextMsg = append(nextMsg, p)
		byteSize += len(p.AsBytes())
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
