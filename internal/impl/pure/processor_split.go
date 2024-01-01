package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	splitPFieldSize     = "size"
	splitPFieldByteSize = "byte_size"
)

func init() {
	err := service.RegisterBatchProcessor(
		"split", service.NewConfigSpec().
			Categories("Utility").
			Stable().
			Summary(`Breaks message batches (synonymous with multiple part messages) into smaller batches. The size of the resulting batches are determined either by a discrete size or, if the field `+"`byte_size`"+` is non-zero, then by total size in bytes (which ever limit is reached first).`).
			Description(`
This processor is for breaking batches down into smaller ones. In order to break a single message out into multiple messages use the `+"[`unarchive` processor](/docs/components/processors/unarchive)"+`.

If there is a remainder of messages after splitting a batch the remainder is also sent as a single batch. For example, if your target size was 10, and the processor received a batch of 95 message parts, the result would be 9 batches of 10 messages followed by a batch of 5 messages.`).
			Fields(
				service.NewIntField(splitPFieldSize).
					Description("The target number of messages.").
					Default(1),
				service.NewIntField(splitPFieldByteSize).
					Description("An optional target of total message bytes.").
					Default(0),
			),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			mgr := interop.UnwrapManagement(res)
			s := &splitProc{log: mgr.Logger()}

			var err error
			if s.size, err = conf.FieldInt(splitPFieldSize); err != nil {
				return nil, err
			}
			if s.byteSize, err = conf.FieldInt(splitPFieldByteSize); err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("split", s, mgr)), nil
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

func (s *splitProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
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
