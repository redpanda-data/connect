package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ippFieldIndex   = "index"
	ippFieldContent = "content"
)

func insertPartSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary("Insert a new message into a batch at an index. If the specified index is greater than the length of the existing batch it will be appended to the end.").
		Description(`
The index can be negative, and if so the message will be inserted from the end counting backwards starting from -1. E.g. if index = -1 then the new message will become the last of the batch, if index = -2 then the new message will be inserted before the last message, and so on. If the negative index is greater than the length of the existing batch it will be inserted at the beginning.

The new message will have metadata copied from the first pre-existing message of the batch.

This processor will interpolate functions within the 'content' field, you can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewIntField(ippFieldIndex).
				Description("The index within the batch to insert the message at.").
				Default(-1),
			service.NewInterpolatedStringField(ippFieldContent).
				Description("The content of the message being inserted.").
				Default(""),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"insert_part", insertPartSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			index, err := conf.FieldInt(ippFieldIndex)
			if err != nil {
				return nil, err
			}

			contentStr, err := conf.FieldString(ippFieldContent)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newInsertPart(index, contentStr, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("insert_part", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type insertPart struct {
	index int
	part  *field.Expression
	log   log.Modular
}

func newInsertPart(index int, contentStr string, mgr bundle.NewManagement) (processor.AutoObservedBatched, error) {
	part, err := mgr.BloblEnvironment().NewField(contentStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse content expression: %v", err)
	}
	return &insertPart{
		part:  part,
		index: index,
		log:   mgr.Logger(),
	}, nil
}

func (p *insertPart) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	newPartBytes, err := p.part.Bytes(0, msg)
	if err != nil {
		p.log.Error("Content interpolation error: %v", err)
		return []message.Batch{msg}, nil
	}

	index := p.index
	msgLen := msg.Len()
	if index < 0 {
		index = msgLen + index + 1
		if index < 0 {
			index = 0
		}
	} else if index > msgLen {
		index = msgLen
	}

	newMsg := message.QuickBatch(nil)
	newPart := msg.Get(0).ShallowCopy()
	newPart.SetBytes(newPartBytes)
	_ = msg.Iter(func(i int, p *message.Part) error {
		if i == index {
			newMsg = append(newMsg, newPart)
		}
		newMsg = append(newMsg, p.ShallowCopy())
		return nil
	})
	if index == msg.Len() {
		newMsg = append(newMsg, newPart)
	}

	return []message.Batch{newMsg}, nil
}

func (p *insertPart) Close(context.Context) error {
	return nil
}
