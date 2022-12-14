package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newInsertPart(conf.InsertPart, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("insert_part", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "insert_part",
		Categories: []string{
			"Composition",
		},
		Summary: `
Insert a new message into a batch at an index. If the specified index is greater
than the length of the existing batch it will be appended to the end.`,
		Description: `
The index can be negative, and if so the message will be inserted from the end
counting backwards starting from -1. E.g. if index = -1 then the new message
will become the last of the batch, if index = -2 then the new message will be
inserted before the last message, and so on. If the negative index is greater
than the length of the existing batch it will be inserted at the beginning.

The new message will have metadata copied from the first pre-existing message of
the batch.

This processor will interpolate functions within the 'content' field, you can
find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("index", "The index within the batch to insert the message at."),
			docs.FieldString("content", "The content of the message being inserted.").IsInterpolated(),
		).ChildDefaultAndTypesFromStruct(processor.NewInsertPartConfig()),
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

func newInsertPart(conf processor.InsertPartConfig, mgr bundle.NewManagement) (processor.V2Batched, error) {
	part, err := mgr.BloblEnvironment().NewField(conf.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse content expression: %v", err)
	}
	return &insertPart{
		part:  part,
		index: conf.Index,
		log:   mgr.Logger(),
	}, nil
}

func (p *insertPart) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	newPartBytes, err := p.part.Bytes(0, msg)
	if err != nil {
		p.log.Errorf("Content interpolation error: %v", err)
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
