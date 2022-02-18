package processor

import (
	"context"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeInsertPart] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newInsertPart(conf.InsertPart, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("insert_part", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("index", "The index within the batch to insert the message at."),
			docs.FieldCommon("content", "The content of the message being inserted.").IsInterpolated(),
		},
	}
}

//------------------------------------------------------------------------------

// InsertPartConfig contains configuration fields for the InsertPart processor.
type InsertPartConfig struct {
	Index   int    `json:"index" yaml:"index"`
	Content string `json:"content" yaml:"content"`
}

// NewInsertPartConfig returns a InsertPartConfig with default values.
func NewInsertPartConfig() InsertPartConfig {
	return InsertPartConfig{
		Index:   -1,
		Content: "",
	}
}

//------------------------------------------------------------------------------

type insertPart struct {
	index int
	part  *field.Expression
}

func newInsertPart(conf InsertPartConfig, mgr interop.Manager) (processor.V2Batched, error) {
	part, err := mgr.BloblEnvironment().NewField(conf.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse content expression: %v", err)
	}
	return &insertPart{
		part:  part,
		index: conf.Index,
	}, nil
}

func (p *insertPart) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	newPartBytes := p.part.Bytes(0, msg)

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
	newPart := msg.Get(0).Copy()
	newPart.Set(newPartBytes)
	_ = msg.Iter(func(i int, p *message.Part) error {
		if i == index {
			newMsg.Append(newPart)
		}
		newMsg.Append(p.Copy())
		return nil
	})
	if index == msg.Len() {
		newMsg.Append(newPart)
	}

	return []*message.Batch{newMsg}, nil
}

func (p *insertPart) Close(context.Context) error {
	return nil
}
