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
	Constructors[TypeSelectParts] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newSelectParts(conf.SelectParts, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("select_parts", p, mgr.Metrics()), nil
		},
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
the last element with be selected, and so on.

This processor is only applicable to [batched messages](/docs/configuration/batching).`,
		UsesBatches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldInt("parts", `An array of message indexes of a batch. Indexes can be negative, and if so the part will be selected from the end counting backwards starting from -1.`).Array(),
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
		Parts: []int{},
	}
}

//------------------------------------------------------------------------------

type selectPartsProc struct {
	parts []int
}

func newSelectParts(conf SelectPartsConfig, mgr interop.Manager) (*selectPartsProc, error) {
	return &selectPartsProc{
		parts: conf.Parts,
	}, nil
}

func (m *selectPartsProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	newMsg := message.QuickBatch(nil)

	lParts := msg.Len()
	for _, index := range m.parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}
		// Check boundary of part index.
		if index < 0 || index >= lParts {
			continue
		}
		newMsg.Append(msg.Get(index).Copy())
	}

	if newMsg.Len() == 0 {
		return nil, nil
	}
	return []*message.Batch{newMsg}, nil
}

func (m *selectPartsProc) Close(ctx context.Context) error {
	return nil
}
