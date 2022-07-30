package pure

import (
	"context"
	"errors"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newBoundsCheck(conf.BoundsCheck, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("bounds_check", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "bounds_check",
		Categories: []string{
			"Utility",
		},
		Summary: `
Removes messages (and batches) that do not fit within certain size boundaries.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("max_part_size", "The maximum size of a message to allow (in bytes)"),
			docs.FieldInt("min_part_size", "The minimum size of a message to allow (in bytes)"),
			docs.FieldInt("max_parts", "The maximum size of message batches to allow (in message count)").Advanced(),
			docs.FieldInt("min_parts", "The minimum size of message batches to allow (in message count)").Advanced(),
		).ChildDefaultAndTypesFromStruct(processor.NewBoundsCheckConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type boundsCheck struct {
	conf processor.BoundsCheckConfig
	log  log.Modular
}

// newBoundsCheck returns a BoundsCheck processor.
func newBoundsCheck(conf processor.BoundsCheckConfig, mgr bundle.NewManagement) (processor.V2Batched, error) {
	return &boundsCheck{
		conf: conf,
		log:  mgr.Logger(),
	}, nil
}

func (m *boundsCheck) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	lParts := msg.Len()
	if lParts < m.conf.MinParts {
		m.log.Debugf(
			"Rejecting message due to message parts below minimum (%v): %v\n",
			m.conf.MinParts, lParts,
		)
		return nil, nil
	} else if lParts > m.conf.MaxParts {
		m.log.Debugf(
			"Rejecting message due to message parts exceeding limit (%v): %v\n",
			m.conf.MaxParts, lParts,
		)
		return nil, nil
	}

	var reject bool
	_ = msg.Iter(func(i int, p *message.Part) error {
		if size := len(p.AsBytes()); size > m.conf.MaxPartSize ||
			size < m.conf.MinPartSize {
			m.log.Debugf(
				"Rejecting message due to message part size (%v -> %v): %v\n",
				m.conf.MinPartSize,
				m.conf.MaxPartSize,
				size,
			)
			reject = true
			return errors.New("exit")
		}
		return nil
	})
	if reject {
		return nil, nil
	}

	msgs := [1]message.Batch{msg}
	return msgs[:], nil
}

func (m *boundsCheck) Close(context.Context) error {
	return nil
}
