package processor

import (
	"context"
	"errors"

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
	Constructors[TypeBoundsCheck] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newBoundsCheck(conf.BoundsCheck, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("bounds_check", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Removes messages (and batches) that do not fit within certain size boundaries.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("max_part_size", "The maximum size of a message to allow (in bytes)"),
			docs.FieldCommon("min_part_size", "The minimum size of a message to allow (in bytes)"),
			docs.FieldAdvanced("max_parts", "The maximum size of message batches to allow (in message count)"),
			docs.FieldAdvanced("min_parts", "The minimum size of message batches to allow (in message count)"),
		},
	}
}

//------------------------------------------------------------------------------

// BoundsCheckConfig contains configuration fields for the BoundsCheck
// processor.
type BoundsCheckConfig struct {
	MaxParts    int `json:"max_parts" yaml:"max_parts"`
	MinParts    int `json:"min_parts" yaml:"min_parts"`
	MaxPartSize int `json:"max_part_size" yaml:"max_part_size"`
	MinPartSize int `json:"min_part_size" yaml:"min_part_size"`
}

// NewBoundsCheckConfig returns a BoundsCheckConfig with default values.
func NewBoundsCheckConfig() BoundsCheckConfig {
	return BoundsCheckConfig{
		MaxParts:    100,
		MinParts:    1,
		MaxPartSize: 1 * 1024 * 1024 * 1024, // 1GB
		MinPartSize: 1,
	}
}

//------------------------------------------------------------------------------

type boundsCheck struct {
	conf BoundsCheckConfig
	log  log.Modular
}

// newBoundsCheck returns a BoundsCheck processor.
func newBoundsCheck(conf BoundsCheckConfig, mgr interop.Manager) (processor.V2Batched, error) {
	return &boundsCheck{
		conf: conf,
		log:  mgr.Logger(),
	}, nil
}

//------------------------------------------------------------------------------

func (m *boundsCheck) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
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
		if size := len(p.Get()); size > m.conf.MaxPartSize ||
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

	msgs := [1]*message.Batch{msg}
	return msgs[:], nil
}

func (m *boundsCheck) Close(context.Context) error {
	return nil
}
