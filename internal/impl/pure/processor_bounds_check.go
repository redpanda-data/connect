package pure

import (
	"context"
	"errors"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	bcpFieldMaxParts    = "max_parts"
	bcpFieldMinParts    = "min_parts"
	bcpFieldMaxPartSize = "max_part_size"
	bcpFieldMinPartSize = "min_part_size"
)

func bcProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary("Removes messages (and batches) that do not fit within certain size boundaries.").
		Fields(
			service.NewIntField(bcpFieldMaxPartSize).
				Description("The maximum size of a message to allow (in bytes)").
				Default(1*1024*1024*1024),
			service.NewIntField(bcpFieldMinPartSize).
				Description("The minimum size of a message to allow (in bytes)").
				Default(1),
			service.NewIntField(bcpFieldMaxParts).
				Description("The maximum size of message batches to allow (in message count)").
				Advanced().
				Default(100),
			service.NewIntField(bcpFieldMinParts).
				Description("The minimum size of message batches to allow (in message count)").
				Advanced().
				Default(1),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"bounds_check", bcProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			maxParts, err := conf.FieldInt(bcpFieldMaxParts)
			if err != nil {
				return nil, err
			}

			minParts, err := conf.FieldInt(bcpFieldMinParts)
			if err != nil {
				return nil, err
			}

			maxPartSize, err := conf.FieldInt(bcpFieldMaxPartSize)
			if err != nil {
				return nil, err
			}

			minPartSize, err := conf.FieldInt(bcpFieldMinPartSize)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newBoundsCheck(maxParts, minParts, maxPartSize, minPartSize, mgr)
			if err != nil {
				return nil, err
			}

			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("bounds_check", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type boundsCheck struct {
	maxParts    int
	minParts    int
	maxPartSize int
	minPartSize int
	log         log.Modular
}

// newBoundsCheck returns a BoundsCheck processor.
func newBoundsCheck(maxParts, minParts, maxPartSize, minPartSize int, mgr bundle.NewManagement) (processor.AutoObservedBatched, error) {
	return &boundsCheck{
		maxParts:    maxParts,
		minParts:    minParts,
		maxPartSize: maxPartSize,
		minPartSize: minPartSize,
		log:         mgr.Logger(),
	}, nil
}

func (m *boundsCheck) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	lParts := msg.Len()
	if lParts < m.minParts {
		m.log.Debug(
			"Rejecting message due to message parts below minimum (%v): %v\n",
			m.minParts, lParts,
		)
		return nil, nil
	} else if lParts > m.maxParts {
		m.log.Debug(
			"Rejecting message due to message parts exceeding limit (%v): %v\n",
			m.maxParts, lParts,
		)
		return nil, nil
	}

	var reject bool
	_ = msg.Iter(func(i int, p *message.Part) error {
		if size := len(p.AsBytes()); size > m.maxPartSize ||
			size < m.minPartSize {
			m.log.Debug(
				"Rejecting message due to message part size (%v -> %v): %v\n",
				m.minPartSize,
				m.maxPartSize,
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
