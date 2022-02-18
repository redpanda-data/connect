package processor

import (
	"context"
	"fmt"
	"strconv"
	"time"

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
	Constructors[TypeForEach] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newForEach(conf.ForEach, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("for_each", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message.`,
		Description: `
This is useful for forcing batch wide processors such as
` + "[`dedupe`](/docs/components/processors/dedupe)" + ` or interpolations such
as the ` + "`value`" + ` field of the ` + "`metadata`" + ` processor to execute
on individual message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.`,
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeProcessor),
	}
}

//------------------------------------------------------------------------------

// ForEachConfig is a config struct containing fields for the ForEach
// processor.
type ForEachConfig []Config

// NewForEachConfig returns a default ForEachConfig.
func NewForEachConfig() ForEachConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

type forEachProc struct {
	children []processor.V1
}

func newForEach(conf ForEachConfig, mgr interop.Manager) (*forEachProc, error) {
	var children []processor.V1
	for i, pconf := range conf {
		pMgr := mgr.IntoPath("for_each", strconv.Itoa(i))
		proc, err := New(pconf, pMgr, pMgr.Logger(), pMgr.Metrics())
		if err != nil {
			return nil, fmt.Errorf("child processor [%v]: %w", i, err)
		}
		children = append(children, proc)
	}
	return &forEachProc{children: children}, nil
}

func (p *forEachProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	individualMsgs := make([]*message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.SetAll([]*message.Part{p})
		individualMsgs[i] = tmpMsg
		return nil
	})

	resMsg := message.QuickBatch(nil)
	for _, tmpMsg := range individualMsgs {
		resultMsgs, err := ExecuteAll(p.children, tmpMsg)
		if err != nil {
			return nil, err
		}
		for _, m := range resultMsgs {
			_ = m.Iter(func(i int, p *message.Part) error {
				resMsg.Append(p)
				return nil
			})
		}
	}

	if resMsg.Len() == 0 {
		return nil, nil
	}
	return []*message.Batch{resMsg}, nil
}

func (p *forEachProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		c.CloseAsync()
	}
	deadline, exists := ctx.Deadline()
	if !exists {
		deadline = time.Now().Add(time.Second * 5)
	}
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(deadline)); err != nil {
			return err
		}
	}
	return nil
}
