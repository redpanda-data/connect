package pure

import (
	"context"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newForEach(conf.ForEach, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("for_each", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "for_each",
		Categories: []string{
			"Composition",
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
		Config: docs.FieldProcessor("", "").Array().HasDefault([]any{}),
	})
	if err != nil {
		panic(err)
	}
}

type forEachProc struct {
	children []processor.V1
}

func newForEach(conf []processor.Config, mgr bundle.NewManagement) (*forEachProc, error) {
	var children []processor.V1
	for i, pconf := range conf {
		pMgr := mgr.IntoPath("for_each", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, fmt.Errorf("child processor [%v]: %w", i, err)
		}
		children = append(children, proc)
	}
	return &forEachProc{children: children}, nil
}

func (p *forEachProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	individualMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		individualMsgs[i] = message.Batch{p}
		return nil
	})

	resMsg := message.QuickBatch(nil)
	for _, tmpMsg := range individualMsgs {
		resultMsgs, err := processor.ExecuteAll(ctx, p.children, tmpMsg)
		if err != nil {
			return nil, err
		}
		for _, m := range resultMsgs {
			_ = m.Iter(func(i int, p *message.Part) error {
				resMsg = append(resMsg, p)
				return nil
			})
		}
	}

	if resMsg.Len() == 0 {
		return nil, nil
	}
	return []message.Batch{resMsg}, nil
}

func (p *forEachProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
