package pure

import (
	"context"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newTryProc(conf.Try, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("try", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "try",
		Categories: []string{
			"Composition",
		},
		Summary: `Executes a list of child processors on messages only if no prior processors have failed (or the errors have been cleared).`,
		Description: `
This processor behaves similarly to the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor, where a list of child processors are applied to individual messages of a batch. However, if a message has failed any prior processor (before or during the try block) then that message will skip all following processors.

For example, with the following config:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo
    - try:
      - resource: bar
      - resource: baz
      - resource: buz
` + "```" + `

If the processor ` + "`bar`" + ` fails for a particular message, that message will skip the processors ` + "`baz` and `buz`" + `. Similarly, if ` + "`bar`" + ` succeeds but ` + "`baz`" + ` does not then ` + "`buz`" + ` will be skipped. If the processor ` + "`foo`" + ` fails for a message then none of ` + "`bar`, `baz` or `buz`" + ` are executed on that message.

This processor is useful for when child processors depend on the successful output of previous processors. This processor can be followed with a ` + "[catch](/docs/components/processors/catch)" + ` processor for defining child processors to be applied only to failed messages.

More information about error handing can be found [here](/docs/configuration/error_handling).

### Nesting within a catch block

In some cases it might be useful to nest a try block within a catch block, since the ` + "[`catch` processor](/docs/components/processors/catch)" + ` only clears errors _after_ executing its child processors this means a nested try processor will not execute unless the errors are explicitly cleared beforehand.

This can be done by inserting an empty catch block before the try block like as follows:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo
    - catch:
      - log:
          level: ERROR
          message: "Foo failed due to: ${! error() }"
      - catch: [] # Clear prior error
      - try:
        - resource: bar
        - resource: baz
` + "```" + `


`,
		Config: docs.FieldProcessor("", "").Array().HasDefault([]any{}),
	})
	if err != nil {
		panic(err)
	}
}

type tryProc struct {
	children []processor.V1
	log      log.Modular
}

func newTryProc(conf []processor.Config, mgr bundle.NewManagement) (*tryProc, error) {
	var children []processor.V1
	for i, pconf := range conf {
		pMgr := mgr.IntoPath("try", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &tryProc{
		children: children,
		log:      mgr.Logger(),
	}, nil
}

func (p *tryProc) ProcessBatch(ctx context.Context, _ []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		resultMsgs[i] = message.Batch{p}
		return nil
	})

	var res error
	if resultMsgs, res = processor.ExecuteTryAll(ctx, p.children, resultMsgs...); res != nil || len(resultMsgs) == 0 {
		return nil, res
	}

	resMsg := message.QuickBatch(nil)
	for _, m := range resultMsgs {
		_ = m.Iter(func(i int, p *message.Part) error {
			resMsg = append(resMsg, p)
			return nil
		})
	}
	if resMsg.Len() == 0 {
		return nil, res
	}

	resMsgs := [1]message.Batch{resMsg}
	return resMsgs[:], nil
}

func (p *tryProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
