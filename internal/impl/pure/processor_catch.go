package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("catch", service.NewConfigSpec().
		Stable().
		Categories("Composition").
		Summary("Applies a list of child processors _only_ when a previous processing step has failed.").
		Description(`
Behaves similarly to the `+"xref:components:processors/for_each.adoc[`for_each`]"+` processor, where a list of child processors are applied to individual messages of a batch. However, processors are only applied to messages that failed a processing step prior to the catch.

For example, with the following config:

`+"```yaml"+`
pipeline:
  processors:
    - resource: foo
    - catch:
      - resource: bar
      - resource: baz
`+"```"+`

If the processor `+"`foo`"+` fails for a particular message, that message will be fed into the processors `+"`bar` and `baz`"+`. Messages that do not fail for the processor `+"`foo`"+` will skip these processors.

When messages leave the catch block their fail flags are cleared. This processor is useful for when it's possible to recover failed messages, or when special actions (such as logging/metrics) are required before dropping them.

More information about error handling can be found in xref:configuration:error_handling.adoc[].`).
		LintRule(`if this.or([]).any(pconf -> pconf.type.or("") == "try" || pconf.try.type() == "array" ) {
  "'catch' block contains a 'try' block which will never execute due to errors only being cleared at the end of the 'catch', for more information about nesting 'try' within 'catch' read: https://www.docs.redpanda.com/redpanda-connect/components/processors/try#nesting-within-a-catch-block"
}`).
		Field(service.NewProcessorListField("").Default([]any{})),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			mgr := interop.UnwrapManagement(res)
			childPubProcs, err := conf.FieldProcessorList()
			if err != nil {
				return nil, err
			}

			childProcs := make([]processor.V1, len(childPubProcs))
			for i, p := range childPubProcs {
				childProcs[i] = interop.UnwrapOwnedProcessor(p)
			}

			tp, err := newCatch(childProcs)
			if err != nil {
				return nil, err
			}

			p := processor.NewAutoObservedBatchedProcessor("catch", tp, mgr)
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type catchProc struct {
	children []processor.V1
}

func newCatch(children []processor.V1) (*catchProc, error) {
	return &catchProc{
		children: children,
	}, nil
}

func (p *catchProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		resultMsgs[i] = message.Batch{p}
		return nil
	})

	var err error
	if resultMsgs, err = processor.ExecuteCatchAll(ctx.Context(), p.children, resultMsgs...); err != nil || len(resultMsgs) == 0 {
		return nil, err
	}

	resMsg := message.QuickBatch(nil)
	for _, m := range resultMsgs {
		_ = m.Iter(func(i int, p *message.Part) error {
			resMsg = append(resMsg, p)
			return nil
		})
	}
	if resMsg.Len() == 0 {
		return nil, nil
	}

	_ = resMsg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(nil)
		return nil
	})

	resMsgs := [1]message.Batch{resMsg}
	return resMsgs[:], nil
}

func (p *catchProc) Close(ctx context.Context) error {
	for _, child := range p.children {
		if err := child.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
