package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("try", service.NewConfigSpec().
		Stable().
		Categories("Composition").
		Summary("Executes a list of child processors on messages only if no prior processors have failed (or the errors have been cleared).").
		Description(`
This processor behaves similarly to the `+"xref:components:processors/for_each.adoc[`for_each`]"+` processor, where a list of child processors are applied to individual messages of a batch. However, if a message has failed any prior processor (before or during the try block) then that message will skip all following processors.

For example, with the following config:

`+"```yaml"+`
pipeline:
  processors:
    - resource: foo
    - try:
      - resource: bar
      - resource: baz
      - resource: buz
`+"```"+`

If the processor `+"`bar`"+` fails for a particular message, that message will skip the processors `+"`baz` and `buz`"+`. Similarly, if `+"`bar`"+` succeeds but `+"`baz`"+` does not then `+"`buz`"+` will be skipped. If the processor `+"`foo`"+` fails for a message then none of `+"`bar`, `baz` or `buz`"+` are executed on that message.

This processor is useful for when child processors depend on the successful output of previous processors. This processor can be followed with a `+"xref:components:processors/catch.adoc[catch]"+` processor for defining child processors to be applied only to failed messages.

More information about error handing can be found in xref:configuration:error_handling.adoc[].

== Nest within a catch block

In some cases it might be useful to nest a try block within a catch block, since the `+"xref:components:processors/catch.adoc[`catch` processor]"+` only clears errors _after_ executing its child processors this means a nested try processor will not execute unless the errors are explicitly cleared beforehand.

This can be done by inserting an empty catch block before the try block like as follows:

`+"```yaml"+`
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
`+"```"+``).
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

			tp, err := newTryProc(childProcs, mgr)
			if err != nil {
				return nil, err
			}

			p := processor.NewAutoObservedBatchedProcessor("try", tp, mgr)
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

type tryProc struct {
	children []processor.V1
	log      log.Modular
}

func newTryProc(children []processor.V1, mgr bundle.NewManagement) (*tryProc, error) {
	return &tryProc{
		children: children,
		log:      mgr.Logger(),
	}, nil
}

func (p *tryProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		resultMsgs[i] = message.Batch{p}
		return nil
	})

	var err error
	if resultMsgs, err = processor.ExecuteTryAll(ctx.Context(), p.children, resultMsgs...); err != nil || len(resultMsgs) == 0 {
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
