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

func processorsProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary(`A processor grouping several sub-processors while retaining batches`).
		Description(`
Quite often you will come across a case where you want to collect several processors under a single processor, whether 
it is for making your configuration easier to read and navigate, or for improving the testability of your 
configuration. You could use a the ['try'](https://benthos.dev/docs/components/processors/try) processor for this 
purpose, if you can live with each message being processed individually. However, if you want to retain the messages
in their batch, you can use this processor instead.'

You can find out more about batching [in this doc](/docs/configuration/batching).`).
		Example(
			"Grouped Processing",
			"Imagine we have a collection of processors who cover a specific functionality. We could use this processor to group them together and make it easier to read and mock during testing by giving the whole block a label:",
			`
pipeline:
  processors:
    - label: my_super_feature
      processors:
        - log:
            message: "Let's do something cool"
		- archive:
			format: json_array
		- mapping: root.items = this
		- http:
			url: https://example.com/endpoint
	        verb: POST
`,
		).
		Field(service.NewProcessorListField("").Default([]any{}))
}

func init() {
	err := service.RegisterBatchProcessor(
		"processors", processorsProcSpec(),
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

			pp, err := newProcessorProc(childProcs, mgr)
			if err != nil {
				return nil, err
			}

			p := processor.NewAutoObservedBatchedProcessor("processors", pp, mgr)
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

func newProcessorProc(children []processor.V1, mgr bundle.NewManagement) (*processorProc, error) {
	return &processorProc{
		children: children,
		log:      mgr.Logger(),
	}, nil
}

type processorProc struct {
	children []processor.V1
	log      log.Modular
}

func (p *processorProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	if msg.Len() == 0 {
		return nil, nil
	}

	msgs := []message.Batch{}
	resultMsgs, res := processor.ExecuteAll(ctx.Context(), p.children, msg)
	if len(resultMsgs) > 0 {
		msgs = append(msgs, resultMsgs...)
	}
	if res != nil {
		if err := res; err != nil {
			p.log.Error("Processor error: %v\n", err)
		}
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	return msgs, nil
}

func (p *processorProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
