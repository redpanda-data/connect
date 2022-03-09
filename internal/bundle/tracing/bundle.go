package tracing

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	iinput "github.com/benthosdev/benthos/v4/internal/component/input"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/output"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
)

// TracedBundle modifies a provided bundle environment so that traceable
// components are wrapped by components that add trace events to the returned
// summary.
func TracedBundle(b *bundle.Environment) (*bundle.Environment, *Summary) {
	summary := NewSummary()
	tracedEnv := b.Clone()

	for _, spec := range b.InputDocs() {
		_ = tracedEnv.InputAdd(func(conf input.Config, nm bundle.NewManagement, pcf ...iprocessor.PipelineConstructorFunc) (iinput.Streamed, error) {
			i, err := b.InputInit(conf, nm, pcf...)
			if err != nil {
				return nil, err
			}
			key := nm.Label()
			if key == "" {
				key = "root." + query.SliceToDotPath(nm.Path()...)
			}
			iEvents, ctr := summary.wInputEvents(key)
			i = traceInput(iEvents, ctr, i)
			return i, err
		}, spec)
	}

	for _, spec := range b.ProcessorDocs() {
		_ = tracedEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (iprocessor.V1, error) {
			i, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}
			key := nm.Label()
			if key == "" {
				key = "root." + query.SliceToDotPath(nm.Path()...)
			}
			pEvents, errCtr := summary.wProcessorEvents(key)
			i = traceProcessor(pEvents, errCtr, i)
			return i, err
		}, spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = tracedEnv.OutputAdd(func(conf output.Config, nm bundle.NewManagement, pcf ...iprocessor.PipelineConstructorFunc) (ioutput.Streamed, error) {
			pcf = output.AppendProcessorsFromConfig(conf, nm, nm.Logger(), nm.Metrics(), pcf...)
			conf.Processors = nil

			o, err := b.OutputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			key := nm.Label()
			if key == "" {
				key = "root." + query.SliceToDotPath(nm.Path()...)
			}
			oEvents, ctr := summary.wOutputEvents(key)
			o = traceOutput(oEvents, ctr, o)

			return output.WrapWithPipelines(o, pcf...)
		}, spec)
	}

	return tracedEnv, summary
}
