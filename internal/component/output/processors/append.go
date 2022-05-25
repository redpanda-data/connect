package processors

import (
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// AppendFromConfig takes a variant arg of pipeline constructor functions and
// returns a new slice of them where the processors of the provided output
// configuration will also be initialized.
func AppendFromConfig(conf output.Config, mgr bundle.NewManagement, pipelines ...processor.PipelineConstructorFunc) []processor.PipelineConstructorFunc {
	if len(conf.Processors) > 0 {
		pipelines = append(pipelines, []processor.PipelineConstructorFunc{func() (processor.Pipeline, error) {
			processors := make([]processor.V1, len(conf.Processors))
			for j, procConf := range conf.Processors {
				var err error
				pMgr := mgr.IntoPath("processors", strconv.Itoa(j))
				processors[j], err = pMgr.NewProcessor(procConf)
				if err != nil {
					return nil, err
				}
			}
			return pipeline.NewProcessor(processors...), nil
		}}...)
	}
	return pipelines
}

// WrapConstructor provides a way to define an output constructor without
// manually initializing processors of the config.
func WrapConstructor(fn func(output.Config, bundle.NewManagement) (output.Streamed, error)) bundle.OutputConstructor {
	return func(c output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
		o, err := fn(c, nm)
		if err != nil {
			return nil, err
		}
		pcf = AppendFromConfig(c, nm, pcf...)
		return output.WrapWithPipelines(o, pcf...)
	}
}
