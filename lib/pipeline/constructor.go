package pipeline

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Config is a configuration struct for creating parallel processing pipelines.
// The number of resuling parallel processing pipelines will match the number of
// threads specified. Processors are executed on each message in the order that
// they are written.
//
// In order to fully utilise each processing thread you must either have a
// number of parallel inputs that matches or surpasses the number of pipeline
// threads, or use a memory buffer.
type Config struct {
	Threads    int                `json:"threads" yaml:"threads"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Threads:    1,
		Processors: []processor.Config{},
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	procConfs := make([]interface{}, len(conf.Processors))
	for i, pConf := range conf.Processors {
		var err error
		if procConfs[i], err = pConf.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{
		"threads":    conf.Threads,
		"processors": procConfs,
	}, nil
}

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	processorCtors ...types.ProcessorConstructorFunc,
) (Type, error) {
	procs := 0
	procCtor := func(i *int) (types.Pipeline, error) {
		processors := make([]types.Processor, len(conf.Processors)+len(processorCtors))
		for j, procConf := range conf.Processors {
			prefix := fmt.Sprintf("processor.%v", *i)
			var err error
			processors[j], err = processor.New(procConf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
			if err != nil {
				return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
			}
			*i++
		}
		for j, procCtor := range processorCtors {
			var err error
			processors[j+len(conf.Processors)], err = procCtor()
			if err != nil {
				return nil, fmt.Errorf("failed to create processor: %v", err)
			}
		}
		return NewProcessor(log, stats, processors...), nil
	}
	if conf.Threads == 1 {
		return procCtor(&procs)
	}
	return NewPool(procCtor, conf.Threads, log, stats)
}

//------------------------------------------------------------------------------
