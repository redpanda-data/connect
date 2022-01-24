package pipeline

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

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

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	processorCtors ...types.ProcessorConstructorFunc,
) (Type, error) {
	processors := make([]types.Processor, len(conf.Processors)+len(processorCtors))
	for j, procConf := range conf.Processors {
		pMgr, pLog, pMetrics := interop.LabelChild(fmt.Sprintf("processor.%v", j), mgr, log, stats)
		var err error
		processors[j], err = processor.New(procConf, pMgr, pLog, pMetrics)
		if err != nil {
			return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
		}
	}
	for j, procCtor := range processorCtors {
		var err error
		processors[j+len(conf.Processors)], err = procCtor()
		if err != nil {
			return nil, fmt.Errorf("failed to create processor: %v", err)
		}
	}

	if conf.Threads == 1 {
		return NewProcessor(log, stats, processors...), nil
	}
	return newPoolV2(conf.Threads, log, stats, processors...)
}
