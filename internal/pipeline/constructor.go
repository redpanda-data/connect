package pipeline

import (
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
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
		Threads:    -1,
		Processors: []processor.Config{},
	}
}

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(conf Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
	processors := make([]processor.V1, len(conf.Processors))
	for j, procConf := range conf.Processors {
		var err error
		pMgr := mgr.IntoPath("processors", strconv.Itoa(j))
		processors[j], err = pMgr.NewProcessor(procConf)
		if err != nil {
			return nil, err
		}
	}
	if conf.Threads == 1 {
		return NewProcessor(processors...), nil
	}
	return NewPool(conf.Threads, mgr.Logger(), processors...)
}
