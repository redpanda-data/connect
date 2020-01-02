package stream

import (
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
)

//------------------------------------------------------------------------------

// Config is a configuration struct representing all four layers of a Benthos
// stream.
type Config struct {
	Input    input.Config    `json:"input" yaml:"input"`
	Buffer   buffer.Config   `json:"buffer" yaml:"buffer"`
	Pipeline pipeline.Config `json:"pipeline" yaml:"pipeline"`
	Output   output.Config   `json:"output" yaml:"output"`
}

// NewConfig returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Input:    input.NewConfig(),
		Buffer:   buffer.NewConfig(),
		Pipeline: pipeline.NewConfig(),
		Output:   output.NewConfig(),
	}
}

// Sanitised returns a sanitised copy of the Benthos configuration, meaning
// fields of no consequence (unused inputs, outputs, processors etc) are
// excluded.
func (c Config) Sanitised() (interface{}, error) {
	inConf, err := input.SanitiseConfig(c.Input)
	if err != nil {
		return nil, err
	}

	var bufConf interface{}
	bufConf, err = buffer.SanitiseConfig(c.Buffer)
	if err != nil {
		return nil, err
	}

	var pipeConf interface{}
	pipeConf, err = pipeline.SanitiseConfig(c.Pipeline)
	if err != nil {
		return nil, err
	}

	var outConf interface{}
	outConf, err = output.SanitiseConfig(c.Output)
	if err != nil {
		return nil, err
	}

	return struct {
		Input    interface{} `json:"input" yaml:"input"`
		Buffer   interface{} `json:"buffer" yaml:"buffer"`
		Pipeline interface{} `json:"pipeline" yaml:"pipeline"`
		Output   interface{} `json:"output" yaml:"output"`
	}{
		Input:    inConf,
		Buffer:   bufConf,
		Pipeline: pipeConf,
		Output:   outConf,
	}, nil
}

//------------------------------------------------------------------------------
