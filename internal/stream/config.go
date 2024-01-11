package stream

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

const (
	fieldInput    = "input"
	fieldBuffer   = "buffer"
	fieldPipeline = "pipeline"
	fieldOutput   = "output"
)

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
func (c Config) Sanitised() (any, error) {
	var node yaml.Node
	if err := node.Encode(c); err != nil {
		return nil, err
	}

	sanitConf := docs.NewSanitiseConfig()
	sanitConf.RemoveTypeField = true
	if err := Spec().SanitiseYAML(&node, sanitConf); err != nil {
		return nil, err
	}

	var g any
	if err := node.Decode(&g); err != nil {
		return nil, err
	}
	return g, nil
}

// FromYAML is for old style tests.
func FromYAML(confStr string) (conf Config, err error) {
	var node *yaml.Node
	if node, err = docs.UnmarshalYAML([]byte(confStr)); err != nil {
		return
	}
	var pConf *docs.ParsedConfig
	if pConf, err = Spec().ParsedConfigFromAny(node); err != nil {
		return
	}
	conf, err = FromParsed(docs.DeprecatedProvider, pConf)
	return
}

func FromParsed(prov docs.Provider, pConf *docs.ParsedConfig) (conf Config, err error) {
	var v any

	if v, err = pConf.FieldAny(fieldInput); err != nil {
		return
	}
	if conf.Input, err = input.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldBuffer); err != nil {
		return
	}
	if conf.Buffer, err = buffer.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldPipeline); err != nil {
		return
	}
	if conf.Pipeline, err = pipeline.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldOutput); err != nil {
		return
	}
	if conf.Output, err = output.FromAny(prov, v); err != nil {
		return
	}
	return
}
