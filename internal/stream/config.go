package stream

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
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
	conf = NewConfig()
	err = conf.fromYAML(docs.DeprecatedProvider, node)
	return
}

func (c *Config) FromAny(prov docs.Provider, value any) (err error) {
	switch t := value.(type) {
	case Config:
		*c = t
		return
	case *yaml.Node:
		return c.fromYAML(prov, t)
	case map[string]any:
		return c.fromMap(prov, t)
	}
	err = fmt.Errorf("unexpected value, expected object, got %T", value)
	return
}

func (c *Config) fromMap(prov docs.Provider, value map[string]any) (err error) {
	if iConf, exists := value["input"]; exists {
		if c.Input, err = input.FromAny(prov, iConf); err != nil {
			return
		}
	}
	if pConf, exists := value["pipeline"]; exists {
		if c.Pipeline, err = pipeline.FromAny(prov, pConf); err != nil {
			return
		}
	}
	if bConf, exists := value["buffer"]; exists {
		if c.Buffer, err = buffer.FromAny(prov, bConf); err != nil {
			return
		}
	}
	if oConf, exists := value["output"]; exists {
		if c.Output, err = output.FromAny(prov, oConf); err != nil {
			return
		}
	}
	return
}

func (c *Config) fromYAML(prov docs.Provider, value *yaml.Node) (err error) {
	for i := 0; i < len(value.Content)-1; i += 2 {
		switch value.Content[i].Value {
		case "input":
			if c.Input, err = input.FromAny(prov, value.Content[i+1]); err != nil {
				return
			}
		case "buffer":
			if c.Buffer, err = buffer.FromAny(prov, value.Content[i+1]); err != nil {
				return
			}
		case "pipeline":
			if c.Pipeline, err = pipeline.FromAny(prov, value.Content[i+1]); err != nil {
				return
			}
		case "output":
			if c.Output, err = output.FromAny(prov, value.Content[i+1]); err != nil {
				return
			}
		}
	}
	return
}
