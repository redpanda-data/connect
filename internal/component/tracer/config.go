package tracer

import (
	"fmt"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Type is an interface implemented by all tracer types.
type Type interface {
	// Close stops and cleans up the tracers resources.
	Close() error
}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all tracer types.
type Config struct {
	Type   string       `json:"type" yaml:"type"`
	Jaeger JaegerConfig `json:"jaeger" yaml:"jaeger"`
	None   struct{}     `json:"none" yaml:"none"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:   "none",
		Jaeger: NewJaegerConfig(),
		None:   struct{}{},
	}
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	if aliased.Type, _, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeTracer, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	*conf = Config(aliased)
	return nil
}
