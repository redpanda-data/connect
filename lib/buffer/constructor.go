package buffer

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

// Config is the all encompassing configuration struct for all buffer types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Type   string      `json:"type" yaml:"type"`
	Plugin interface{} `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:   "none",
		Plugin: nil,
	}
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeBuffer, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

// New creates a buffer type based on a buffer configuration.
func New(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if mgrV2, ok := mgr.(interface {
		NewBuffer(conf Config) (Type, error)
	}); ok {
		return mgrV2.NewBuffer(conf)
	}
	return nil, component.ErrInvalidBufferType
}
