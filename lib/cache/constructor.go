package cache

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"

	"gopkg.in/yaml.v3"
)

// Config is the all encompassing configuration struct for all cache types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label  string      `json:"label" yaml:"label"`
	Type   string      `json:"type" yaml:"type"`
	Plugin interface{} `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Label:  "",
		Type:   "memory",
		Plugin: nil,
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

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeCache, aliased.Type, value); err != nil {
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

// New creates a cache type based on an cache configuration.
func New(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (cache.V1, error) {
	if mgrV2, ok := mgr.(interface {
		NewCache(conf Config) (cache.V1, error)
	}); ok {
		return mgrV2.NewCache(conf)
	}
	return nil, component.ErrInvalidCacheType
}
