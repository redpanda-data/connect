package ratelimit

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

type ratelimitConstructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.RateLimit, error)

// TypeSpec is a constructor and a usage description for each ratelimit type.
type TypeSpec struct {
	constructor ratelimitConstructor

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
}

// ConstructorFunc is a func signature able to construct a rate limiter.
type ConstructorFunc func(Config, types.Manager, log.Modular, metrics.Type) (types.RateLimit, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		spec := docs.ComponentSpec{
			Type:        docs.TypeRateLimit,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      docs.FieldComponent().WithChildren(v.FieldSpecs.DefaultAndTypeFrom(inferred[k])...),
			Status:      v.Status,
			Version:     v.Version,
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all cache types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each ratelimit type.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
const (
	TypeLocal = "local"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all cache types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label  string      `json:"label" yaml:"label"`
	Type   string      `json:"type" yaml:"type"`
	Local  LocalConfig `json:"local" yaml:"local"`
	Plugin interface{} `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
func NewConfig() Config {
	return Config{
		Label:  "",
		Type:   "local",
		Local:  NewLocalConfig(),
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeRateLimit, aliased.Type, value); err != nil {
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

//------------------------------------------------------------------------------

// New creates a rate limit type based on an rate limit configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (types.RateLimit, error) {
	if mgrV2, ok := mgr.(interface {
		NewRateLimit(conf Config) (types.RateLimit, error)
	}); ok {
		return mgrV2.NewRateLimit(conf)
	}
	if c, ok := Constructors[conf.Type]; ok {
		rl, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create rate limit '%v': %v", conf.Type, err)
		}
		return rl, nil
	}
	return nil, types.ErrInvalidRateLimitType
}
