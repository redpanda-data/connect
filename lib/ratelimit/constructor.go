package ratelimit

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

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
	for k, v := range Constructors {
		spec := docs.ComponentSpec{
			Type:        docs.TypeRateLimit,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      docs.FieldComponent().WithChildren(v.FieldSpecs...),
			Status:      v.Status,
			Version:     v.Version,
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
	for k, v := range pluginSpecs {
		spec := docs.ComponentSpec{
			Type:   docs.TypeRateLimit,
			Name:   k,
			Status: docs.StatusExperimental,
			Plugin: true,
			Config: docs.FieldComponent().Unlinted(),
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all cache types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each ratelimit type.
const (
	TypeLocal = "local"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all cache types.
type Config struct {
	Label  string      `json:"label" yaml:"label"`
	Type   string      `json:"type" yaml:"type"`
	Local  LocalConfig `json:"local" yaml:"local"`
	Plugin interface{} `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Label:  "",
		Type:   "local",
		Local:  NewLocalConfig(),
		Plugin: nil,
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig creates a sanitised version of a config.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	outputMap, err := config.SanitizeComponent(conf)
	if err != nil {
		return nil, err
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		if spec.confSanitiser != nil {
			outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
		}
	}
	if err := docs.SanitiseComponentConfig(
		docs.TypeRateLimit,
		map[string]interface{}(outputMap),
		docs.ShouldDropDeprecated(removeDeprecated),
	); err != nil {
		return nil, err
	}
	return outputMap, nil
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromNode(docs.TypeRateLimit, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigNode(aliased.Type, value)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
			conf := spec.confConstructor()
			if err = pluginNode.Decode(conf); err != nil {
				return fmt.Errorf("line %v: %v", value.Line, err)
			}
			aliased.Plugin = conf
		} else {
			aliased.Plugin = &pluginNode
		}
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-rate-limits`" + `

A rate limit is a strategy for limiting the usage of a shared resource across
parallel components in a Benthos instance, or potentially across multiple
instances.

For example, if we wanted to protect an HTTP service with a local rate limit
we could configure one like so:

` + "``` yaml" + `
input:
  type: foo
pipeline:
  threads: 8
  processors:
  - http:
      url: http://foo.bar/baz
      rate_limit: foobar
      parallel: true
resources:
  rate_limits:
    foobar:
      local:
        count: 500
        interval: 1s
` + "```" + `

In this example if the messages from the input ` + "`foo`" + ` are batches the
requests of a batch will be sent in parallel. This is usually going to be what
we want, but could potentially stress our HTTP server if a batch is large.

However, by using a rate limit we can guarantee that even across parallel
processing pipelines and variable sized batches we wont hit the service more
than 500 times per second.`

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our rate limit types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Rate Limits\n")
	buf.WriteString(strings.Repeat("=", 11))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = config.MarshalYAML(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(Constructors[name].Description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	return buf.String()
}

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
	if c, ok := pluginSpecs[conf.Type]; ok {
		rl, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create rate limit '%v': %v", conf.Type, err)
		}
		return rl, nil
	}
	return nil, types.ErrInvalidRateLimitType
}

//------------------------------------------------------------------------------
