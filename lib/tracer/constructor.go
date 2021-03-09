package tracer

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Errors for the tracer package.
var (
	ErrInvalidTracerType = errors.New("invalid tracer type")
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each tracer type.
type TypeSpec struct {
	constructor func(conf Config, opts ...func(Type)) (Type, error)

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Footnotes   string
	config      docs.FieldSpec
	FieldSpecs  docs.FieldSpecs
}

// ConstructorFunc is a func signature able to construct a tracer.
type ConstructorFunc func(Config, ...func(Type)) (Type, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	for k, v := range Constructors {
		conf := v.config
		if len(v.FieldSpecs) > 0 {
			conf = docs.FieldComponent().WithChildren(v.FieldSpecs...)
		}
		spec := docs.ComponentSpec{
			Type:        docs.TypeTracer,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Status:      v.Status,
			Version:     v.Version,
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all tracer types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each tracer type.
const (
	TypeJaeger = "jaeger"
	TypeNone   = "none"
)

//------------------------------------------------------------------------------

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
		Type:   TypeNone,
		Jaeger: NewJaegerConfig(),
		None:   struct{}{},
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
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
	if err := docs.SanitiseComponentConfig(
		docs.TypeTracer,
		(map[string]interface{})(outputMap),
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

	if aliased.Type, _, err = docs.GetInferenceCandidateFromNode(docs.TypeTracer, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-tracers`" + `

A tracer type represents a destination for Benthos to send opentracing events to
such as [Jaeger](https://www.jaegertracing.io/).

When a tracer is configured all messages will be allocated a root span during
ingestion that represents their journey through a Benthos pipeline. Many Benthos
processors create spans, and so opentracing is a great way to analyse the
pathways of individual messages as they progress through a Benthos instance.

Some inputs, such as ` + "`http_server` and `http_client`" + `, are capable of
extracting a root span from the source of the message (HTTP headers). This is
a work in progress and should eventually expand so that all inputs have a way of
doing so.

A tracer config section looks like this:

` + "``` yaml" + `
tracer:
  jaeger:
    agent_address: localhost:6831
    sampler_param: 1
    sampler_type: const
    service_name: benthos
` + "```" + `

WARNING: Although the configuration spec of this component is stable the format
of spans, tags and logs created by Benthos is subject to change as it is tuned
for improvement.`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Tracer Types\n")
	buf.WriteString(strings.Repeat("=", 12))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

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

// New creates a tracer type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidTracerType
}

//------------------------------------------------------------------------------
