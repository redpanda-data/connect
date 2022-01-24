package tracer

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	yaml "gopkg.in/yaml.v3"
)

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
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.config
		if len(v.FieldSpecs) > 0 {
			conf = docs.FieldComponent().WithChildren(v.FieldSpecs.DefaultAndTypeFrom(inferred[k])...)
		} else {
			conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
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

	if aliased.Type, _, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeTracer, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// New creates a tracer type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidTracerType
}
