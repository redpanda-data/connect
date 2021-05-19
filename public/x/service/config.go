package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"gopkg.in/yaml.v3"
)

// ConfigField describes a field within a component configuration, to be added
// to a ConfigSpec.
type ConfigField struct {
	field docs.FieldSpec
}

// NewConfigField describes a new config field with basic information including
// a field name and a description.
func NewConfigField(name, description string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, description),
	}
}

//------------------------------------------------------------------------------

// ConfigSpec describes the configuration specification for a plugin
// component. This will be used for validating and linting configuration files
// and providing a parsed configuration struct to the plugin constructor.
type ConfigSpec struct {
	component  docs.ComponentSpec
	configCtor ConfigStructConstructor
}

func (c *ConfigSpec) configFromNode(node *yaml.Node) (*ParsedConfig, error) {
	if c.configCtor != nil {
		conf := c.configCtor()
		if err := node.Decode(conf); err != nil {
			return nil, err
		}
		return &ParsedConfig{asStruct: conf}, nil
	}

	var m interface{}
	if err := node.Decode(&m); err != nil {
		return nil, err
	}

	// TODO: When field specs support explicit defaults we can walk them here
	// and populate the generic representation with those values.
	return &ParsedConfig{generic: m}, nil
}

// NewConfigSpec creates a new empty component configuration spec. If the
// plugin does not require configuration fields the result of this call is
// enough.
func NewConfigSpec() *ConfigSpec {
	return &ConfigSpec{
		component: docs.ComponentSpec{
			Status: docs.StatusPlugin,
			Config: docs.FieldComponent(),
		},
	}
}

// ConfigStructConstructor is a function signature that must return a pointer to
// a struct to be used for parsing configuration fields of a component plugin,
// ideally instanciated with default field values.
//
// The function will be called each time a parsed configuration file contains
// the plugin type, and the returned struct will be unmarshalled as YAML using
// gopkg.in/yaml.v3.
//
// The returned value must be a pointer type in order to be properly
// unmarshalled during config parsing.
type ConfigStructConstructor func() interface{}

// NewStructConfigSpec creates a new component configuration spec around a
// constructor func. The provided constructor func will be used during parsing
// in order to validate and return fields for the plugin from a configuration
// file.
func NewStructConfigSpec(ctor ConfigStructConstructor) (*ConfigSpec, error) {
	var node yaml.Node
	if err := node.Encode(ctor()); err != nil {
		return nil, fmt.Errorf("unable to marshal config struct as yaml: %v", err)
	}

	confSpec := NewConfigSpec()
	confSpec.component.Config = confSpec.component.Config.WithChildren(docs.FieldsFromNode(&node)...)
	confSpec.configCtor = ctor

	return confSpec, nil
}

// Summary adds a short summary to the plugin configuration spec that describes
// the general purpose of the component.
func (c *ConfigSpec) Summary(summary string) *ConfigSpec {
	c.component.Summary = summary
	return c
}

// Description adds a description to the plugin configuration spec that
// describes in more detail the behaviour of the component and how it should be
// used.
func (c *ConfigSpec) Description(description string) *ConfigSpec {
	c.component.Description = description
	return c
}

// Field sets the specification of a field within the config spec, used for
// linting and generating documentation for the component.
//
// When creating a spec with a struct constructor the fields from that struct
// will already be inferred. However, setting a field explicitly is sometimes
// useful for enriching the field documentation with more information.
func (c *ConfigSpec) Field(f *ConfigField) *ConfigSpec {
	for i, s := range c.component.Config.Children {
		if s.Name == f.field.Name {
			c.component.Config.Children[i] = f.field
			return c
		}
	}
	c.component.Config.Children = append(c.component.Config.Children, f.field)
	return c
}

//------------------------------------------------------------------------------

// ParsedConfig represents a plugin configuration that has been validated and
// parsed from a ConfigSpec, and allows plugin constructors to access
// configuration fields.
//
// The correct way to access configuration fields depends on how the
// configuration spec was built. For example, if the spec was established with
// a struct constructor then the method AsStruct should be used in order to
// access the parsed struct.
type ParsedConfig struct {
	asStruct interface{}
	generic  interface{}
}

// AsStruct returns a struct parsed from a plugin configuration. If the config
// spec that resulted in this parsed config was not defined using a struct
// constructor then nil is returned.
func (p *ParsedConfig) AsStruct() interface{} {
	return p.asStruct
}
