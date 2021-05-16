package service

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"gopkg.in/yaml.v3"
)

// ConfigSpec describes the configuration specification for a plugin
// component. This will be used for validating and linting configuration files
// and providing a parsed configuration struct to the plugin constructor.
type ConfigSpec struct {
	spec       docs.FieldSpec
	configCtor ConfigStructConstructor
}

func (c *ConfigSpec) fieldSpec() docs.FieldSpec {
	return c.spec
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
	return &ParsedConfig{generic: m}, nil
}

// NewConfigSpec creates a new empty component configuration spec. If the
// plugin does not require configuration fields the result of this call is
// enough.
func NewConfigSpec() *ConfigSpec {
	return &ConfigSpec{
		spec: docs.FieldComponent(),
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
func NewStructConfigSpec(ctor ConfigStructConstructor) *ConfigSpec {
	return &ConfigSpec{
		spec:       docs.FieldComponent(),
		configCtor: ctor,
	}
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
