package service

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"
)

// ConfigField describes a field within a component configuration, to be added
// to a ConfigSpec.
type ConfigField struct {
	field docs.FieldSpec
}

// NewStringField describes a new string type config field.
func NewStringField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, "").HasType(docs.FieldTypeString),
	}
}

// NewStringListField describes a new config field consisting of a list of
// strings.
func NewStringListField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, "").Array().HasType(docs.FieldTypeString),
	}
}

// NewIntField describes a new int type config field.
func NewIntField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, "").HasType(docs.FieldTypeInt),
	}
}

// NewFloatField describes a new float type config field.
func NewFloatField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, "").HasType(docs.FieldTypeFloat),
	}
}

// NewBoolField describes a new bool type config field.
func NewBoolField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldCommon(name, "").HasType(docs.FieldTypeBool),
	}
}

// NewObjectField describes a new object type config field, consisting of one
// or more child fields.
func NewObjectField(name string, fields ...*ConfigField) *ConfigField {
	children := make([]docs.FieldSpec, len(fields))
	for i, f := range fields {
		children[i] = f.field
	}
	return &ConfigField{
		field: docs.FieldCommon(name, "").WithChildren(children...),
	}
}

// Description adds a description to the field which will be shown when printing
// documentation for the component config spec.
func (c *ConfigField) Description(d string) *ConfigField {
	c.field.Description = d
	return c
}

// Default specifies a default value that this field will assume if it is
// omitted from a provided config. Fields that do not have a default value are
// considered mandatory, and so parsing a config will fail in their absence.
func (c *ConfigField) Default(v interface{}) *ConfigField {
	c.field = c.field.HasDefault(v)
	return c
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

	fields, err := c.component.Config.Children.YAMLToMap(false, node)
	if err != nil {
		return nil, err
	}

	return &ParsedConfig{generic: fields}, nil
}

// NewConfigSpec creates a new empty component configuration spec. If the
// plugin does not require configuration fields the result of this call is
// enough.
func NewConfigSpec() *ConfigSpec {
	return &ConfigSpec{
		component: docs.ComponentSpec{
			Status: docs.StatusExperimental,
			Plugin: true,
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
	confSpec.component.Config = confSpec.component.Config.WithChildren(docs.FieldsFromYAML(&node)...)
	confSpec.configCtor = ctor

	return confSpec, nil
}

// Stable sets a documentation label on the component indicating that its
// configuration spec is stable. Plugins are considered experimental by default.
func (c *ConfigSpec) Stable() *ConfigSpec {
	c.component.Status = docs.StatusStable
	return c
}

// Beta sets a documentation label on the component indicating that its
// configuration spec is ready for beta testing, meaning backwards incompatible
// changes will not be made unless a fundamental problem is found. Plugins are
// considered experimental by default.
func (c *ConfigSpec) Beta() *ConfigSpec {
	c.component.Status = docs.StatusBeta
	return c
}

// Categories adds one or more string tags to the component, these are used for
// arbitrarily grouping components in documentation.
func (c *ConfigSpec) Categories(categories ...string) *ConfigSpec {
	c.component.Categories = categories
	return c
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
	generic  map[string]interface{}
}

// Root returns the root of the parsed config. If the configuration spec was
// built around a config constructor then the value returned will match the type
// returned by the constructor, otherwise it will be a generic
// map[string]interface{} type.
func (p *ParsedConfig) Root() interface{} {
	if p.asStruct != nil {
		return p.asStruct
	}
	return p.generic
}

// Field accesses a field from the parsed config by its name and returns the
// value if the field is found and a boolean indicating whether it was found.
// Nested fields can be accessed by specifing the series of field names.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) field(path ...string) (interface{}, bool) {
	gObj := gabs.Wrap(p.generic)
	if exists := gObj.Exists(path...); !exists {
		return nil, false
	}
	return gObj.S(path...).Data(), true
}

// FieldString accesses a string field from the parsed config by its name. If
// the field is not found or is not a string an error is returned.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) FieldString(path ...string) (string, error) {
	v, exists := p.field(path...)
	if !exists {
		return "", fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	str, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, "."), v)
	}
	return str, nil
}

// FieldStringList accesses a field that is a list of strings from the parsed
// config by its name and returns the value. Returns an error if the field is
// not found, or is not a list of strings.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) FieldStringList(path ...string) ([]string, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	iList, ok := v.([]interface{})
	if !ok {
		if sList, ok := v.([]string); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a string list, got %T", strings.Join(path, "."), v)
	}
	sList := make([]string, len(iList))
	for i, ev := range iList {
		if sList[i], ok = ev.(string); !ok {
			return nil, fmt.Errorf("expected field '%v' to be a string list, found an element of type %T", strings.Join(path, "."), ev)
		}
	}
	return sList, nil
}

// FieldInt accesses an int field from the parsed config by its name and returns
// the value. Returns an error if the field is not found or is not an int.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) FieldInt(path ...string) (int, error) {
	v, exists := p.field(path...)
	if !exists {
		return 0, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	i, ok := v.(int)
	if !ok {
		return 0, fmt.Errorf("expected field '%v' to be an int, got %T", strings.Join(path, "."), v)
	}
	return i, nil
}

// FieldFloat accesses a float field from the parsed config by its name and
// returns the value. Returns an error if the field is not found or is not a
// float.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) FieldFloat(path ...string) (float64, error) {
	v, exists := p.field(path...)
	if !exists {
		return 0, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected field '%v' to be a float, got %T", strings.Join(path, "."), v)
	}
	return f, nil
}

// FieldBool accesses a bool field from the parsed config by its name and
// returns the value. Returns an error if the field is not found or is not a
// bool.
//
// This method is not valid when the configuration spec was built around a
// config constructor.
func (p *ParsedConfig) FieldBool(path ...string) (bool, error) {
	v, e := p.field(path...)
	if !e {
		return false, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected field '%v' to be a bool, got %T", strings.Join(path, "."), v)
	}
	return b, nil
}
