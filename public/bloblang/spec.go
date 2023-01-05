package bloblang

import (
	"encoding/json"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// ParamDefinition describes a single parameter for a function or method.
type ParamDefinition struct {
	def query.ParamDefinition
}

// NewStringParam creates a new string typed parameter. Parameter names must
// match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewStringParam(name string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamString(name, ""),
	}
}

// NewInt64Param creates a new 64-bit integer typed parameter. Parameter names
// must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewInt64Param(name string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamInt64(name, ""),
	}
}

// NewFloat64Param creates a new float64 typed parameter. Parameter names must
// match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewFloat64Param(name string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamFloat(name, ""),
	}
}

// NewBoolParam creates a new bool typed parameter. Parameter names must match
// the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewBoolParam(name string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamBool(name, ""),
	}
}

// NewAnyParam creates a new parameter that can be any type. Parameter names
// must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewAnyParam(name string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamAny(name, ""),
	}
}

// Description adds an optional description to the parameter definition, this is
// used when generating documentation for the parameter to describe what the
// parameter is for.
func (d ParamDefinition) Description(str string) ParamDefinition {
	d.def.Description = str
	return d
}

// Optional marks the parameter as optional.
func (d ParamDefinition) Optional() ParamDefinition {
	d.def = d.def.Optional()
	return d
}

// Default adds a default value to a parameter, also making it implicitly
// optional.
func (d ParamDefinition) Default(v any) ParamDefinition {
	d.def = d.def.Default(v)
	return d
}

//------------------------------------------------------------------------------

// PluginSpec documents and defines the parameters of a function or method and
// the way in which it should be used.
//
// Using a plugin spec with explicit parameters means that instantiations of the
// plugin can be done using either classic argument types (foo, bar, baz),
// following the order in which the parameters are added, or named style
// (c: baz, a: foo).
type PluginSpec struct {
	status      query.Status
	category    string
	description string
	impure      bool
	static      bool
	params      query.Params
	examples    []pluginExample
	version     string
}

type pluginExample struct {
	summary      string
	mapping      string
	inputOutputs [][2]string
}

// NewPluginSpec creates a new plugin definition for a function or method
// plugin that describes the arguments that the plugin expects.
func NewPluginSpec() *PluginSpec {
	return &PluginSpec{
		params: query.NewParams(),
	}
}

// Experimental flags the plugin as an experimental component.
func (p *PluginSpec) Experimental() *PluginSpec {
	p.status = query.StatusExperimental
	return p
}

// Beta flags the plugin as a beta component.
func (p *PluginSpec) Beta() *PluginSpec {
	p.status = query.StatusBeta
	return p
}

// Deprecated flags the plugin as a deprecated component, it will still be valid
// in mappings but won't appear prominently in documentation.
func (p *PluginSpec) Deprecated() *PluginSpec {
	p.status = query.StatusDeprecated
	return p
}

// Category adds an optional category string to the plugin spec, this is used
// when generating documentation for the plugin.
func (p *PluginSpec) Category(str string) *PluginSpec {
	p.category = str
	return p
}

// Description adds an optional description to the plugin spec, this is used
// when generating documentation for the plugin.
func (p *PluginSpec) Description(str string) *PluginSpec {
	p.description = str
	return p
}

// Version specifies that this plugin was introduced in a given version.
func (p *PluginSpec) Version(v string) *PluginSpec {
	p.version = v
	return p
}

// Example adds an optional example to the plugin spec, this is used when
// generating documentation for the plugin. An example consists of a short
// summary, a mapping demonstrating the plugin, and one or more input/output
// combinations.
func (p *PluginSpec) Example(summary, mapping string, inputOutputs ...[2]string) *PluginSpec {
	p.examples = append(p.examples, pluginExample{
		summary:      summary,
		mapping:      mapping,
		inputOutputs: inputOutputs,
	})
	return p
}

// Variadic marks this plugin as having variadic parameters, which means any
// number of arguments can be provided and they are unnamed. It is invalid to
// combine variadic with named parameters.
//
// A variadic method is able to extract arguments from a *ParsedParams object
// via the AsSlice method.
func (p *PluginSpec) Variadic() *PluginSpec {
	p.params.Variadic = true
	return p
}

// Param adds a parameter to the spec. Instantiations of the plugin with
// nameless arguments (foo, bar, baz) must follow the order in which fields are
// added to the spec.
func (p *PluginSpec) Param(def ParamDefinition) *PluginSpec {
	p.params = p.params.Add(def.def)
	return p
}

// Impure marks the plugin as "impure", meaning it either reads from or
// interacts with state outside of the boundaries of a single mapping
// invocation. This usually means reading state from the machine. Impure plugins
// are excluded from some bloblang environments.
func (p *PluginSpec) Impure() *PluginSpec {
	p.impure = true
	return p
}

// Static marks the plugin as a statically evaluated function or method. This is
// a guarantee that given the name parameters this plugin will always yield the
// same value.
//
// Marking a function or method as static has the advantage that it can
// sometimes be optimistically evaluated at mapping parse time when given static
// arguments.
func (p *PluginSpec) Static() *PluginSpec {
	p.static = true
	return p
}

// EncodeJSON attempts to parse a JSON object as a byte slice and uses it to
// populate the configuration spec. The schema of this method is undocumented
// and is not intended for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func (p *PluginSpec) EncodeJSON(v []byte) error {
	def := struct {
		Description string       `json:"description"`
		Params      query.Params `json:"params"`
	}{}
	if err := json.Unmarshal(v, &def); err != nil {
		return err
	}
	p.description = def.Description
	p.params = def.Params
	return nil
}

//------------------------------------------------------------------------------

// ParsedParams is a reference to the arguments of a method or function
// instantiation.
type ParsedParams struct {
	par *query.ParsedParams
}

// AsSlice returns a slice of raw argument values.
func (p *ParsedParams) AsSlice() []any {
	return p.par.Raw()
}

// Get an argument value with a given name and return it boxed within an empty
// interface.
func (p *ParsedParams) Get(name string) (any, error) {
	return p.par.Field(name)
}

// GetString returns a string argument value with a given name.
func (p *ParsedParams) GetString(name string) (string, error) {
	return p.par.FieldString(name)
}

// GetOptionalString returns a string argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) GetOptionalString(name string) (*string, error) {
	return p.par.FieldOptionalString(name)
}

// GetInt64 returns an integer argument value with a given name.
func (p *ParsedParams) GetInt64(name string) (int64, error) {
	return p.par.FieldInt64(name)
}

// GetOptionalInt64 returns an int argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) GetOptionalInt64(name string) (*int64, error) {
	return p.par.FieldOptionalInt64(name)
}

// GetFloat64 returns a float argument value with a given name.
func (p *ParsedParams) GetFloat64(name string) (float64, error) {
	return p.par.FieldFloat(name)
}

// GetOptionalFloat64 returns a float argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) GetOptionalFloat64(name string) (*float64, error) {
	return p.par.FieldOptionalFloat(name)
}

// GetBool returns a bool argument value with a given name.
func (p *ParsedParams) GetBool(name string) (bool, error) {
	return p.par.FieldBool(name)
}

// GetOptionalBool returns a bool argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) GetOptionalBool(name string) (*bool, error) {
	return p.par.FieldOptionalBool(name)
}
