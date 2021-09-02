package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// ParamDefinition describes a single parameter for a function or method.
type ParamDefinition struct {
	def query.ParamDefinition
}

// ParamString creates a new string typed parameter. Parameter names must match
// the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func ParamString(name, description string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamString(name, description),
	}
}

// ParamInt64 creates a new 64-bit integer typed parameter. Parameter names must
// match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func ParamInt64(name, description string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamInt64(name, description),
	}
}

// ParamFloat64 creates a new float64 typed parameter. Parameter names must
// match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func ParamFloat64(name, description string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamFloat(name, description),
	}
}

// ParamBool creates a new bool typed parameter. Parameter names must match the
// regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func ParamBool(name, description string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamBool(name, description),
	}
}

// ParamAny creates a new parameter that can be any type. Parameter names must
// match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func ParamAny(name, description string) ParamDefinition {
	return ParamDefinition{
		def: query.ParamAny(name, description),
	}
}

// Optional marks the parameter as optional.
func (d ParamDefinition) Optional() ParamDefinition {
	d.def = d.def.Optional()
	return d
}

// Default adds a default value to a parameter, also making it implicitly
// optional.
func (d ParamDefinition) Default(v interface{}) ParamDefinition {
	d.def = d.def.Default(v)
	return d
}

//------------------------------------------------------------------------------

// ParamsSpec documents and defines the expected arguments of a function or
// method and it's behaviour.
//
// Using a params spec means that instantiations of the plugin can be done using
// either classic argument types (foo, bar, baz), following the order in which
// the parameters are added, or named style (c: baz, a: foo).
type ParamsSpec struct {
	name        string
	description string
	params      query.Params
}

// NewParamsSpec creates a new parameters definition for a function or method.
// Method and function names must match the regular expression
// /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func NewParamsSpec(name, description string) ParamsSpec {
	return ParamsSpec{
		name:        name,
		description: description,
		params:      query.NewParams(),
	}
}

// Add a parameter to the spec.
func (p ParamsSpec) Add(def ParamDefinition) ParamsSpec {
	p.params = p.params.Add(def.def)
	return p
}

//------------------------------------------------------------------------------

// ParsedParams is a reference to the arguments of a method or function
// instantiation.
type ParsedParams struct {
	par *query.ParsedParams
}

// Field returns an argument value with a given name.
func (p *ParsedParams) Field(name string) (interface{}, error) {
	return p.par.Field(name)
}

// FieldString returns a string argument value with a given name.
func (p *ParsedParams) FieldString(name string) (string, error) {
	return p.par.FieldString(name)
}

// FieldOptionalString returns a string argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) FieldOptionalString(name string) (*string, error) {
	return p.par.FieldOptionalString(name)
}

// FieldInt64 returns an integer argument value with a given name.
func (p *ParsedParams) FieldInt64(name string) (int64, error) {
	return p.par.FieldInt64(name)
}

// FieldOptionalInt64 returns an int argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) FieldOptionalInt64(name string) (*int64, error) {
	return p.par.FieldOptionalInt64(name)
}

// FieldFloat64 returns a float argument value with a given name.
func (p *ParsedParams) FieldFloat64(name string) (float64, error) {
	return p.par.FieldFloat(name)
}

// FieldOptionalFloat64 returns a float argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) FieldOptionalFloat64(name string) (*float64, error) {
	return p.par.FieldOptionalFloat(name)
}

// FieldBool returns a bool argument value with a given name.
func (p *ParsedParams) FieldBool(name string) (bool, error) {
	return p.par.FieldBool(name)
}

// FieldOptionalBool returns a bool argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) FieldOptionalBool(name string) (*bool, error) {
	return p.par.FieldOptionalBool(name)
}
