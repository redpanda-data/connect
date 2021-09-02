package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// Environment provides an isolated Bloblang environment where the available
// features, functions and methods can be modified.
type Environment struct {
	functions *query.FunctionSet
	methods   *query.MethodSet
}

// GlobalEnvironment returns the global default environment. Modifying this
// environment will impact all Bloblang parses that aren't initialized with an
// isolated environment, as well as any new environments initialized after the
// changes.
func GlobalEnvironment() *Environment {
	return &Environment{
		functions: query.AllFunctions,
		methods:   query.AllMethods,
	}
}

// NewEnvironment creates a fresh Bloblang environment, starting with the full
// range of globally defined features (functions and methods), and provides APIs
// for expanding or contracting the features available to this environment.
//
// It's worth using an environment when you need to restrict the access or
// capabilities that certain bloblang mappings have versus others.
//
// For example, an environment could be created that removes any functions for
// accessing environment variables or reading data from the host disk, which
// could be used in certain situations without removing those functions globally
// for all mappings.
func NewEnvironment() *Environment {
	return GlobalEnvironment().WithoutFunctions().WithoutMethods()
}

// NewEmptyEnvironment creates a fresh Bloblang environment starting completely
// empty, where no functions or methods are initially available.
func NewEmptyEnvironment() *Environment {
	return &Environment{
		functions: query.NewFunctionSet(),
		methods:   query.NewMethodSet(),
	}
}

// NewField attempts to parse and create a dynamic field expression from a
// string. If the expression is invalid an error is returned.
//
// When a parsing error occurs the returned error will be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func (e *Environment) NewField(expr string) (*field.Expression, error) {
	pCtx := parser.GlobalContext()
	if e != nil {
		pCtx.Functions = e.functions
		pCtx.Methods = e.methods
	}
	f, err := parser.ParseField(pCtx, expr)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// NewMapping parses a Bloblang mapping using the Environment to determine the
// features (functions and methods) available to the mapping.
//
// When a parsing error occurs the error will be the type *parser.Error, which
// gives access to the line and column where the error occurred, as well as a
// method for creating a well formatted error message.
func (e *Environment) NewMapping(path, blobl string) (*mapping.Executor, error) {
	pCtx := parser.GlobalContext()
	if e != nil {
		pCtx.Functions = e.functions
		pCtx.Methods = e.methods
	}
	exec, err := parser.ParseMapping(pCtx, path, blobl)
	if err != nil {
		return nil, err
	}
	return exec, nil
}

// RegisterMethod adds a new Bloblang method to the environment.
func (e *Environment) RegisterMethod(spec query.MethodSpec, ctor query.MethodCtor) error {
	return e.methods.Add(spec, ctor)
}

// RegisterFunction adds a new Bloblang function to the environment.
func (e *Environment) RegisterFunction(spec query.FunctionSpec, ctor query.FunctionCtor) error {
	return e.functions.Add(spec, ctor)
}

// WithoutMethods returns a copy of the environment but with a variadic list of
// method names removed. Instantiation of these removed methods within a mapping
// will cause errors at parse time.
func (e *Environment) WithoutMethods(names ...string) *Environment {
	return &Environment{
		functions: e.functions,
		methods:   e.methods.Without(names...),
	}
}

// WithoutFunctions returns a copy of the environment but with a variadic list
// of function names removed. Instantiation of these removed functions within a
// mapping will cause errors at parse time.
func (e *Environment) WithoutFunctions(names ...string) *Environment {
	return &Environment{
		functions: e.functions.Without(names...),
		methods:   e.methods,
	}
}
