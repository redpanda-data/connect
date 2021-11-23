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
	pCtx parser.Context
}

// GlobalEnvironment returns the global default environment. Modifying this
// environment will impact all Bloblang parses that aren't initialized with an
// isolated environment, as well as any new environments initialized after the
// changes.
func GlobalEnvironment() *Environment {
	return &Environment{
		pCtx: parser.GlobalContext(),
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
		pCtx: parser.EmptyContext(),
	}
}

// NewField attempts to parse and create a dynamic field expression from a
// string. If the expression is invalid an error is returned.
//
// When a parsing error occurs the returned error will be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func (e *Environment) NewField(expr string) (*field.Expression, error) {
	f, err := parser.ParseField(e.pCtx, expr)
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
func (e *Environment) NewMapping(blobl string) (*mapping.Executor, error) {
	exec, err := parser.ParseMapping(e.pCtx, blobl)
	if err != nil {
		return nil, err
	}
	return exec, nil
}

// Deactivated returns a version of the environment where constructors are
// disabled for all functions and methods, allowing mappings to be parsed and
// validated but not executed.
//
// The underlying register of functions and methods is shared with the target
// environment, and therefore functions/methods registered to this set will also
// be added to the still activated environment. Use the Without methods (with
// empty args if applicable) in order to create a deep copy of the environment
// that is independent of the source.
func (e *Environment) Deactivated() *Environment {
	return &Environment{
		pCtx: e.pCtx.Deactivated(),
	}
}

// OnlyPure removes any methods and functions that have been registered but are
// marked as impure. Impure in this context means the method/function is able to
// mutate global state or access machine state (read environment variables,
// files, etc). Note that methods/functions that access the machine clock are
// not marked as pure, so timestamp functions will still work.
func (e *Environment) OnlyPure() *Environment {
	newCtx := e.pCtx
	newCtx.Functions = newCtx.Functions.OnlyPure()
	newCtx.Methods = newCtx.Methods.OnlyPure()
	return &Environment{
		pCtx: newCtx,
	}
}

// RegisterMethod adds a new Bloblang method to the environment.
func (e *Environment) RegisterMethod(spec query.MethodSpec, ctor query.MethodCtor) error {
	return e.pCtx.Methods.Add(spec, ctor)
}

// RegisterFunction adds a new Bloblang function to the environment.
func (e *Environment) RegisterFunction(spec query.FunctionSpec, ctor query.FunctionCtor) error {
	return e.pCtx.Functions.Add(spec, ctor)
}

// WithImporter returns a new environment where Bloblang imports are performed
// from a new importer.
func (e *Environment) WithImporter(importer parser.Importer) *Environment {
	nextCtx := e.pCtx.WithImporter(importer)
	return &Environment{
		pCtx: nextCtx,
	}
}

// WithImporterRelativeToFile returns a new environment where any relative
// imports will be made from the directory of the provided file path. The
// provided path can itself be relative (to the current importer directory) or
// absolute.
func (e *Environment) WithImporterRelativeToFile(filePath string) *Environment {
	nextCtx := e.pCtx.WithImporterRelativeToFile(filePath)
	return &Environment{
		pCtx: nextCtx,
	}
}

// WithDisabledImports returns a version of the environment where imports within
// mappings are disabled entirely. This prevents mappings from accessing files
// from the host disk.
func (e *Environment) WithDisabledImports() *Environment {
	return &Environment{
		pCtx: e.pCtx.DisabledImports(),
	}
}

// WithCustomImporter returns a version of the environment where file imports
// are done exclusively through a provided closure function, which takes an
// import path (relative or absolute).
func (e *Environment) WithCustomImporter(fn func(name string) ([]byte, error)) *Environment {
	return &Environment{
		pCtx: e.pCtx.CustomImporter(fn),
	}
}

// WithoutMethods returns a copy of the environment but with a variadic list of
// method names removed. Instantiation of these removed methods within a mapping
// will cause errors at parse time.
func (e *Environment) WithoutMethods(names ...string) *Environment {
	nextCtx := e.pCtx
	nextCtx.Methods = e.pCtx.Methods.Without(names...)
	return &Environment{
		pCtx: nextCtx,
	}
}

// WithoutFunctions returns a copy of the environment but with a variadic list
// of function names removed. Instantiation of these removed functions within a
// mapping will cause errors at parse time.
func (e *Environment) WithoutFunctions(names ...string) *Environment {
	nextCtx := e.pCtx
	nextCtx.Functions = e.pCtx.Functions.Without(names...)
	return &Environment{
		pCtx: nextCtx,
	}
}

// WalkFunctions executes a provided function argument for every function that
// has been registered to the environment.
func (e *Environment) WalkFunctions(fn func(name string, spec query.FunctionSpec)) {
	for _, f := range e.pCtx.Functions.Docs() {
		fn(f.Name, f)
	}
}

// WalkMethods executes a provided function argument for every method that has
// been registered to the environment.
func (e *Environment) WalkMethods(fn func(name string, spec query.MethodSpec)) {
	for _, m := range e.pCtx.Methods.Docs() {
		fn(m.Name, m)
	}
}
