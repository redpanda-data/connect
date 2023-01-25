package bloblang

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// Environment provides an isolated Bloblang environment where the available
// features, functions and methods can be modified.
type Environment struct {
	env *bloblang.Environment
}

// GlobalEnvironment returns the global default environment. Modifying this
// environment will impact all Bloblang parses that aren't initialized with an
// isolated environment, as well as any new environments initialized after the
// changes.
func GlobalEnvironment() *Environment {
	return &Environment{
		env: bloblang.GlobalEnvironment(),
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
	return GlobalEnvironment().Clone()
}

// NewEmptyEnvironment creates a fresh Bloblang environment starting completely
// empty, where no functions or methods are initially available.
func NewEmptyEnvironment() *Environment {
	return &Environment{
		env: bloblang.NewEmptyEnvironment(),
	}
}

// Clone an environment in order to register functions and methods without
// modifying the existing environment.
func (e *Environment) Clone() *Environment {
	return e.WithoutFunctions().WithoutMethods()
}

// Parse a Bloblang mapping using the Environment to determine the features
// (functions and methods) available to the mapping.
//
// When a parsing error occurs the error will be the type *ParseError, which
// gives access to the line and column where the error occurred, as well as a
// method for creating a well formatted error message.
func (e *Environment) Parse(blobl string) (*Executor, error) {
	exec, err := e.env.NewMapping(blobl)
	if err != nil {
		if pErr, ok := err.(*parser.Error); ok {
			return nil, internalToPublicParserError([]rune(blobl), pErr)
		}
		return nil, err
	}
	return newExecutor(exec), nil
}

// CheckInterpolatedString attempts to parse a Bloblang interpolated string
// using the Environment to determine the features (functions and methods)
// available to it.
//
// When a parsing error occurs the error will be the type *ParseError, which
// gives access to the line and column where the error occurred, as well as a
// method for creating a well formatted error message.
func (e *Environment) CheckInterpolatedString(str string) error {
	_, err := e.env.NewField(str)
	if err != nil {
		if pErr, ok := err.(*parser.Error); ok {
			return internalToPublicParserError([]rune(str), pErr)
		}
		return err
	}
	return nil
}

// RegisterMethod adds a new Bloblang method to the environment. All method
// names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake
// case).
func (e *Environment) RegisterMethod(name string, ctor MethodConstructor) error {
	spec := query.NewMethodSpec(name, "").InCategory(query.MethodCategoryPlugin, "")
	spec.Params = query.VariadicParams()
	return e.env.RegisterMethod(spec, func(target query.Function, args *query.ParsedParams) (query.Function, error) {
		fn, err := ctor(args.Raw()...)
		if err != nil {
			return nil, err
		}
		return query.ClosureFunction("method "+name, func(ctx query.FunctionContext) (any, error) {
			v, err := target.Exec(ctx)
			if err != nil {
				return nil, err
			}
			return fn(v)
		}, target.QueryTargets), nil
	})
}

// RegisterMethodV2 adds a new Bloblang method to the environment using a
// provided ParamsSpec to define the name of the method and its parameters.
//
// Plugin names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func (e *Environment) RegisterMethodV2(name string, spec *PluginSpec, ctor MethodConstructorV2) error {
	category := spec.category
	if category == "" {
		category = query.MethodCategoryPlugin
	}
	// Deprecated overrides all others
	if spec.status == query.StatusDeprecated {
		category = query.MethodCategoryDeprecated
	}
	var examples []query.ExampleSpec
	for _, e := range spec.examples {
		var res []string
		for _, inputOutput := range e.inputOutputs {
			res = append(res, inputOutput[0], inputOutput[1])
		}
		examples = append(examples, query.NewExampleSpec(e.summary, e.mapping, res...))
	}
	iSpec := query.NewMethodSpec(name, spec.description).InCategory(category, "", examples...).AtVersion(spec.version)
	if spec.status != "" {
		iSpec.Status = spec.status
	}
	if spec.impure {
		iSpec = iSpec.MarkImpure()
	}
	iSpec.Params = spec.params
	return e.env.RegisterMethod(iSpec, func(target query.Function, args *query.ParsedParams) (query.Function, error) {
		fn, err := ctor(&ParsedParams{par: args})
		if err != nil {
			return nil, err
		}
		if spec.static {
			if sTarget, isLiteral := target.(*query.Literal); isLiteral {
				v, err := fn(sTarget.Value)
				if err != nil {
					return nil, err
				}
				return query.NewLiteralFunction("method "+name, v), nil
			}
		}
		return query.ClosureFunction("method "+name, func(ctx query.FunctionContext) (any, error) {
			v, err := target.Exec(ctx)
			if err != nil {
				return nil, err
			}
			return fn(v)
		}, target.QueryTargets), nil
	})
}

// RegisterFunction adds a new Bloblang function to the environment. All
// function names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func (e *Environment) RegisterFunction(name string, ctor FunctionConstructor) error {
	spec := query.NewFunctionSpec(query.FunctionCategoryPlugin, name, "")
	spec.Params = query.VariadicParams()
	return e.env.RegisterFunction(spec, func(args *query.ParsedParams) (query.Function, error) {
		fn, err := ctor(args.Raw()...)
		if err != nil {
			return nil, err
		}
		return query.ClosureFunction("function "+name, func(ctx query.FunctionContext) (any, error) {
			return fn()
		}, nil), nil
	})
}

// RegisterFunctionV2 adds a new Bloblang function to the environment using a
// provided ParamsSpec to define the name of the function and its parameters.
//
// Plugin names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func (e *Environment) RegisterFunctionV2(name string, spec *PluginSpec, ctor FunctionConstructorV2) error {
	category := spec.category
	if category == "" {
		category = query.FunctionCategoryPlugin
	}
	// Deprecated overrides all others
	if spec.status == query.StatusDeprecated {
		category = query.FunctionCategoryDeprecated
	}
	var examples []query.ExampleSpec
	for _, e := range spec.examples {
		var res []string
		for _, inputOutput := range e.inputOutputs {
			res = append(res, inputOutput[0], inputOutput[1])
		}
		examples = append(examples, query.NewExampleSpec(e.summary, e.mapping, res...))
	}
	iSpec := query.NewFunctionSpec(category, name, spec.description, examples...).AtVersion(spec.version)
	if spec.status != "" {
		iSpec.Status = spec.status
	}
	if spec.impure {
		iSpec = iSpec.MarkImpure()
	}
	iSpec.Params = spec.params
	return e.env.RegisterFunction(iSpec, func(args *query.ParsedParams) (query.Function, error) {
		fn, err := ctor(&ParsedParams{par: args})
		if err != nil {
			return nil, err
		}
		if spec.static {
			v, err := fn()
			if err != nil {
				return nil, err
			}
			return query.NewLiteralFunction("function "+name, v), nil
		}
		return query.ClosureFunction("function "+name, func(ctx query.FunctionContext) (any, error) {
			return fn()
		}, nil), nil
	})
}

// WithoutMethods returns a copy of the environment but with a variadic list of
// method names removed. Instantiation of these removed methods within a mapping
// will cause errors at parse time.
func (e *Environment) WithoutMethods(names ...string) *Environment {
	return &Environment{
		env: e.env.WithoutMethods(names...),
	}
}

// WithoutFunctions returns a copy of the environment but with a variadic list
// of function names removed. Instantiation of these removed functions within a
// mapping will cause errors at parse time.
func (e *Environment) WithoutFunctions(names ...string) *Environment {
	return &Environment{
		env: e.env.WithoutFunctions(names...),
	}
}

// WithDisabledImports returns a copy of the environment where imports within
// mappings are disabled.
func (e *Environment) WithDisabledImports() *Environment {
	return &Environment{
		env: e.env.WithDisabledImports(),
	}
}

// WithCustomImporter returns a copy of the environment where imports from
// mappings are done via a provided closure function.
func (e *Environment) WithCustomImporter(fn func(name string) ([]byte, error)) *Environment {
	return &Environment{
		env: e.env.WithCustomImporter(fn),
	}
}

// WithMaxMapRecursion returns a copy of the environment where the maximum
// recursion allowed for maps is set to a given value. If the execution of a
// mapping from this environment matches this number of recursive map calls the
// mapping will error out.
func (e *Environment) WithMaxMapRecursion(n int) *Environment {
	return &Environment{
		env: e.env.WithMaxMapRecursion(n),
	}
}

// OnlyPure removes any methods and functions that have been registered but are
// marked as impure. Impure in this context means the method/function is able to
// mutate global state or access machine state (read environment variables,
// files, etc). Note that methods/functions that access the machine clock are
// not marked as pure, so timestamp functions will still work.
func (e *Environment) OnlyPure() *Environment {
	return &Environment{
		env: e.env.OnlyPure(),
	}
}

// Deactivated returns a version of the environment where constructors are
// disabled for all functions and methods, allowing mappings to be parsed and
// validated but not executed.
//
// The underlying register of functions and methods is shared with the target
// environment, and therefore functions/methods registered to this set will also
// be added to the still activated environment. Use Clone in order to avoid
// this.
func (e *Environment) Deactivated() *Environment {
	return &Environment{
		env: e.env.Deactivated(),
	}
}

//------------------------------------------------------------------------------

// Parse a Bloblang mapping allowing the use of the globally accessible range of
// features (functions and methods).
//
// When a parsing error occurs the error will be the type *ParseError, which
// gives access to the line and column where the error occurred, as well as a
// method for creating a well formatted error message.
func Parse(blobl string) (*Executor, error) {
	exec, err := parser.ParseMapping(parser.GlobalContext(), blobl)
	if err != nil {
		return nil, internalToPublicParserError([]rune(blobl), err)
	}
	return newExecutor(exec), nil
}

// RegisterMethod adds a new Bloblang method to the global environment. All
// method names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func RegisterMethod(name string, ctor MethodConstructor) error {
	return GlobalEnvironment().RegisterMethod(name, ctor)
}

// RegisterMethodV2 adds a new Bloblang method to the global environment. All
// method names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func RegisterMethodV2(name string, spec *PluginSpec, ctor MethodConstructorV2) error {
	return GlobalEnvironment().RegisterMethodV2(name, spec, ctor)
}

// RegisterFunction adds a new Bloblang function to the global environment. All
// function names must match the regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/
// (snake case).
func RegisterFunction(name string, ctor FunctionConstructor) error {
	return GlobalEnvironment().RegisterFunction(name, ctor)
}

// RegisterFunctionV2 adds a new Bloblang function to the global environment.
// All function names must match the regular expression
// /^[a-z0-9]+(_[a-z0-9]+)*$/ (snake case).
func RegisterFunctionV2(name string, spec *PluginSpec, ctor FunctionConstructorV2) error {
	return GlobalEnvironment().RegisterFunctionV2(name, spec, ctor)
}

// WalkFunctions executes a provided function argument for every function that
// has been registered to the environment.
func (e *Environment) WalkFunctions(fn func(name string, spec *FunctionView)) {
	e.env.WalkFunctions(func(name string, spec query.FunctionSpec) {
		v := &FunctionView{spec: spec}
		fn(name, v)
	})
}

// WalkMethods executes a provided function argument for every method that has
// been registered to the environment.
func (e *Environment) WalkMethods(fn func(name string, spec *MethodView)) {
	e.env.WalkMethods(func(name string, spec query.MethodSpec) {
		v := &MethodView{spec: spec}
		fn(name, v)
	})
}
