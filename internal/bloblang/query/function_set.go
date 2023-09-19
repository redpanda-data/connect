package query

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
)

type functionDetails struct {
	ctor FunctionCtor
	spec FunctionSpec
}

// FunctionSet contains an explicit set of functions to be available in a
// Bloblang query.
type FunctionSet struct {
	disableCtors bool
	functions    map[string]functionDetails
}

// NewFunctionSet creates a function set without any functions in it.
func NewFunctionSet() *FunctionSet {
	return &FunctionSet{
		functions: map[string]functionDetails{},
	}
}

var (
	nameRegexpRaw = `^[a-z0-9]+(_[a-z0-9]+)*$`
	nameRegexp    = regexp.MustCompile(nameRegexpRaw)
)

// Add a new function to this set by providing a spec (name and documentation),
// a constructor to be called for each instantiation of the function, and
// information regarding the arguments of the function.
func (f *FunctionSet) Add(spec FunctionSpec, ctor FunctionCtor) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("function name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if err := spec.Params.validate(); err != nil {
		return err
	}
	f.functions[spec.Name] = functionDetails{ctor: ctor, spec: spec}
	return nil
}

// Docs returns a slice of function specs, which document each function.
func (f *FunctionSet) Docs() []FunctionSpec {
	specSlice := make([]FunctionSpec, 0, len(f.functions))
	for _, v := range f.functions {
		specSlice = append(specSlice, v.spec)
	}
	sort.Slice(specSlice, func(i, j int) bool {
		return specSlice[i].Name < specSlice[j].Name
	})
	return specSlice
}

// Params attempts to obtain an argument specification for a given function.
func (f *FunctionSet) Params(name string) (Params, error) {
	details, exists := f.functions[name]
	if !exists {
		return VariadicParams(), badFunctionErr(name)
	}
	return details.spec.Params, nil
}

// Init attempts to initialize a function of the set by name and zero or more
// arguments.
func (f *FunctionSet) Init(name string, args *ParsedParams) (Function, error) {
	details, exists := f.functions[name]
	if !exists {
		return nil, badFunctionErr(name)
	}
	if f.disableCtors {
		return disabledFunction(name), nil
	}
	return wrapCtorWithDynamicArgs(name, args, details.ctor)
}

// Without creates a clone of the function set that can be mutated in isolation,
// where a variadic list of functions will be excluded from the set.
func (f *FunctionSet) Without(functions ...string) *FunctionSet {
	excludeMap := make(map[string]struct{}, len(functions))
	for _, k := range functions {
		excludeMap[k] = struct{}{}
	}

	details := make(map[string]functionDetails, len(f.functions))
	for k, v := range f.functions {
		if _, exists := excludeMap[k]; !exists {
			details[k] = v
		}
	}
	return &FunctionSet{disableCtors: f.disableCtors, functions: details}
}

// OnlyPure creates a clone of the function set that can be mutated in
// isolation, where all impure functions are removed.
func (f *FunctionSet) OnlyPure() *FunctionSet {
	var excludes []string
	for _, v := range f.functions {
		if v.spec.Impure {
			excludes = append(excludes, v.spec.Name)
		}
	}
	return f.Without(excludes...)
}

// NoMessage creates a clone of the function set that can be mutated in
// isolation, where all message access functions are removed.
func (f *FunctionSet) NoMessage() *FunctionSet {
	var excludes []string
	for _, v := range f.functions {
		if v.spec.Category == FunctionCategoryMessage {
			excludes = append(excludes, v.spec.Name)
		}
	}
	return f.Without(excludes...)
}

// Deactivated returns a version of the function set where constructors are
// disabled, allowing mappings to be parsed and validated but not executed.
//
// The underlying register of functions is shared with the target set, and
// therefore functions added to this set will also be added to the still
// activated set. Use the Without method (with empty args if applicable) in
// order to create a deep copy of the set that is independent of the source.
func (f *FunctionSet) Deactivated() *FunctionSet {
	newSet := *f
	newSet.disableCtors = true
	return &newSet
}

//------------------------------------------------------------------------------

// AllFunctions is a set containing every single function declared by this
// package, and any globally declared plugin methods.
var AllFunctions = NewFunctionSet()

func registerFunction(spec FunctionSpec, ctor FunctionCtor) struct{} {
	if err := AllFunctions.Add(spec, func(args *ParsedParams) (Function, error) {
		return ctor(args)
	}); err != nil {
		panic(err)
	}
	return struct{}{}
}

func registerSimpleFunction(spec FunctionSpec, fn func(ctx FunctionContext) (any, error)) struct{} {
	if err := AllFunctions.Add(spec, func(*ParsedParams) (Function, error) {
		return ClosureFunction("function "+spec.Name, fn, nil), nil
	}); err != nil {
		panic(err)
	}
	return struct{}{}
}

// InitFunctionHelper attempts to initialise a function by its name and a list
// of arguments, this is convenient for writing tests.
func InitFunctionHelper(name string, args ...any) (Function, error) {
	details, ok := AllFunctions.functions[name]
	if !ok {
		return nil, badFunctionErr(name)
	}
	parsedArgs, err := details.spec.Params.PopulateNameless(args...)
	if err != nil {
		return nil, err
	}
	return AllFunctions.Init(name, parsedArgs)
}

// FunctionDocs returns a slice of specs, one for each function.
func FunctionDocs() []FunctionSpec {
	return AllFunctions.Docs()
}

//------------------------------------------------------------------------------

func disabledFunction(name string) Function {
	return ClosureFunction("function "+name, func(ctx FunctionContext) (any, error) {
		return nil, errors.New("this function has been disabled")
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil })
}

func wrapCtorWithDynamicArgs(name string, args *ParsedParams, fn FunctionCtor) (Function, error) {
	fns := args.dynamic()
	if len(fns) == 0 {
		return fn(args)
	}
	return ClosureFunction("function "+name, func(ctx FunctionContext) (any, error) {
		newArgs, err := args.ResolveDynamic(ctx)
		if err != nil {
			return nil, fmt.Errorf("function '%s': %w", name, err)
		}
		dynFunc, err := fn(newArgs)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	}, aggregateTargetPaths(fns...)), nil
}
