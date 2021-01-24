package query

import (
	"fmt"
	"sort"
)

// FunctionSet contains an explicit set of functions to be available in a
// Bloblang query.
type FunctionSet struct {
	constructors map[string]FunctionCtor
	specs        []FunctionSpec
}

// Add a new function to this set by providing a spec (name and documentation),
// a constructor to be called for each instantiation of the function, and
// information regarding the arguments of the function.
func (f *FunctionSet) Add(spec FunctionSpec, allowDynamicArgs bool, ctor FunctionCtor, checks ...ArgCheckFn) error {
	if len(checks) > 0 {
		ctor = checkArgs(ctor, checks...)
	}
	if allowDynamicArgs {
		ctor = enableDynamicArgs(ctor)
	}
	if _, exists := f.constructors[spec.Name]; exists {
		return fmt.Errorf("conflicting function name: %v", spec.Name)
	}
	f.constructors[spec.Name] = ctor
	f.specs = append(f.specs, spec)
	return nil
}

// Docs returns a slice of function specs, which document each function.
func (f *FunctionSet) Docs() []FunctionSpec {
	return f.specs
}

// List returns a slice of function names in alphabetical order.
func (f *FunctionSet) List() []string {
	functionNames := make([]string, 0, len(f.constructors))
	for k := range f.constructors {
		functionNames = append(functionNames, k)
	}
	sort.Strings(functionNames)
	return functionNames
}

// Init attempts to initialize a function of the set by name and zero or more
// arguments.
func (f *FunctionSet) Init(name string, args ...interface{}) (Function, error) {
	ctor, exists := f.constructors[name]
	if !exists {
		return nil, badFunctionErr(name)
	}
	expandLiteralArgs(args)
	return ctor(args...)
}

//------------------------------------------------------------------------------

// AllFunctions is a set containing every single function declared by this
// package, and any globally declared plugin methods.
var AllFunctions = &FunctionSet{
	constructors: map[string]FunctionCtor{},
	specs:        []FunctionSpec{},
}

// RegisterFunction to be accessible from Bloblang queries. Returns an empty
// struct in order to allow inline calls.
func RegisterFunction(spec FunctionSpec, allowDynamicArgs bool, ctor FunctionCtor, checks ...ArgCheckFn) struct{} {
	if err := AllFunctions.Add(spec, allowDynamicArgs, ctor, checks...); err != nil {
		panic(err)
	}
	return struct{}{}
}

// InitFunction attempts to initialise a function by its name and arguments.
func InitFunction(name string, args ...interface{}) (Function, error) {
	return AllFunctions.Init(name, args...)
}

// FunctionDocs returns a slice of specs, one for each function.
func FunctionDocs() []FunctionSpec {
	return AllFunctions.Docs()
}

// ListFunctions returns a slice of function names, sorted alphabetically.
func ListFunctions() []string {
	return AllFunctions.List()
}
