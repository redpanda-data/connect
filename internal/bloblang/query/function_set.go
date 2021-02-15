package query

import (
	"fmt"
	"regexp"
	"sort"
)

// FunctionSet contains an explicit set of functions to be available in a
// Bloblang query.
type FunctionSet struct {
	constructors map[string]FunctionCtor
	specs        []FunctionSpec
}

var nameRegexpRaw = `^[a-z0-9]+(_[a-z0-9]+)*$`
var nameRegexp = regexp.MustCompile(nameRegexpRaw)

// Add a new function to this set by providing a spec (name and documentation),
// a constructor to be called for each instantiation of the function, and
// information regarding the arguments of the function.
func (f *FunctionSet) Add(spec FunctionSpec, ctor FunctionCtor, autoResolveFunctionArgs bool, checks ...ArgCheckFn) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("function name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if len(checks) > 0 {
		ctor = checkArgs(ctor, checks...)
	}
	if autoResolveFunctionArgs {
		ctor = functionWithAutoResolvedFunctionArgs(ctor)
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

// Without creates a clone of the function set that can be mutated in isolation,
// where a variadic list of functions will be excluded from the set.
func (f *FunctionSet) Without(functions ...string) *FunctionSet {
	excludeMap := make(map[string]struct{}, len(functions))
	for _, k := range functions {
		excludeMap[k] = struct{}{}
	}

	constructors := make(map[string]FunctionCtor, len(f.constructors))
	for k, v := range f.constructors {
		if _, exists := excludeMap[k]; !exists {
			constructors[k] = v
		}
	}

	specs := make([]FunctionSpec, 0, len(f.specs))
	for _, v := range f.specs {
		if _, exists := excludeMap[v.Name]; !exists {
			specs = append(specs, v)
		}
	}
	return &FunctionSet{constructors, specs}
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
func RegisterFunction(spec FunctionSpec, autoResolveFunctionArgs bool, ctor FunctionCtor, checks ...ArgCheckFn) struct{} {
	if err := AllFunctions.Add(spec, ctor, autoResolveFunctionArgs, checks...); err != nil {
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
