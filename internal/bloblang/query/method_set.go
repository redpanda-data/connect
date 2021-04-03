package query

import (
	"fmt"
	"sort"
)

// MethodSet contains an explicit set of methods to be available in a Bloblang
// query.
type MethodSet struct {
	constructors map[string]MethodCtor
	specs        []MethodSpec
}

// Add a new method to this set by providing a spec (name and documentation),
// a constructor to be called for each instantiation of the method, and
// information regarding the arguments of the method.
func (m *MethodSet) Add(spec MethodSpec, ctor MethodCtor, autoResolveFunctionArgs bool, checks ...ArgCheckFn) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("method name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if len(checks) > 0 {
		ctor = checkMethodArgs(ctor, checks...)
	}
	if autoResolveFunctionArgs {
		ctor = methodWithAutoResolvedFunctionArgs("method "+spec.Name, ctor)
	}
	if _, exists := m.constructors[spec.Name]; exists {
		return fmt.Errorf("conflicting method name: %v", spec.Name)
	}
	m.constructors[spec.Name] = ctor
	m.specs = append(m.specs, spec)
	return nil
}

// Docs returns a slice of method specs, which document each method.
func (m *MethodSet) Docs() []MethodSpec {
	return m.specs
}

// List returns a slice of method names in alphabetical order.
func (m *MethodSet) List() []string {
	methodNames := make([]string, 0, len(m.constructors))
	for k := range m.constructors {
		methodNames = append(methodNames, k)
	}
	sort.Strings(methodNames)
	return methodNames
}

// Init attempts to initialize a method of the set by name from a target
// function and zero or more arguments.
func (m *MethodSet) Init(name string, target Function, args ...interface{}) (Function, error) {
	ctor, exists := m.constructors[name]
	if !exists {
		return nil, badMethodErr(name)
	}
	expandLiteralArgs(args)
	return ctor(target, args...)
}

// Without creates a clone of the method set that can be mutated in isolation,
// where a variadic list of methods will be excluded from the set.
func (m *MethodSet) Without(methods ...string) *MethodSet {
	excludeMap := make(map[string]struct{}, len(methods))
	for _, k := range methods {
		excludeMap[k] = struct{}{}
	}

	constructors := make(map[string]MethodCtor, len(m.constructors))
	for k, v := range m.constructors {
		if _, exists := excludeMap[k]; !exists {
			constructors[k] = v
		}
	}

	specs := make([]MethodSpec, 0, len(m.specs))
	for _, v := range m.specs {
		if _, exists := excludeMap[v.Name]; !exists {
			specs = append(specs, v)
		}
	}
	return &MethodSet{constructors, specs}
}

//------------------------------------------------------------------------------

// AllMethods is a set containing every single method declared by this package,
// and any globally declared plugin methods.
var AllMethods = &MethodSet{
	constructors: map[string]MethodCtor{},
	specs:        []MethodSpec{},
}

// InitMethod attempts to initialise a method by its name, target function and
// arguments.
func InitMethod(name string, target Function, args ...interface{}) (Function, error) {
	return AllMethods.Init(name, target, args...)
}

// ListMethods returns a slice of method names, sorted alphabetically.
func ListMethods() []string {
	return AllMethods.List()
}

// MethodDocs returns a slice of specs, one for each method.
func MethodDocs() []MethodSpec {
	return AllMethods.Docs()
}
