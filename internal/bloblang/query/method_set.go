package query

import (
	"errors"
	"fmt"
	"sort"
)

type methodDetails struct {
	ctor MethodCtor
	spec MethodSpec
}

// MethodSet contains an explicit set of methods to be available in a Bloblang
// query.
type MethodSet struct {
	disableCtors bool
	methods      map[string]methodDetails
}

// NewMethodSet creates a method set without any methods in it.
func NewMethodSet() *MethodSet {
	return &MethodSet{
		methods: map[string]methodDetails{},
	}
}

// Add a new method to this set by providing a spec (name and documentation),
// a constructor to be called for each instantiation of the method, and
// information regarding the arguments of the method.
func (m *MethodSet) Add(spec MethodSpec, ctor MethodCtor) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("method name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if err := spec.Params.validate(); err != nil {
		return err
	}
	m.methods[spec.Name] = methodDetails{ctor: ctor, spec: spec}
	return nil
}

// Docs returns a slice of method specs, which document each method.
func (m *MethodSet) Docs() []MethodSpec {
	specSlice := make([]MethodSpec, 0, len(m.methods))
	for _, v := range m.methods {
		specSlice = append(specSlice, v.spec)
	}
	sort.Slice(specSlice, func(i, j int) bool {
		return specSlice[i].Name < specSlice[j].Name
	})
	return specSlice
}

// Params attempts to obtain an argument specification for a given method type.
func (m *MethodSet) Params(name string) (Params, error) {
	details, exists := m.methods[name]
	if !exists {
		return VariadicParams(), badMethodErr(name)
	}
	return details.spec.Params, nil
}

// Init attempts to initialize a method of the set by name from a target
// function and zero or more arguments.
func (m *MethodSet) Init(name string, target Function, args *ParsedParams) (Function, error) {
	details, exists := m.methods[name]
	if !exists {
		return nil, badMethodErr(name)
	}
	if m.disableCtors {
		return disabledMethod(name), nil
	}
	return wrapMethodCtorWithDynamicArgs(name, target, args, details.ctor)
}

// Without creates a clone of the method set that can be mutated in isolation,
// where a variadic list of methods will be excluded from the set.
func (m *MethodSet) Without(methods ...string) *MethodSet {
	excludeMap := make(map[string]struct{}, len(methods))
	for _, k := range methods {
		excludeMap[k] = struct{}{}
	}

	details := make(map[string]methodDetails, len(m.methods))
	for k, v := range m.methods {
		if _, exists := excludeMap[k]; !exists {
			details[k] = v
		}
	}
	return &MethodSet{disableCtors: m.disableCtors, methods: details}
}

// OnlyPure creates a clone of the methods set that can be mutated in isolation,
// where all impure methods are removed.
func (m *MethodSet) OnlyPure() *MethodSet {
	var excludes []string
	for _, v := range m.methods {
		if v.spec.Impure {
			excludes = append(excludes, v.spec.Name)
		}
	}
	return m.Without(excludes...)
}

// Deactivated returns a version of the method set where constructors are
// disabled, allowing mappings to be parsed and validated but not executed.
//
// The underlying register of methods is shared with the target set, and
// therefore methods added to this set will also be added to the still activated
// set. Use the Without method (with empty args if applicable) in order to
// create a deep copy of the set that is independent of the source.
func (m *MethodSet) Deactivated() *MethodSet {
	newSet := *m
	newSet.disableCtors = true
	return &newSet
}

//------------------------------------------------------------------------------

// AllMethods is a set containing every single method declared by this package,
// and any globally declared plugin methods.
var AllMethods = NewMethodSet()

func registerMethod(spec MethodSpec, ctor MethodCtor) struct{} {
	if err := AllMethods.Add(spec, func(target Function, args *ParsedParams) (Function, error) {
		return ctor(target, args)
	}); err != nil {
		panic(err)
	}
	return struct{}{}
}

// InitMethodHelper attempts to initialise a method by its name, target function
// and arguments, this is convenient for writing tests.
func InitMethodHelper(name string, target Function, args ...any) (Function, error) {
	details, ok := AllMethods.methods[name]
	if !ok {
		return nil, badMethodErr(name)
	}
	parsedArgs, err := details.spec.Params.PopulateNameless(args...)
	if err != nil {
		return nil, err
	}
	return AllMethods.Init(name, target, parsedArgs)
}

// MethodDocs returns a slice of specs, one for each method.
func MethodDocs() []MethodSpec {
	return AllMethods.Docs()
}

//------------------------------------------------------------------------------

func disabledMethod(name string) Function {
	return ClosureFunction("method "+name, func(ctx FunctionContext) (any, error) {
		return nil, errors.New("this method has been disabled")
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil })
}

func wrapMethodCtorWithDynamicArgs(name string, target Function, args *ParsedParams, fn MethodCtor) (Function, error) {
	fns := args.dynamic()
	if len(fns) == 0 {
		return fn(target, args)
	}
	return ClosureFunction("method "+name, func(ctx FunctionContext) (any, error) {
		newArgs, err := args.ResolveDynamic(ctx)
		if err != nil {
			return nil, fmt.Errorf("method '%s': %w", name, err)
		}
		dynFunc, err := fn(target, newArgs)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	}, aggregateTargetPaths(fns...)), nil
}
