package query

import (
	"fmt"
	"sort"
)

//------------------------------------------------------------------------------

func methodWithDynamicArgs(args []interface{}, target Function, ctor MethodCtor) Function {
	fns := []Function{target}
	for _, dArg := range args {
		if fArg, isDyn := dArg.(Function); isDyn {
			fns = append(fns, fArg)
		}
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		dynArgs := make([]interface{}, 0, len(args))
		for i, dArg := range args {
			if fArg, isDyn := dArg.(Function); isDyn {
				res, err := fArg.Exec(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to extract input arg %v: %w", i, err)
				}
				dynArgs = append(dynArgs, res)
			} else {
				dynArgs = append(dynArgs, dArg)
			}
		}
		dynFunc, err := ctor(target, dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)

	}, aggregateTargetPaths(fns...))
}

func enableMethodDynamicArgs(fn MethodCtor) MethodCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for _, arg := range args {
			if _, isDyn := arg.(Function); isDyn {
				return methodWithDynamicArgs(args, target, fn), nil
			}
		}
		return fn(target, args...)
	}
}

func checkMethodArgs(fn MethodCtor, checks ...ArgCheckFn) MethodCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for _, check := range checks {
			if err := check(args); err != nil {
				return nil, err
			}
		}
		return fn(target, args...)
	}
}

func simpleMethod(
	target Function,
	fn func(interface{}, FunctionContext) (interface{}, error),
) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		return fn(v, ctx)
	}, target.QueryTargets)
}

//------------------------------------------------------------------------------

// MethodCtor constructs a new method from a target function and input args.
type MethodCtor func(target Function, args ...interface{}) (Function, error)

// RegisterMethod to be accessible from Bloblang queries. Returns an empty
// struct in order to allow inline calls.
func RegisterMethod(spec MethodSpec, allowDynamicArgs bool, ctor MethodCtor, checks ...ArgCheckFn) struct{} {
	if len(checks) > 0 {
		ctor = checkMethodArgs(ctor, checks...)
	}
	if allowDynamicArgs {
		ctor = enableMethodDynamicArgs(ctor)
	}
	if _, exists := methods[spec.Name]; exists {
		panic(fmt.Sprintf("Conflicting method name: %v", spec.Name))
	}
	methods[spec.Name] = ctor
	methodSpecs = append(methodSpecs, spec)
	return struct{}{}
}

// InitMethod attempts to initialise a method by its name, target function and
// arguments.
func InitMethod(name string, target Function, args ...interface{}) (Function, error) {
	ctor, exists := methods[name]
	if !exists {
		return nil, badMethodErr(name)
	}
	return ctor(target, args...)
}

var methods = map[string]MethodCtor{}
var methodSpecs = []MethodSpec{}

// ListMethods returns a slice of method names, sorted alphabetically.
func ListMethods() []string {
	methodNames := make([]string, 0, len(methods))
	for k := range methods {
		methodNames = append(methodNames, k)
	}
	sort.Strings(methodNames)
	return methodNames
}

// MethodDocs returns a slice of specs, one for each method.
func MethodDocs() []MethodSpec {
	return methodSpecs
}

//------------------------------------------------------------------------------
