package query

import (
	"fmt"

	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

func methodWithDynamicArgs(args []interface{}, target Function, ctor MethodCtor) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		dynArgs := make([]interface{}, 0, len(args))
		for i, dArg := range args {
			if fArg, isDyn := dArg.(Function); isDyn {
				res, err := fArg.Exec(ctx)
				if err != nil {
					return nil, xerrors.Errorf("failed to extract input arg %v: %w", i, err)
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
	})
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

//------------------------------------------------------------------------------

// MethodCtor constructs a new method from a target function and input args.
type MethodCtor func(target Function, args ...interface{}) (Function, error)

// RegisterMethod to be accessible from Bloblang queries. Returns an empty
// struct in order to allow inline calls.
func RegisterMethod(name string, allowDynamicArgs bool, ctor MethodCtor, checks ...ArgCheckFn) struct{} {
	if len(checks) > 0 {
		ctor = checkMethodArgs(ctor, checks...)
	}
	if allowDynamicArgs {
		ctor = enableMethodDynamicArgs(ctor)
	}
	if _, exists := methods[name]; exists {
		panic(fmt.Sprintf("Conflicting method name: %v", name))
	}
	methods[name] = ctor
	return struct{}{}
}

var methods = map[string]MethodCtor{}

//------------------------------------------------------------------------------
