package query

import (
	"fmt"
)

// Deprecated: Use the new types and functions from params.go instead.
type methodOldCtor func(target Function, args ...interface{}) (Function, error)

// Deprecated: Use the new types and functions from params.go instead.
func methodOldParamsWithDynamicArgs(annotation string, args []interface{}, target Function, ctor methodOldCtor) Function {
	fns := []Function{target}
	for _, dArg := range args {
		if fArg, isDyn := dArg.(Function); isDyn {
			fns = append(fns, fArg)
		}
	}
	return ClosureFunction(annotation, func(ctx FunctionContext) (interface{}, error) {
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

// Deprecated: Use the new types and functions from params.go instead.
func methodOldParamsWithAutoResolvedFunctionArgs(annotation string, fn methodOldCtor) methodOldCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for i, arg := range args {
			switch t := arg.(type) {
			case *Literal:
				args[i] = t.Value
			case Function:
				return methodOldParamsWithDynamicArgs(annotation, args, target, fn), nil
			}
		}
		return fn(target, args...)
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func checkOldParamsMethodArgs(fn methodOldCtor, checks ...oldParamsArgCheckFn) methodOldCtor {
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

// Deprecated: Use the new types and functions from params.go instead.
func registerOldParamsMethod(spec MethodSpec, autoResolveFunctionArgs bool, ctor methodOldCtor, checks ...oldParamsArgCheckFn) struct{} {
	spec.Params = OldStyleParams()
	if len(checks) > 0 {
		ctor = checkOldParamsMethodArgs(ctor, checks...)
	}
	if autoResolveFunctionArgs {
		ctor = methodOldParamsWithAutoResolvedFunctionArgs("method "+spec.Name, ctor)
	}
	if err := AllMethods.Add(spec, func(target Function, args *ParsedParams) (Function, error) {
		return ctor(target, args.Raw()...)
	}); err != nil {
		panic(err)
	}
	return struct{}{}
}

// Deprecated: Use the new types and functions from params.go instead.
type simpleOldParamsMethodConstructor func(args ...interface{}) (simpleMethod, error)

// Deprecated: Use the new types and functions from params.go instead.
func registerOldParamsSimpleMethod(spec MethodSpec, ctor simpleOldParamsMethodConstructor, autoResolveFunctionArgs bool, checks ...oldParamsArgCheckFn) struct{} {
	return registerOldParamsMethod(spec, autoResolveFunctionArgs, func(target Function, args ...interface{}) (Function, error) {
		fn, err := ctor(args...)
		if err != nil {
			return nil, err
		}
		return ClosureFunction("method "+spec.Name, func(ctx FunctionContext) (interface{}, error) {
			v, err := target.Exec(ctx)
			if err != nil {
				return nil, err
			}
			res, err := fn(v, ctx)
			if err != nil {
				return nil, ErrFrom(err, target)
			}
			return res, nil
		}, target.QueryTargets), nil
	}, checks...)
}
