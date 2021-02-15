package query

import (
	"encoding/json"
	"fmt"
)

// MethodCtor constructs a new method from a target function and input args.
type MethodCtor func(target Function, args ...interface{}) (Function, error)

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

func methodWithAutoResolvedFunctionArgs(fn MethodCtor) MethodCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for i, arg := range args {
			switch t := arg.(type) {
			case *Literal:
				args[i] = t.Value
			case Function:
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

func numberMethod(
	target Function,
	fn func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error),
) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var f *float64
		var i *int64
		var ui *uint64
		switch t := v.(type) {
		case float64:
			f = &t
		case int64:
			i = &t
		case uint64:
			ui = &t
		case json.Number:
			if ji, err := t.Int64(); err == nil {
				i = &ji
			} else if jf, err := t.Float64(); err == nil {
				f = &jf
			} else {
				return nil, fmt.Errorf("failed to parse number: %v", err)
			}
		default:
			return nil, NewTypeError(v, ValueNumber)
		}
		return fn(f, i, ui, ctx)
	}, target.QueryTargets)
}
