package query

import (
	"encoding/json"
	"fmt"
)

// MethodCtor constructs a new method from a target function and input args.
type MethodCtor func(target Function, args *ParsedParams) (Function, error)

type simpleMethodConstructor func(args *ParsedParams) (simpleMethod, error)

func registerSimpleMethod(spec MethodSpec, ctor simpleMethodConstructor) struct{} {
	return registerMethod(spec, func(target Function, args *ParsedParams) (Function, error) {
		fn, err := ctor(args)
		if err != nil {
			return nil, err
		}
		return ClosureFunction("method "+spec.Name, func(ctx FunctionContext) (any, error) {
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
	})
}

type simpleMethod func(v any, ctx FunctionContext) (any, error)

func stringMethod(fn func(v string) (any, error)) simpleMethod {
	return func(v any, ctx FunctionContext) (any, error) {
		s, err := IGetString(v)
		if err != nil {
			return nil, err
		}
		return fn(s)
	}
}

func numberMethod(fn func(f *float64, i *int64, ui *uint64) (any, error)) simpleMethod {
	return func(v any, ctx FunctionContext) (any, error) {
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
		return fn(f, i, ui)
	}
}
