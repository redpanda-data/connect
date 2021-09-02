package query

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Deprecated: Use the new types and functions from params.go instead.
type oldParamsFunctionCtor func(args ...interface{}) (Function, error)

// Deprecated: Use the new types and functions from params.go instead.
func registerOldParamsFunction(spec FunctionSpec, autoResolveFunctionArgs bool, ctor oldParamsFunctionCtor, checks ...oldParamsArgCheckFn) struct{} {
	spec.Params = OldStyleParams()
	if len(checks) > 0 {
		ctor = oldParamsCheckArgs(ctor, checks...)
	}
	if autoResolveFunctionArgs {
		ctor = functionWithOldParamsAutoResolvedFunctionArgs("function "+spec.Name, ctor)
	}
	if err := AllFunctions.Add(spec, func(args *ParsedParams) (Function, error) {
		return ctor(args.Raw()...)
	}); err != nil {
		panic(err)
	}
	return struct{}{}
}

//------------------------------------------------------------------------------

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsWithDynamicArgs(annotation string, args []interface{}, fn oldParamsFunctionCtor) Function {
	fns := []Function{}
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
		dynFunc, err := fn(dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	}, aggregateTargetPaths(fns...))
}

// Deprecated: Use the new types and functions from params.go instead.
func functionWithOldParamsAutoResolvedFunctionArgs(annotation string, fn oldParamsFunctionCtor) oldParamsFunctionCtor {
	return func(args ...interface{}) (Function, error) {
		for i, arg := range args {
			switch t := arg.(type) {
			case *Literal:
				args[i] = t.Value
			case Function:
				return oldParamsWithDynamicArgs(annotation, args, fn), nil
			}
		}
		return fn(args...)
	}
}

//------------------------------------------------------------------------------

// Deprecated: Use the new types and functions from params.go instead.
type oldParamsArgCheckFn func(args []interface{}) error

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsCheckArgs(fn oldParamsFunctionCtor, checks ...oldParamsArgCheckFn) oldParamsFunctionCtor {
	return func(args ...interface{}) (Function, error) {
		for _, check := range checks {
			if err := check(args); err != nil {
				return nil, err
			}
		}
		return fn(args...)
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectAtLeastOneArg() oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) == 0 {
			return errors.New("expected at least one argument, received none")
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectOneOrZeroArgs() oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) > 1 {
			return fmt.Errorf("expected one or zero arguments, received: %v", len(args))
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectBetweenNAndMArgs(n, m int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) < n {
			return fmt.Errorf("expected at least %v arguments, received: %v", n, len(args))
		}
		if len(args) > m {
			return fmt.Errorf("expected fewer than %v arguments, received: %v", m, len(args))
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectNArgs(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) != i {
			return fmt.Errorf("expected %v arguments, received: %v", i, len(args))
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectStringArg(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case string:
		case []byte:
			// Allow byte slice value here but cast it.
			args[i] = string(t)
		default:
			return fmt.Errorf("expected string argument, received %T", args[i])
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectFunctionArg(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		if _, ok := args[i].(Function); ok {
			return nil
		}
		args[i] = NewLiteralFunction("", args[i])
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectAllStringArgs() oldParamsArgCheckFn {
	return func(args []interface{}) error {
		for i, arg := range args {
			switch t := arg.(type) {
			case string:
			case []byte:
				// Allow byte slice value here but cast it.
				args[i] = string(t)
			default:
				return fmt.Errorf("expected string argument %v, received %T", i, arg)
			}
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectIntArg(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case int64:
		case float64:
			args[i] = int64(t)
		case json.Number:
			var err error
			if args[i], err = t.Int64(); err != nil {
				if fValue, ferr := t.Float64(); ferr == nil {
					args[i] = int64(fValue)
				} else {
					return fmt.Errorf("expected int argument, failed to parse: %w", err)
				}
			}
		default:
			return fmt.Errorf("expected int argument, received %T", args[i])
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectFloatArg(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case int64:
			args[i] = float64(t)
		case float64:
		case json.Number:
			var err error
			if args[i], err = t.Float64(); err != nil {
				return fmt.Errorf("expected float argument, failed to parse: %w", err)
			}
		default:
			return fmt.Errorf("expected float argument, received %T", args[i])
		}
		return nil
	}
}

// Deprecated: Use the new types and functions from params.go instead.
func oldParamsExpectBoolArg(i int) oldParamsArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		_, ok := args[i].(bool)
		if !ok {
			return fmt.Errorf("expected bool argument, received %T", args[i])
		}
		return nil
	}
}
