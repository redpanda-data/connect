package query

import (
	"errors"
	"fmt"
	"sort"

	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

type closureFn func(ctx FunctionContext) (interface{}, error)

func (f closureFn) Exec(ctx FunctionContext) (interface{}, error) {
	return f(ctx)
}

//------------------------------------------------------------------------------

func withDynamicArgs(args []interface{}, fn FunctionCtor) Function {
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
		dynFunc, err := fn(dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	})
}

func enableDynamicArgs(fn FunctionCtor) FunctionCtor {
	return func(args ...interface{}) (Function, error) {
		for _, arg := range args {
			if _, isDyn := arg.(Function); isDyn {
				return withDynamicArgs(args, fn), nil
			}
		}
		return fn(args...)
	}
}

//------------------------------------------------------------------------------

// ArgCheckFn is an optional argument type checker for a function constructor.
type ArgCheckFn func(args []interface{}) error

func checkArgs(fn FunctionCtor, checks ...ArgCheckFn) FunctionCtor {
	return func(args ...interface{}) (Function, error) {
		for _, check := range checks {
			if err := check(args); err != nil {
				return nil, err
			}
		}
		return fn(args...)
	}
}

// ExpectAtLeastOneArg returns an error unless >0 arguments are specified.
func ExpectAtLeastOneArg() ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) == 0 {
			return errors.New("expected at least one parameter, received none")
		}
		return nil
	}
}

// ExpectOneOrZeroArgs returns an error if more than one arg is specified.
func ExpectOneOrZeroArgs() ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) > 1 {
			return fmt.Errorf("expected one or zero parameters, received: %v", len(args))
		}
		return nil
	}
}

// ExpectNArgs returns an error unless exactly N arguments are specified.
func ExpectNArgs(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) != i {
			return fmt.Errorf("expected %v parameters, received: %v", i, len(args))
		}
		return nil
	}
}

// ExpectStringArg returns an error if an argument i is not a string type (or a
// byte slice that can be converted).
func ExpectStringArg(i int) ArgCheckFn {
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
			return fmt.Errorf("expected string param, received %T", args[i])
		}
		return nil
	}
}

// ExpectAllStringArgs returns an error if any argument is not a string type (or
// a byte slice that can be converted).
func ExpectAllStringArgs() ArgCheckFn {
	return func(args []interface{}) error {
		for i, arg := range args {
			switch t := arg.(type) {
			case string:
			case []byte:
				// Allow byte slice value here but cast it.
				args[i] = string(t)
			default:
				return fmt.Errorf("expected string param %v, received %T", i, arg)
			}
		}
		return nil
	}
}

// ExpectIntArg returns an error if an argument i is not an integer type.
func ExpectIntArg(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case int64:
		case float64:
			args[i] = int64(t)
		default:
			return fmt.Errorf("expected int param, received %T", args[i])
		}
		return nil
	}
}

// ExpectFloatArg returns an error if an argument i is not a float type.
func ExpectFloatArg(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case int64:
			args[i] = float64(t)
		case float64:
		default:
			return fmt.Errorf("expected float param, received %T", args[i])
		}
		return nil
	}
}

// ExpectBoolArg returns an error if an argument i is not a boolean type.
func ExpectBoolArg(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		_, ok := args[i].(bool)
		if !ok {
			return fmt.Errorf("expected bool param, received %T", args[i])
		}
		return nil
	}
}

//------------------------------------------------------------------------------

// FunctionCtor constructs a new function from input arguments.
type FunctionCtor func(args ...interface{}) (Function, error)

// RegisterFunction to be accessible from Bloblang queries. Returns an empty
// struct in order to allow inline calls.
func RegisterFunction(name string, allowDynamicArgs bool, ctor FunctionCtor, checks ...ArgCheckFn) struct{} {
	if len(checks) > 0 {
		ctor = checkArgs(ctor, checks...)
	}
	if allowDynamicArgs {
		ctor = enableDynamicArgs(ctor)
	}
	if _, exists := functions[name]; exists {
		panic(fmt.Sprintf("Conflicting function name: %v", name))
	}
	functions[name] = ctor
	return struct{}{}
}

var functions = map[string]FunctionCtor{}

// ListFunctions returns a slice of function names, sorted alphabetically.
func ListFunctions() []string {
	functionNames := make([]string, 0, len(functions))
	for k := range functions {
		functionNames = append(functionNames, k)
	}
	sort.Strings(functionNames)
	return functionNames
}

//------------------------------------------------------------------------------
