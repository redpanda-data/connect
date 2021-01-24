package query

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Function takes a set of contextual arguments and returns the result of the
// query.
type Function interface {
	// Execute this function for a message of a batch.
	Exec(ctx FunctionContext) (interface{}, error)

	// Return a map of target types to path segments for any targets that this
	// query function references.
	QueryTargets(ctx TargetsContext) []TargetPath
}

// FunctionCtor constructs a new function from input arguments.
type FunctionCtor func(args ...interface{}) (Function, error)

//------------------------------------------------------------------------------

// ClosureFunction allows you to define a Function using closures, this is a
// convenient constructor for function implementations that don't manage complex
// state.
func ClosureFunction(
	exec func(ctx FunctionContext) (interface{}, error),
	queryTargets func(ctx TargetsContext) []TargetPath,
) Function {
	if queryTargets == nil {
		queryTargets = func(TargetsContext) []TargetPath { return nil }
	}
	return closureFunction{exec, queryTargets}
}

type closureFunction struct {
	exec         func(ctx FunctionContext) (interface{}, error)
	queryTargets func(ctx TargetsContext) []TargetPath
}

// Exec the underlying closure.
func (f closureFunction) Exec(ctx FunctionContext) (interface{}, error) {
	return f.exec(ctx)
}

// QueryTargets returns nothing.
func (f closureFunction) QueryTargets(ctx TargetsContext) []TargetPath {
	return f.queryTargets(ctx)
}

//------------------------------------------------------------------------------

func expandLiteralArgs(args []interface{}) {
	for i, dArg := range args {
		if lit, isLit := dArg.(*Literal); isLit {
			args[i] = lit.Value
		}
	}
}

func withDynamicArgs(args []interface{}, fn FunctionCtor) Function {
	fns := []Function{}
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
		dynFunc, err := fn(dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	}, aggregateTargetPaths(fns...))
}

func enableDynamicArgs(fn FunctionCtor) FunctionCtor {
	return func(args ...interface{}) (Function, error) {
		for i, arg := range args {
			switch t := arg.(type) {
			case *Literal:
				args[i] = t.Value
			case Function:
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
			return errors.New("expected at least one argument, received none")
		}
		return nil
	}
}

// ExpectOneOrZeroArgs returns an error if more than one arg is specified.
func ExpectOneOrZeroArgs() ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) > 1 {
			return fmt.Errorf("expected one or zero arguments, received: %v", len(args))
		}
		return nil
	}
}

// ExpectBetweenNAndMArgs returns an error unless between N and M arguments are
// specified.
func ExpectBetweenNAndMArgs(n, m int) ArgCheckFn {
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

// ExpectNArgs returns an error unless exactly N arguments are specified.
func ExpectNArgs(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) != i {
			return fmt.Errorf("expected %v arguments, received: %v", i, len(args))
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
			return fmt.Errorf("expected string argument, received %T", args[i])
		}
		return nil
	}
}

// ExpectFunctionArg ensures that parameters are query functions. If the
// argument is a static value it will be converted into a literal function.
func ExpectFunctionArg(i int) ArgCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		if _, ok := args[i].(Function); ok {
			return nil
		}
		args[i] = NewLiteralFunction(args[i])
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
				return fmt.Errorf("expected string argument %v, received %T", i, arg)
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
		case json.Number:
			var err error
			if args[i], err = t.Int64(); err != nil {
				return fmt.Errorf("expected int argument, failed to parse: %w", err)
			}
		default:
			return fmt.Errorf("expected int argument, received %T", args[i])
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

// ExpectBoolArg returns an error if an argument i is not a boolean type.
func ExpectBoolArg(i int) ArgCheckFn {
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
