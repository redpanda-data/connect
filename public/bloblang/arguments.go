package bloblang

import (
	"fmt"
	"reflect"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// ArgSpec provides an API for validating and extracting function or method
// arguments by registering them with pointer receivers.
type ArgSpec struct {
	n          int
	validators []func(args []any) error
}

// NewArgSpec creates an argument parser/validator.
func NewArgSpec() *ArgSpec {
	return &ArgSpec{}
}

// Extract the specified typed arguments from a slice of generic arguments.
// Returns an error if the number of arguments does not match the spec, and
// returns an *ArgError if the type of an argument is mismatched.
func (a *ArgSpec) Extract(args []any) error {
	if len(args) != a.n {
		return fmt.Errorf("expected %v arguments, received %v", a.n, len(args))
	}
	for _, v := range a.validators {
		if err := v(args); err != nil {
			return err
		}
	}
	return nil
}

// IntVar creates an int argument to follow the previously created argument.
func (a *ArgSpec) IntVar(i *int) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		v, err := query.IGetInt(args[index])
		if err != nil {
			return newArgError(index, reflect.Int, args[index])
		}
		*i = int(v)
		return nil
	})

	return a
}

// Int64Var creates an int64 argument to follow the previously created argument.
func (a *ArgSpec) Int64Var(i *int64) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		v, err := query.IGetInt(args[index])
		if err != nil {
			return newArgError(index, reflect.Int64, args[index])
		}
		*i = v
		return nil
	})

	return a
}

// Float64Var creates a Float64 argument to follow the previously created
// argument.
func (a *ArgSpec) Float64Var(f *float64) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		v, err := query.IGetNumber(args[index])
		if err != nil {
			return newArgError(index, reflect.Float64, args[index])
		}
		*f = v
		return nil
	})

	return a
}

// BoolVar creates a boolean argument to follow the previously created argument.
func (a *ArgSpec) BoolVar(b *bool) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		v, err := query.IGetBool(args[index])
		if err != nil {
			return newArgError(index, reflect.Bool, args[index])
		}
		*b = v
		return nil
	})

	return a
}

// StringVar creates a string argument to follow the previously created
// argument.
func (a *ArgSpec) StringVar(s *string) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		v, err := query.IGetString(args[index])
		if err != nil {
			return newArgError(index, reflect.String, args[index])
		}
		*s = v
		return nil
	})

	return a
}

// AnyVar creates an argument to follow the previously created argument that can
// have any value.
func (a *ArgSpec) AnyVar(i *any) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []any) error {
		*i = args[index]
		return nil
	})

	return a
}

//------------------------------------------------------------------------------

// ArgError represents an error encountered when parsing a function or method
// argument.
type ArgError struct {
	// The argument index
	Index int

	// The expected argument type
	ExpectedKind reflect.Kind

	// The actual type provided
	ActualKind reflect.Kind

	// The value of the argument
	Value any
}

func (a *ArgError) Error() string {
	return fmt.Sprintf("bad argument %v: expected %v value, got %v (%v)", a.Index, a.ExpectedKind.String(), a.ActualKind.String(), a.Value)
}

func newArgError(index int, expected reflect.Kind, actual any) error {
	return &ArgError{
		Index:        index,
		ExpectedKind: expected,
		ActualKind:   reflect.TypeOf(actual).Kind(),
		Value:        actual,
	}
}
