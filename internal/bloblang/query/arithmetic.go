package query

import (
	"errors"
	"fmt"

	"github.com/google/go-cmp/cmp"
)

//------------------------------------------------------------------------------

// ArithmeticOperator represents an arithmetic operation that combines the
// results of two query functions.
type ArithmeticOperator int

// All arithmetic operators.
const (
	ArithmeticAdd ArithmeticOperator = iota
	ArithmeticSub
	ArithmeticDiv
	ArithmeticMul
	ArithmeticMod
	ArithmeticEq
	ArithmeticNeq
	ArithmeticGt
	ArithmeticLt
	ArithmeticGte
	ArithmeticLte
	ArithmeticAnd
	ArithmeticOr
	ArithmeticPipe
)

func restrictForComparison(v interface{}) interface{} {
	switch t := v.(type) {
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case []byte:
		return string(t)
	}
	return v
}

func add(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}

		switch leftV.(type) {
		case float64, int64, uint64:
			var lhs, rhs float64
			if lhs, err = IGetNumber(leftV); err == nil {
				rhs, err = IGetNumber(rightV)
			}
			if err != nil {
				return nil, err
			}
			return lhs + rhs, nil
		case string, []byte:
			var lhs, rhs string
			if lhs, err = IGetString(leftV); err == nil {
				rhs, err = IGetString(rightV)
			}
			if err != nil {
				return nil, err
			}
			return lhs + rhs, nil
		}
		return nil, NewTypeError(leftV, ValueNumber, ValueString)
	})
}

func sub(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}

		var lhs, rhs float64
		if lhs, err = IGetNumber(leftV); err == nil {
			rhs, err = IGetNumber(rightV)
		}
		if err != nil {
			return nil, err
		}

		return lhs - rhs, nil
	})
}

// ErrDivideByZero occurs when an arithmetic operator is prevented from dividing
// a value by zero.
var ErrDivideByZero = errors.New("attempted to divide by zero")

func divide(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}

		var lhs, rhs float64
		if lhs, err = IGetNumber(leftV); err == nil {
			rhs, err = IGetNumber(rightV)
		}
		if err != nil {
			return nil, err
		}

		if rhs == 0 {
			return nil, ErrDivideByZero
		}

		return lhs / rhs, nil
	})
}

func multiply(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}

		var lhs, rhs float64
		if lhs, err = IGetNumber(leftV); err == nil {
			rhs, err = IGetNumber(rightV)
		}
		if err != nil {
			return nil, err
		}

		return lhs * rhs, nil
	})
}

func modulo(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}

		var lhs, rhs int64
		if lhs, err = IGetInt(leftV); err == nil {
			rhs, err = IGetInt(rightV)
		}
		if err != nil {
			return nil, err
		}

		if rhs == 0 {
			return nil, ErrDivideByZero
		}

		return lhs % rhs, nil
	})
}

func coalesce(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		lhsV, err := lhs.Exec(ctx)
		if err == nil && !IIsNull(lhsV) {
			return lhsV, nil
		}
		return rhs.Exec(ctx)
	})
}

func compareNumFn(op ArithmeticOperator) func(lhs, rhs float64) bool {
	switch op {
	case ArithmeticEq:
		return func(lhs, rhs float64) bool {
			return lhs == rhs
		}
	case ArithmeticNeq:
		return func(lhs, rhs float64) bool {
			return lhs != rhs
		}
	case ArithmeticGt:
		return func(lhs, rhs float64) bool {
			return lhs > rhs
		}
	case ArithmeticGte:
		return func(lhs, rhs float64) bool {
			return lhs >= rhs
		}
	case ArithmeticLt:
		return func(lhs, rhs float64) bool {
			return lhs < rhs
		}
	case ArithmeticLte:
		return func(lhs, rhs float64) bool {
			return lhs <= rhs
		}
	}
	return nil
}

func compareStrFn(op ArithmeticOperator) func(lhs, rhs string) bool {
	switch op {
	case ArithmeticEq:
		return func(lhs, rhs string) bool {
			return lhs == rhs
		}
	case ArithmeticNeq:
		return func(lhs, rhs string) bool {
			return lhs != rhs
		}
	case ArithmeticGt:
		return func(lhs, rhs string) bool {
			return lhs > rhs
		}
	case ArithmeticGte:
		return func(lhs, rhs string) bool {
			return lhs >= rhs
		}
	case ArithmeticLt:
		return func(lhs, rhs string) bool {
			return lhs < rhs
		}
	case ArithmeticLte:
		return func(lhs, rhs string) bool {
			return lhs <= rhs
		}
	}
	return nil
}

func compareBoolFn(op ArithmeticOperator) func(lhs, rhs bool) bool {
	switch op {
	case ArithmeticEq:
		return func(lhs, rhs bool) bool {
			return lhs == rhs
		}
	case ArithmeticNeq:
		return func(lhs, rhs bool) bool {
			return lhs != rhs
		}
	}
	return nil
}

func compareGenericFn(op ArithmeticOperator) func(lhs, rhs interface{}) bool {
	switch op {
	case ArithmeticEq:
		return func(lhs, rhs interface{}) bool {
			return cmp.Equal(lhs, rhs)
		}
	case ArithmeticNeq:
		return func(lhs, rhs interface{}) bool {
			return !cmp.Equal(lhs, rhs)
		}
	}
	return nil
}

func compare(lhs, rhs Function, op ArithmeticOperator) (Function, error) {
	strOpFn := compareStrFn(op)
	numOpFn := compareNumFn(op)
	boolOpFn := compareBoolFn(op)
	genericOpFn := compareGenericFn(op)
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var lhsV, rhsV interface{}
		var err error
		if lhsV, err = lhs.Exec(ctx); err == nil {
			rhsV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}
		switch lhs := restrictForComparison(lhsV).(type) {
		case string:
			if strOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			rhs, err := IGetString(rhsV)
			if err != nil {
				if op == ArithmeticNeq {
					return true, nil
				}
				return nil, err
			}
			return strOpFn(lhs, rhs), nil
		case float64:
			if numOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			rhs, err := IGetNumber(rhsV)
			if err != nil {
				if op == ArithmeticNeq {
					return true, nil
				}
				return nil, err
			}
			return numOpFn(lhs, rhs), nil
		case bool:
			if boolOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			rhs, err := IGetBool(rhsV)
			if err != nil {
				if op == ArithmeticNeq {
					return true, nil
				}
				return nil, err
			}
			return boolOpFn(lhs, rhs), nil
		default:
			if genericOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			return genericOpFn(lhsV, rhsV), nil
		}
	}), nil
}

func logicalBool(lhs, rhs Function, op ArithmeticOperator) (Function, error) {
	switch op {
	case ArithmeticAnd:
		return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			var lhsV, rhsV bool
			var err error

			if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
				lhsV, err = IGetBool(leftV)
			} else {
				err = tmpErr
			}
			if err != nil {
				return nil, err
			}
			if !lhsV {
				return false, nil
			}

			if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
				rhsV, err = IGetBool(rightV)
			} else {
				err = tmpErr
			}
			if err != nil {
				return nil, err
			}
			return rhsV, nil
		}), nil
	case ArithmeticOr:
		return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			var lhsV, rhsV bool
			var err error

			if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
				lhsV, err = IGetBool(leftV)
			} else {
				err = tmpErr
			}
			if err != nil {
				return nil, err
			}
			if lhsV {
				return true, nil
			}

			if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
				rhsV, err = IGetBool(rightV)
			} else {
				err = tmpErr
			}
			if err != nil {
				return nil, err
			}
			return lhsV || rhsV, nil
		}), nil
	default:
		return nil, fmt.Errorf("operator not supported: %v", op)
	}
}

// NewArithmeticExpression creates a single query function from a list of child
// functions and the arithmetic operator types that chain them together. The
// length of functions must be exactly one fewer than the length of operators.
func NewArithmeticExpression(fns []Function, ops []ArithmeticOperator) (Function, error) {
	if len(fns) == 1 && len(ops) == 0 {
		return fns[0], nil
	}
	if len(fns) != (len(ops) + 1) {
		return nil, fmt.Errorf("mismatch of functions (%v) to arithmetic operators (%v)", len(fns), len(ops))
	}

	// First pass to resolve division, multiplication and coalesce
	fnsNew, opsNew := []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		switch op {
		case ArithmeticMul:
			fnsNew[len(fnsNew)-1] = multiply(fnsNew[len(fnsNew)-1], fns[i+1])
		case ArithmeticDiv:
			fnsNew[len(fnsNew)-1] = divide(fnsNew[len(fnsNew)-1], fns[i+1])
		case ArithmeticMod:
			fnsNew[len(fnsNew)-1] = modulo(fnsNew[len(fnsNew)-1], fns[i+1])
		case ArithmeticPipe:
			fnsNew[len(fnsNew)-1] = coalesce(fnsNew[len(fnsNew)-1], fns[i+1])
		default:
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	// Second pass to resolve addition and subtraction
	fnsNew, opsNew = []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		switch op {
		case ArithmeticAdd:
			fnsNew[len(fnsNew)-1] = add(fnsNew[len(fnsNew)-1], fns[i+1])
		case ArithmeticSub:
			fnsNew[len(fnsNew)-1] = sub(fnsNew[len(fnsNew)-1], fns[i+1])
		default:
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	// Third pass for numerical comparison
	var err error
	fnsNew, opsNew = []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		switch op {
		case ArithmeticEq,
			ArithmeticNeq,
			ArithmeticGt,
			ArithmeticGte,
			ArithmeticLt,
			ArithmeticLte:
			if fnsNew[len(fnsNew)-1], err = compare(fnsNew[len(fnsNew)-1], fns[i+1], op); err != nil {
				return nil, err
			}
		default:
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	// Fourth pass for boolean operators
	fnsNew, opsNew = []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		switch op {
		case ArithmeticAnd,
			ArithmeticOr:
			if fnsNew[len(fnsNew)-1], err = logicalBool(fnsNew[len(fnsNew)-1], fns[i+1], op); err != nil {
				return nil, err
			}
		default:
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	return nil, fmt.Errorf("unresolved arithmetic operators (%v)", ops)
}

//------------------------------------------------------------------------------
