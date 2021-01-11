package query

import (
	"encoding/json"
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

type arithmeticOpFunc func(l, r interface{}) (interface{}, error)

func arithmeticFunc(lhs, rhs Function, op arithmeticOpFunc) (Function, error) {
	var litL, litR *Literal
	var isLit bool
	if litL, isLit = lhs.(*Literal); isLit {
		if litR, isLit = rhs.(*Literal); isLit {
			res, err := op(litL.Value, litR.Value)
			if err != nil {
				return nil, err
			}
			return NewLiteralFunction(res), nil
		}
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		var err error
		var leftV, rightV interface{}
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}
		return op(leftV, rightV)
	}, aggregateTargetPaths(lhs, rhs)), nil
}

//------------------------------------------------------------------------------

func restrictForComparison(v interface{}) interface{} {
	v = ISanitize(v)
	switch t := v.(type) {
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case json.Number:
		if f, err := IGetNumber(t); err == nil {
			return f
		}
	case []byte:
		return string(t)
	}
	return v
}

// ErrDivideByZero occurs when an arithmetic operator is prevented from dividing
// a value by zero.
var ErrDivideByZero = errors.New("attempted to divide by zero")

func prodOp(op ArithmeticOperator) (arithmeticOpFunc, bool) {
	switch op {
	case ArithmeticMul:
		return func(left, right interface{}) (interface{}, error) {
			var err error
			var lhs, rhs float64
			if lhs, err = IGetNumber(left); err == nil {
				rhs, err = IGetNumber(right)
			}
			if err != nil {
				return nil, err
			}
			return lhs * rhs, nil
		}, true
	case ArithmeticDiv:
		return func(left, right interface{}) (interface{}, error) {
			var err error
			var lhs, rhs float64
			if lhs, err = IGetNumber(left); err == nil {
				rhs, err = IGetNumber(right)
			}
			if err != nil {
				return nil, err
			}
			if rhs == 0 {
				return nil, ErrDivideByZero
			}
			return lhs / rhs, nil
		}, true
	case ArithmeticMod:
		return func(left, right interface{}) (interface{}, error) {
			var err error
			var lhs, rhs int64
			if lhs, err = IGetInt(left); err == nil {
				rhs, err = IGetInt(right)
			}
			if err != nil {
				return nil, err
			}
			if rhs == 0 {
				return nil, ErrDivideByZero
			}
			return lhs % rhs, nil
		}, true
	}
	return nil, false
}

func coalesce(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		lhsV, err := lhs.Exec(ctx)
		if err == nil && !IIsNull(lhsV) {
			return lhsV, nil
		}
		return rhs.Exec(ctx)
	}, aggregateTargetPaths(lhs, rhs))
}

func sumOp(op ArithmeticOperator) (arithmeticOpFunc, bool) {
	switch op {
	case ArithmeticAdd:
		return func(left, right interface{}) (interface{}, error) {
			var err error
			switch left.(type) {
			case float64, int, int64, uint64, json.Number:
				var lhs, rhs float64
				if lhs, err = IGetNumber(left); err == nil {
					rhs, err = IGetNumber(right)
				}
				if err != nil {
					return nil, err
				}
				return lhs + rhs, nil
			case string, []byte:
				var lhs, rhs string
				if lhs, err = IGetString(left); err == nil {
					rhs, err = IGetString(right)
				}
				if err != nil {
					return nil, err
				}
				return lhs + rhs, nil
			}
			return nil, NewTypeError(left, ValueNumber, ValueString)
		}, true
	case ArithmeticSub:
		return func(left, right interface{}) (interface{}, error) {
			var err error
			var lhs, rhs float64
			if lhs, err = IGetNumber(left); err == nil {
				rhs, err = IGetNumber(right)
			}
			if err != nil {
				return nil, err
			}
			return lhs - rhs, nil
		}, true
	}
	return nil, false
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

func compareOp(op ArithmeticOperator) (arithmeticOpFunc, bool) {
	switch op {
	case ArithmeticEq,
		ArithmeticNeq,
		ArithmeticGt,
		ArithmeticGte,
		ArithmeticLt,
		ArithmeticLte:
		strOpFn := compareStrFn(op)
		numOpFn := compareNumFn(op)
		boolOpFn := compareBoolFn(op)
		genericOpFn := compareGenericFn(op)
		return func(left, right interface{}) (interface{}, error) {
			switch lhs := restrictForComparison(left).(type) {
			case string:
				if strOpFn == nil {
					return nil, NewTypeError(left)
				}
				rhs, err := IGetString(right)
				if err != nil {
					if op == ArithmeticNeq {
						return true, nil
					}
					return nil, err
				}
				return strOpFn(lhs, rhs), nil
			case float64:
				if numOpFn == nil {
					return nil, NewTypeError(left)
				}
				rhs, err := IGetNumber(right)
				if err != nil {
					if op == ArithmeticNeq {
						return true, nil
					}
					return nil, err
				}
				return numOpFn(lhs, rhs), nil
			case bool:
				if boolOpFn == nil {
					return nil, NewTypeError(left)
				}
				rhs, err := IGetBool(right)
				if err != nil {
					if op == ArithmeticNeq {
						return true, nil
					}
					return nil, err
				}
				return boolOpFn(lhs, rhs), nil
			default:
				if genericOpFn == nil {
					return nil, NewTypeError(left)
				}
				return genericOpFn(left, right), nil
			}
		}, true
	}
	return nil, false
}

func boolOr(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		lhsV, err := lhs.Exec(ctx)
		if err != nil {
			return nil, err
		}
		b, err := IGetBool(lhsV)
		if err != nil {
			return nil, err
		}
		if b {
			return true, nil
		}
		rhsV, err := rhs.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if b, err = IGetBool(rhsV); err != nil {
			return nil, err
		}
		return b, nil
	}, aggregateTargetPaths(lhs, rhs))
}

func boolAnd(lhs, rhs Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		lhsV, err := lhs.Exec(ctx)
		if err != nil {
			return nil, err
		}
		b, err := IGetBool(lhsV)
		if err != nil {
			return nil, err
		}
		if !b {
			return false, nil
		}
		rhsV, err := rhs.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if b, err = IGetBool(rhsV); err != nil {
			return nil, err
		}
		return b, nil
	}, aggregateTargetPaths(lhs, rhs))
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

	var err error

	// First pass to resolve division, multiplication and coalesce
	fnsNew, opsNew := []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		if opFunc, isProd := prodOp(op); isProd {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(fnsNew[len(fnsNew)-1], fns[i+1], opFunc); err != nil {
				return nil, err
			}
		} else if op == ArithmeticPipe {
			fnsNew[len(fnsNew)-1] = coalesce(fnsNew[len(fnsNew)-1], fns[i+1])
		} else {
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
		if opFunc, isSum := sumOp(op); isSum {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(fnsNew[len(fnsNew)-1], fns[i+1], opFunc); err != nil {
				return nil, err
			}
		} else {
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	// Third pass for numerical comparison
	fnsNew, opsNew = []Function{fns[0]}, []ArithmeticOperator{}
	for i, op := range ops {
		if opFunc, isCompare := compareOp(op); isCompare {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(fnsNew[len(fnsNew)-1], fns[i+1], opFunc); err != nil {
				return nil, err
			}
		} else {
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
		case ArithmeticAnd:
			fnsNew[len(fnsNew)-1] = boolAnd(fnsNew[len(fnsNew)-1], fns[i+1])
		case ArithmeticOr:
			fnsNew[len(fnsNew)-1] = boolOr(fnsNew[len(fnsNew)-1], fns[i+1])
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
