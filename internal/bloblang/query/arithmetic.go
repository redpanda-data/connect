package query

import (
	"encoding/json"
	"errors"
	"fmt"
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

func (o ArithmeticOperator) String() string {
	switch o {
	case ArithmeticAdd:
		return "add"
	case ArithmeticSub:
		return "subtract"
	case ArithmeticDiv:
		return "divide"
	case ArithmeticMul:
		return "multiply"
	case ArithmeticMod:
		return "modulo"
	case ArithmeticEq, ArithmeticNeq, ArithmeticGt, ArithmeticLt, ArithmeticGte, ArithmeticLte:
		return "compare"
	case ArithmeticAnd:
		return "boolean and"
	case ArithmeticOr:
		return "boolean or"
	case ArithmeticPipe:
		return "coalesce"
	}
	return ""
}

type arithmeticOpFunc func(lhs, rhs Function, l, r any) (any, error)

func arithmeticFunc(lhs, rhs Function, op arithmeticOpFunc) (Function, error) {
	annotation := rhs.Annotation()

	var litL, litR *Literal
	var isLit bool
	if litL, isLit = lhs.(*Literal); isLit {
		if litR, isLit = rhs.(*Literal); isLit {
			res, err := op(lhs, rhs, litL.Value, litR.Value)
			if err != nil {
				return nil, err
			}
			return NewLiteralFunction(annotation, res), nil
		}
	}

	return ClosureFunction(annotation, func(ctx FunctionContext) (any, error) {
		var err error
		var leftV, rightV any
		if leftV, err = lhs.Exec(ctx); err == nil {
			rightV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}
		return op(lhs, rhs, leftV, rightV)
	}, aggregateTargetPaths(lhs, rhs)), nil
}

//------------------------------------------------------------------------------

// ErrDivideByZero occurs when an arithmetic operator is prevented from dividing
// a value by zero.
var ErrDivideByZero = errors.New("attempted to divide by zero")

type (
	intArithmeticFunc   func(left, right int64) (int64, error)
	floatArithmeticFunc func(left, right float64) (float64, error)
)

// Takes two arithmetic funcs, one for integer values and one for float values
// and returns a generic arithmetic func. If both values can be represented as
// integers the integer func is called, otherwise the float func is called.
func numberDegradationFunc(op ArithmeticOperator, iFn intArithmeticFunc, fFn floatArithmeticFunc) arithmeticOpFunc {
	return func(lhs, rhs Function, left, right any) (any, error) {
		left = ISanitize(left)
		right = ISanitize(right)

		if leftFloat, leftIsFloat := left.(float64); leftIsFloat {
			rightFloat, err := IGetNumber(right)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return fFn(leftFloat, rightFloat)
		}
		if rightFloat, rightIsFloat := right.(float64); rightIsFloat {
			leftFloat, err := IGetNumber(left)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return fFn(leftFloat, rightFloat)
		}

		leftInt, err := IGetInt(left)
		if err != nil {
			return nil, NewTypeMismatch(op.String(), lhs, rhs, left, right)
		}
		rightInt, err := IGetInt(right)
		if err != nil {
			return nil, NewTypeMismatch(op.String(), lhs, rhs, left, right)
		}

		return iFn(leftInt, rightInt)
	}
}

func prodOp(op ArithmeticOperator) (arithmeticOpFunc, bool) {
	switch op {
	case ArithmeticMul:
		return numberDegradationFunc(op,
			func(lhs, rhs int64) (int64, error) {
				return lhs * rhs, nil
			},
			func(lhs, rhs float64) (float64, error) {
				return lhs * rhs, nil
			},
		), true
	case ArithmeticDiv:
		// Only executes on float values.
		return func(lFn, rFn Function, left, right any) (any, error) {
			lhs, err := IGetNumber(left)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := IGetNumber(right)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			if rhs == 0 {
				return nil, ErrFrom(ErrDivideByZero, rFn)
			}
			return lhs / rhs, nil
		}, true
	case ArithmeticMod:
		// Only executes on integer values.
		return func(lFn, rFn Function, left, right any) (any, error) {
			lhs, err := IGetInt(left)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := IGetInt(right)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			if rhs == 0 {
				return nil, ErrFrom(ErrDivideByZero, rFn)
			}
			return lhs % rhs, nil
		}, true
	}
	return nil, false
}

func sumOp(op ArithmeticOperator) (arithmeticOpFunc, bool) {
	switch op {
	case ArithmeticAdd:
		numberAdd := numberDegradationFunc(op,
			func(left, right int64) (int64, error) {
				return left + right, nil
			},
			func(left, right float64) (float64, error) {
				return left + right, nil
			},
		)
		return func(lFn, rFn Function, left, right any) (any, error) {
			switch left.(type) {
			case float64, int, int64, uint64, json.Number:
				return numberAdd(lFn, rFn, left, right)
			case string, []byte:
				lhs, err := IGetString(left)
				if err != nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				rhs, err := IGetString(right)
				if err != nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				return lhs + rhs, nil
			}
			return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
		}, true
	case ArithmeticSub:
		return numberDegradationFunc(op,
			func(lhs, rhs int64) (int64, error) {
				return lhs - rhs, nil
			},
			func(lhs, rhs float64) (float64, error) {
				return lhs - rhs, nil
			},
		), true
	}
	return nil, false
}

//------------------------------------------------------------------------------

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

func compareGenericFn(op ArithmeticOperator) func(lhs, rhs any) bool {
	switch op {
	case ArithmeticEq:
		return ICompare
	case ArithmeticNeq:
		return func(lhs, rhs any) bool {
			return !ICompare(lhs, rhs)
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
		return func(lFn, rFn Function, left, right any) (any, error) {
			switch lhs := restrictForComparison(left).(type) {
			case string:
				if strOpFn == nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				rhs, err := IGetString(right)
				if err != nil {
					if genericOpFn == nil {
						return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
					}
					return genericOpFn(lhs, restrictForComparison(right)), nil
				}
				return strOpFn(lhs, rhs), nil
			case float64:
				if numOpFn == nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				rhs, err := IGetNumber(right)
				if err != nil {
					if genericOpFn == nil {
						return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
					}
					return genericOpFn(lhs, restrictForComparison(right)), nil
				}
				return numOpFn(lhs, rhs), nil
			case bool:
				if boolOpFn == nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				rhs, err := IGetBool(right)
				if err != nil {
					if genericOpFn == nil {
						return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
					}
					return genericOpFn(lhs, restrictForComparison(right)), nil
				}
				return boolOpFn(lhs, rhs), nil
			default:
				if genericOpFn == nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				return genericOpFn(left, right), nil
			}
		}, true
	}
	return nil, false
}

func boolOr(lhs, rhs Function) Function {
	return ClosureFunction(rhs.Annotation(), func(ctx FunctionContext) (any, error) {
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
	return ClosureFunction(rhs.Annotation(), func(ctx FunctionContext) (any, error) {
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

func coalesce(lhs, rhs Function) Function {
	return ClosureFunction(rhs.Annotation(), func(ctx FunctionContext) (any, error) {
		lhsV, err := lhs.Exec(ctx)
		if err == nil && !IIsNull(lhsV) {
			return lhsV, nil
		}
		return rhs.Exec(ctx)
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
		leftFn, rightFn := fnsNew[len(fnsNew)-1], fns[i+1]
		if opFunc, isProd := prodOp(op); isProd {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(leftFn, rightFn, opFunc); err != nil {
				return nil, err
			}
		} else if op == ArithmeticPipe {
			fnsNew[len(fnsNew)-1] = coalesce(leftFn, rightFn)
		} else {
			fnsNew = append(fnsNew, rightFn)
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
		leftFn, rightFn := fnsNew[len(fnsNew)-1], fns[i+1]
		if opFunc, isSum := sumOp(op); isSum {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(leftFn, rightFn, opFunc); err != nil {
				return nil, err
			}
		} else {
			fnsNew = append(fnsNew, rightFn)
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
		leftFn, rightFn := fnsNew[len(fnsNew)-1], fns[i+1]
		if opFunc, isCompare := compareOp(op); isCompare {
			if fnsNew[len(fnsNew)-1], err = arithmeticFunc(leftFn, rightFn, opFunc); err != nil {
				return nil, err
			}
		} else {
			fnsNew = append(fnsNew, rightFn)
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
		leftFn, rightFn := fnsNew[len(fnsNew)-1], fns[i+1]
		switch op {
		case ArithmeticAnd:
			fnsNew[len(fnsNew)-1] = boolAnd(leftFn, rightFn)
		case ArithmeticOr:
			fnsNew[len(fnsNew)-1] = boolOr(leftFn, rightFn)
		default:
			fnsNew = append(fnsNew, rightFn)
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
