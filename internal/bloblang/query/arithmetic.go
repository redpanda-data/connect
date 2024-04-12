package query

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/value"
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

type arithmeticOpFunc[T any] func(lhs, rhs Function, l, r any) (T, error)

func arithmeticFunc[T any](lhs, rhs Function, op arithmeticOpFunc[T]) (Function, error) {
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
	intArithmeticFunc[T any]   func(left, right int64) (T, error)
	uintArithmeticFunc[T any]  func(left, right uint64) (T, error)
	floatArithmeticFunc[T any] func(left, right float64) (T, error)
)

// Takes two arithmetic funcs, one for integer values and one for float values
// and returns a generic arithmetic func. If both values can be represented as
// integers the integer func is called, otherwise the float func is called.
func numberDegradationFunc[T any](
	op ArithmeticOperator,
	uiFn uintArithmeticFunc[T],
	iFn intArithmeticFunc[T],
	fFn floatArithmeticFunc[T],
) arithmeticOpFunc[T] {
	return func(lhs, rhs Function, left, right any) (t T, err error) {
		left = value.ISanitize(left)
		right = value.ISanitize(right)

		// If either value is a float then we degrade into a float calculation.
		if leftFloat, leftIsFloat := left.(float64); leftIsFloat {
			rightFloat, err := value.IGetNumber(right)
			if err != nil {
				return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return fFn(leftFloat, rightFloat)
		}
		if rightFloat, rightIsFloat := right.(float64); rightIsFloat {
			leftFloat, err := value.IGetNumber(left)
			if err != nil {
				return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return fFn(leftFloat, rightFloat)
		}

		// If either value is a signed integer then we degrade into a signed int
		// calculation.
		if leftInt, leftIsInt := left.(int64); leftIsInt {
			rightInt, err := value.IGetInt(right)
			if err != nil {
				return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return iFn(leftInt, rightInt)
		}
		if rightInt, rightIsInt := right.(int64); rightIsInt {
			leftInt, err := value.IGetInt(left)
			if err != nil {
				return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
			}
			return iFn(leftInt, rightInt)
		}

		// Finally, if we can obtain an unsigned integer from both values we can
		// calculate based on those values.
		leftUInt, err := value.IGetUInt(left)
		if err != nil {
			return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
		}
		rightUInt, err := value.IGetUInt(right)
		if err != nil {
			return t, NewTypeMismatch(op.String(), lhs, rhs, left, right)
		}
		return uiFn(leftUInt, rightUInt)
	}
}

func prodOp(op ArithmeticOperator) (arithmeticOpFunc[any], bool) {
	switch op {
	case ArithmeticMul:
		return numberDegradationFunc(op,
			func(lhs, rhs uint64) (any, error) {
				return lhs * rhs, nil
			},
			func(lhs, rhs int64) (any, error) {
				return lhs * rhs, nil
			},
			func(lhs, rhs float64) (any, error) {
				return lhs * rhs, nil
			},
		), true
	case ArithmeticDiv:
		// Only executes on float values.
		return func(lFn, rFn Function, left, right any) (any, error) {
			lhs, err := value.IGetNumber(left)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := value.IGetNumber(right)
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
			lhs, err := value.IGetInt(left)
			if err != nil {
				return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := value.IGetInt(right)
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

func sumOp(op ArithmeticOperator) (arithmeticOpFunc[any], bool) {
	switch op {
	case ArithmeticAdd:
		numberAdd := numberDegradationFunc(op,
			func(lhs, rhs uint64) (any, error) {
				return lhs + rhs, nil
			},
			func(left, right int64) (any, error) {
				return left + right, nil
			},
			func(left, right float64) (any, error) {
				return left + right, nil
			},
		)
		return func(lFn, rFn Function, left, right any) (any, error) {
			switch left.(type) {
			case float64, int, int64, uint64, json.Number:
				return numberAdd(lFn, rFn, left, right)
			case string, []byte:
				lhs, err := value.IGetString(left)
				if err != nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				rhs, err := value.IGetString(right)
				if err != nil {
					return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				return lhs + rhs, nil
			}
			return nil, NewTypeMismatch(op.String(), lFn, rFn, left, right)
		}, true
	case ArithmeticSub:
		return numberDegradationFunc(op,
			func(lhs, rhs uint64) (any, error) {
				return lhs - rhs, nil
			},
			func(lhs, rhs int64) (any, error) {
				return lhs - rhs, nil
			},
			func(lhs, rhs float64) (any, error) {
				return lhs - rhs, nil
			},
		), true
	}
	return nil, false
}

//------------------------------------------------------------------------------

func compareTFn[T int64 | uint64 | float64 | string](op ArithmeticOperator) func(lhs, rhs T) bool {
	switch op {
	case ArithmeticEq:
		return func(lhs, rhs T) bool {
			return lhs == rhs
		}
	case ArithmeticNeq:
		return func(lhs, rhs T) bool {
			return lhs != rhs
		}
	case ArithmeticGt:
		return func(lhs, rhs T) bool {
			return lhs > rhs
		}
	case ArithmeticGte:
		return func(lhs, rhs T) bool {
			return lhs >= rhs
		}
	case ArithmeticLt:
		return func(lhs, rhs T) bool {
			return lhs < rhs
		}
	case ArithmeticLte:
		return func(lhs, rhs T) bool {
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
		return value.ICompare
	case ArithmeticNeq:
		return func(lhs, rhs any) bool {
			return !value.ICompare(lhs, rhs)
		}
	}
	return nil
}

func compareOp(op ArithmeticOperator) (arithmeticOpFunc[bool], bool) {
	if _, exists := map[ArithmeticOperator]struct{}{
		ArithmeticEq:  {},
		ArithmeticNeq: {},
		ArithmeticGt:  {},
		ArithmeticGte: {},
		ArithmeticLt:  {},
		ArithmeticLte: {},
	}[op]; !exists {
		return nil, false
	}

	strOpFn := compareTFn[string](op)

	floatCompareFn := compareTFn[float64](op)
	intCompareFn := compareTFn[int64](op)
	uintCompareFn := compareTFn[uint64](op)
	numOpFn := numberDegradationFunc(op, func(left, right uint64) (bool, error) {
		return uintCompareFn(left, right), nil
	}, func(left, right int64) (bool, error) {
		return intCompareFn(left, right), nil
	}, func(left, right float64) (bool, error) {
		return floatCompareFn(left, right), nil
	})

	boolOpFn := compareBoolFn(op)
	genericOpFn := compareGenericFn(op)

	return func(lFn, rFn Function, left, right any) (bool, error) {
		switch lhs := value.RestrictForComparison(left).(type) {
		case string:
			if strOpFn == nil {
				return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := value.IGetString(right)
			if err != nil {
				if genericOpFn == nil {
					return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				return genericOpFn(lhs, value.RestrictForComparison(right)), nil
			}
			return strOpFn(lhs, rhs), nil

		case float64, int64, uint64:
			if numOpFn == nil {
				return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			b, err := numOpFn(lFn, rFn, left, right)
			if err != nil {
				if genericOpFn == nil {
					return false, err
				}
				return genericOpFn(lhs, value.RestrictForComparison(right)), nil
			}
			return b, nil

		case bool:
			if boolOpFn == nil {
				return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			rhs, err := value.IGetBool(right)
			if err != nil {
				if genericOpFn == nil {
					return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
				}
				return genericOpFn(lhs, value.RestrictForComparison(right)), nil
			}
			return boolOpFn(lhs, rhs), nil

		default:
			if genericOpFn == nil {
				return false, NewTypeMismatch(op.String(), lFn, rFn, left, right)
			}
			return genericOpFn(left, right), nil
		}
	}, true
}

func boolOr(lhs, rhs Function) Function {
	return ClosureFunction(rhs.Annotation(), func(ctx FunctionContext) (any, error) {
		lhsV, err := lhs.Exec(ctx)
		if err != nil {
			return nil, err
		}
		b, err := value.IGetBool(lhsV)
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
		if b, err = value.IGetBool(rhsV); err != nil {
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
		b, err := value.IGetBool(lhsV)
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
		if b, err = value.IGetBool(rhsV); err != nil {
			return nil, err
		}
		return b, nil
	}, aggregateTargetPaths(lhs, rhs))
}

func coalesce(lhs, rhs Function) Function {
	return ClosureFunction(rhs.Annotation(), func(ctx FunctionContext) (any, error) {
		lhsV, err := lhs.Exec(ctx)
		if err == nil && !value.IIsNull(lhsV) {
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
