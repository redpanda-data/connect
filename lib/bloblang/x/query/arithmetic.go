package query

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
)

//------------------------------------------------------------------------------

type arithmeticOp int

const (
	arithmeticAdd arithmeticOp = iota
	arithmeticSub
	arithmeticDiv
	arithmeticMul
	arithmeticMod
	arithmeticEq
	arithmeticNeq
	arithmeticGt
	arithmeticLt
	arithmeticGte
	arithmeticLte
	arithmeticAnd
	arithmeticOr
	arithmeticPipe
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
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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

		return lhs + rhs, nil
	})
}

func sub(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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

func divide(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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

		return lhs / rhs, nil
	})
}

func multiply(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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

		return lhs % rhs, nil
	})
}

func coalesce(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		lhsV, err := lhs.Exec(ctx)
		if err == nil && !IIsNull(lhsV) {
			return lhsV, nil
		}
		return rhs.Exec(ctx)
	})
}

func compareNumFn(op arithmeticOp) func(lhs, rhs float64) bool {
	switch op {
	case arithmeticEq:
		return func(lhs, rhs float64) bool {
			return lhs == rhs
		}
	case arithmeticNeq:
		return func(lhs, rhs float64) bool {
			return lhs != rhs
		}
	case arithmeticGt:
		return func(lhs, rhs float64) bool {
			return lhs > rhs
		}
	case arithmeticGte:
		return func(lhs, rhs float64) bool {
			return lhs >= rhs
		}
	case arithmeticLt:
		return func(lhs, rhs float64) bool {
			return lhs < rhs
		}
	case arithmeticLte:
		return func(lhs, rhs float64) bool {
			return lhs <= rhs
		}
	}
	return nil
}

func compareStrFn(op arithmeticOp) func(lhs, rhs string) bool {
	switch op {
	case arithmeticEq:
		return func(lhs, rhs string) bool {
			return lhs == rhs
		}
	case arithmeticNeq:
		return func(lhs, rhs string) bool {
			return lhs != rhs
		}
	case arithmeticGt:
		return func(lhs, rhs string) bool {
			return lhs > rhs
		}
	case arithmeticGte:
		return func(lhs, rhs string) bool {
			return lhs >= rhs
		}
	case arithmeticLt:
		return func(lhs, rhs string) bool {
			return lhs < rhs
		}
	case arithmeticLte:
		return func(lhs, rhs string) bool {
			return lhs <= rhs
		}
	}
	return nil
}

func compareBoolFn(op arithmeticOp) func(lhs, rhs bool) bool {
	switch op {
	case arithmeticEq:
		return func(lhs, rhs bool) bool {
			return lhs == rhs
		}
	case arithmeticNeq:
		return func(lhs, rhs bool) bool {
			return lhs != rhs
		}
	}
	return nil
}

func compareGenericFn(op arithmeticOp) func(lhs, rhs interface{}) bool {
	switch op {
	case arithmeticEq:
		return func(lhs, rhs interface{}) bool {
			return cmp.Equal(lhs, rhs)
		}
	case arithmeticNeq:
		return func(lhs, rhs interface{}) bool {
			return !cmp.Equal(lhs, rhs)
		}
	}
	return nil
}

func compare(lhs, rhs Function, op arithmeticOp) (Function, error) {
	strOpFn := compareStrFn(op)
	numOpFn := compareNumFn(op)
	boolOpFn := compareBoolFn(op)
	genericOpFn := compareGenericFn(op)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
				return nil, err
			}
			return strOpFn(lhs, rhs), nil
		case float64:
			if numOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			rhs, err := IGetNumber(rhsV)
			if err != nil {
				return nil, err
			}
			return numOpFn(lhs, rhs), nil
		case bool:
			if boolOpFn == nil {
				return nil, NewTypeError(lhsV)
			}
			rhs, err := IGetBool(rhsV)
			if err != nil {
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

func logicalBool(lhs, rhs Function, op arithmeticOp) (Function, error) {
	var opFn func(lhs, rhs bool) bool
	switch op {
	case arithmeticAnd:
		opFn = func(lhs, rhs bool) bool {
			return lhs && rhs
		}
	case arithmeticOr:
		opFn = func(lhs, rhs bool) bool {
			return lhs || rhs
		}
	default:
		return nil, fmt.Errorf("operator not supported: %v", op)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var lhsV, rhsV bool
		var err error

		if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
			lhsV, err = IGetBool(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); err == nil && tmpErr == nil {
			rhsV, err = IGetBool(rightV)
		} else {
			err = tmpErr
		}
		if err != nil {
			return nil, err
		}
		return opFn(lhsV, rhsV), nil
	}), nil
}

func resolveArithmetic(fns []Function, ops []arithmeticOp) (Function, error) {
	if len(fns) == 1 && len(ops) == 0 {
		return fns[0], nil
	}
	if len(fns) != (len(ops) + 1) {
		return nil, fmt.Errorf("mismatch of functions (%v) to arithmetic operators (%v)", len(fns), len(ops))
	}

	// First pass to resolve division, multiplication and coalesce
	fnsNew, opsNew := []Function{fns[0]}, []arithmeticOp{}
	for i, op := range ops {
		switch op {
		case arithmeticMul:
			fnsNew[len(fnsNew)-1] = multiply(fnsNew[len(fnsNew)-1], fns[i+1])
		case arithmeticDiv:
			fnsNew[len(fnsNew)-1] = divide(fnsNew[len(fnsNew)-1], fns[i+1])
		case arithmeticMod:
			fnsNew[len(fnsNew)-1] = modulo(fnsNew[len(fnsNew)-1], fns[i+1])
		case arithmeticPipe:
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
	fnsNew, opsNew = []Function{fns[0]}, []arithmeticOp{}
	for i, op := range ops {
		switch op {
		case arithmeticAdd:
			fnsNew[len(fnsNew)-1] = add(fnsNew[len(fnsNew)-1], fns[i+1])
		case arithmeticSub:
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
	fnsNew, opsNew = []Function{fns[0]}, []arithmeticOp{}
	for i, op := range ops {
		switch op {
		case arithmeticEq,
			arithmeticNeq,
			arithmeticGt,
			arithmeticGte,
			arithmeticLt,
			arithmeticLte:
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
	fnsNew, opsNew = []Function{fns[0]}, []arithmeticOp{}
	for i, op := range ops {
		switch op {
		case arithmeticAnd,
			arithmeticOr:
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
