package query

import (
	"fmt"
)

//------------------------------------------------------------------------------

type arithmeticOp int

const (
	arithmeticAdd arithmeticOp = iota
	arithmeticSub
	arithmeticDiv
	arithmeticMul
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
	case []byte:
		return string(t)
	}
	return v
}

func add(fns []Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var total float64
		var err error

		for _, fn := range fns {
			var nextF float64
			next, tmpErr := fn.Exec(ctx)
			if tmpErr == nil {
				nextF, tmpErr = IGetNumber(next)
			}
			if tmpErr != nil {
				err = tmpErr
				continue
			}
			total += nextF
		}

		if err != nil {
			return nil, &ErrRecoverable{
				Err:       err,
				Recovered: total,
			}
		}
		return total, nil
	})
}

func sub(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var total float64
		var err error

		if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
			total, err = IGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var toSub float64
			if toSub, tmpErr = IGetNumber(rightV); tmpErr != nil {
				err = tmpErr
			} else {
				total -= toSub
			}
		} else {
			err = tmpErr
		}

		if err != nil {
			return nil, &ErrRecoverable{
				Err:       err,
				Recovered: total,
			}
		}
		return total, nil
	})
}

func divide(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var result float64
		var err error

		if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
			result, err = IGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var denom float64
			if denom, tmpErr = IGetNumber(rightV); tmpErr != nil {
				err = tmpErr
			} else {
				result = result / denom
			}
		} else {
			err = tmpErr
		}

		if err != nil {
			return nil, err
		}
		return result, nil
	})
}

func multiply(lhs, rhs Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var result float64
		var err error

		if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
			result, err = IGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var denom float64
			if denom, tmpErr = IGetNumber(rightV); tmpErr != nil {
				err = tmpErr
			} else {
				result = result * denom
			}
		} else {
			err = tmpErr
		}

		if err != nil {
			return nil, err
		}
		return result, nil
	})
}

func compareFloat(lhs, rhs Function, op arithmeticOp) (Function, error) {
	var opFn func(lhs, rhs float64) bool
	switch op {
	case arithmeticEq:
		opFn = func(lhs, rhs float64) bool {
			return lhs == rhs
		}
	case arithmeticNeq:
		opFn = func(lhs, rhs float64) bool {
			return lhs != rhs
		}
	case arithmeticGt:
		opFn = func(lhs, rhs float64) bool {
			return lhs > rhs
		}
	case arithmeticGte:
		opFn = func(lhs, rhs float64) bool {
			return lhs >= rhs
		}
	case arithmeticLt:
		opFn = func(lhs, rhs float64) bool {
			return lhs < rhs
		}
	case arithmeticLte:
		opFn = func(lhs, rhs float64) bool {
			return lhs <= rhs
		}
	default:
		return nil, fmt.Errorf("operator not supported: %v", op)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var lhsV, rhsV float64
		var err error

		if leftV, tmpErr := lhs.Exec(ctx); tmpErr == nil {
			lhsV, err = IGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			if rhsV, tmpErr = IGetNumber(rightV); tmpErr != nil {
				err = tmpErr
			}
		} else {
			err = tmpErr
		}
		if err != nil {
			return nil, err
		}
		return opFn(lhsV, rhsV), nil
	}), nil
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

func compareGeneric(lhs, rhs Function, op arithmeticOp) (Function, error) {
	var opFn func(lhs, rhs interface{}) bool
	switch op {
	case arithmeticEq:
		opFn = func(lhs, rhs interface{}) bool {
			return lhs == rhs
		}
	case arithmeticNeq:
		opFn = func(lhs, rhs interface{}) bool {
			return lhs != rhs
		}
	default:
		return nil, fmt.Errorf("operator not supported: %v", op)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		var lhsV, rhsV interface{}
		var err error
		if lhsV, err = lhs.Exec(ctx); err == nil {
			rhsV, err = rhs.Exec(ctx)
		}
		if err != nil {
			return nil, err
		}
		lhsV = restrictForComparison(lhsV)
		rhsV = restrictForComparison(rhsV)
		return opFn(lhsV, rhsV), nil
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
			lhsV, _ = leftV.(bool)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			rhsV, _ = rightV.(bool)
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
			fnsNew[len(fnsNew)-1] = add([]Function{fnsNew[len(fnsNew)-1], fns[i+1]})
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
			arithmeticNeq:
			if fnsNew[len(fnsNew)-1], err = compareGeneric(fnsNew[len(fnsNew)-1], fns[i+1], op); err != nil {
				return nil, err
			}
		case arithmeticGt,
			arithmeticGte,
			arithmeticLt,
			arithmeticLte:
			if fnsNew[len(fnsNew)-1], err = compareFloat(fnsNew[len(fnsNew)-1], fns[i+1], op); err != nil {
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
