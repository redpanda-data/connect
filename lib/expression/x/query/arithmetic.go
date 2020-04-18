package query

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/expression/x/parser"
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

func arithmeticOpParser() parser.Type {
	opParser := parser.AnyOf(
		parser.Char('+'),
		parser.Char('-'),
		parser.Char('/'),
		parser.Char('*'),
		parser.Match("&&"),
		parser.Match("||"),
		parser.Match("=="),
		parser.Match("!="),
		parser.Match(">="),
		parser.Match("<="),
		parser.Char('>'),
		parser.Char('<'),
		parser.Char('|'),
	)
	return func(input []rune) parser.Result {
		res := opParser(input)
		if res.Err != nil {
			return res
		}
		switch res.Result.(string) {
		case "+":
			res.Result = arithmeticAdd
		case "-":
			res.Result = arithmeticSub
		case "/":
			res.Result = arithmeticDiv
		case "*":
			res.Result = arithmeticMul
		case "==":
			res.Result = arithmeticEq
		case "!=":
			res.Result = arithmeticNeq
		case "&&":
			res.Result = arithmeticAnd
		case "||":
			res.Result = arithmeticOr
		case ">":
			res.Result = arithmeticGt
		case "<":
			res.Result = arithmeticLt
		case ">=":
			res.Result = arithmeticGte
		case "<=":
			res.Result = arithmeticLte
		case "|":
			res.Result = arithmeticPipe
		default:
			return parser.Result{
				Remaining: input,
				Err:       fmt.Errorf("operator not recognized: %v", res.Result),
			}
		}
		return res
	}
}

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
				nextF, tmpErr = iGetNumber(next)
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
			total, err = iGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var toSub float64
			if toSub, tmpErr = iGetNumber(rightV); tmpErr != nil {
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
			result, err = iGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var denom float64
			if denom, tmpErr = iGetNumber(rightV); tmpErr != nil {
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
			result, err = iGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			var denom float64
			if denom, tmpErr = iGetNumber(rightV); tmpErr != nil {
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
			lhsV, err = iGetNumber(leftV)
		} else {
			err = tmpErr
		}
		if rightV, tmpErr := rhs.Exec(ctx); tmpErr == nil {
			if rhsV, tmpErr = iGetNumber(rightV); tmpErr != nil {
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
		if err == nil && lhsV != nil {
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
		return nil, fmt.Errorf("mismatch of functions to arithmetic operators")
	}

	// First pass to resolve division and multiplication
	fnsNew, opsNew := []Function{fns[0]}, []arithmeticOp{}
	for i, op := range ops {
		switch op {
		case arithmeticMul:
			fnsNew[len(fnsNew)-1] = multiply(fnsNew[len(fnsNew)-1], fns[i+1])
		case arithmeticDiv:
			fnsNew[len(fnsNew)-1] = divide(fnsNew[len(fnsNew)-1], fns[i+1])
		default:
			fnsNew = append(fnsNew, fns[i+1])
			opsNew = append(opsNew, op)
		}
	}
	fns, ops = fnsNew, opsNew
	if len(fns) == 1 {
		return fns[0], nil
	}

	// Next, resolve additions and subtractions
	var addPile, subPile []Function
	addPile = append(addPile, fns[0])
	for i, op := range ops {
		switch op {
		case arithmeticAdd:
			addPile = append(addPile, fns[i+1])
		case arithmeticSub:
			subPile = append(subPile, fns[i+1])
		case arithmeticAnd,
			arithmeticOr:
			var rhs Function
			lhs, err := resolveArithmetic(fns[:i+1], ops[:i])
			if err == nil {
				rhs, err = resolveArithmetic(fns[i+1:], ops[i+1:])
			}
			if err != nil {
				return nil, err
			}
			return logicalBool(lhs, rhs, op)
		case arithmeticEq,
			arithmeticNeq:
			var rhs Function
			lhs, err := resolveArithmetic(fns[:i+1], ops[:i])
			if err == nil {
				rhs, err = resolveArithmetic(fns[i+1:], ops[i+1:])
			}
			if err != nil {
				return nil, err
			}
			return compareGeneric(lhs, rhs, op)
		case arithmeticGt,
			arithmeticGte,
			arithmeticLt,
			arithmeticLte:
			var rhs Function
			lhs, err := resolveArithmetic(fns[:i+1], ops[:i])
			if err == nil {
				rhs, err = resolveArithmetic(fns[i+1:], ops[i+1:])
			}
			if err != nil {
				return nil, err
			}
			return compareFloat(lhs, rhs, op)
		case arithmeticPipe:
			var rhs Function
			lhs, err := resolveArithmetic(fns[:i+1], ops[:i])
			if err == nil {
				rhs, err = resolveArithmetic(fns[i+1:], ops[i+1:])
			}
			if err != nil {
				return nil, err
			}
			return coalesce(lhs, rhs), nil
		}
	}

	fn := add(addPile)
	if len(subPile) > 0 {
		fn = sub(fn, add(subPile))
	}
	return fn, nil
}

//------------------------------------------------------------------------------
