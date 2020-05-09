package query

import (
	"errors"
	"fmt"

	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

type fromMethod struct {
	index  int
	target Function
}

func (f *fromMethod) Exec(ctx FunctionContext) (interface{}, error) {
	ctx.Index = f.index
	return f.target.Exec(ctx)
}

func (f *fromMethod) ToBytes(ctx FunctionContext) []byte {
	ctx.Index = f.index
	return f.target.ToBytes(ctx)
}

func (f *fromMethod) ToString(ctx FunctionContext) string {
	ctx.Index = f.index
	return f.target.ToString(ctx)
}

//------------------------------------------------------------------------------

func methodWithDynamicArgs(args []interface{}, target Function, ctor methodCtor) Function {
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
		dynFunc, err := ctor(target, dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	})
}

func enableMethodDynamicArgs(fn methodCtor) methodCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for _, arg := range args {
			if _, isDyn := arg.(Function); isDyn {
				return methodWithDynamicArgs(args, target, fn), nil
			}
		}
		return fn(target, args...)
	}
}

func checkMethodArgs(fn methodCtor, checks ...argCheckFn) methodCtor {
	return func(target Function, args ...interface{}) (Function, error) {
		for _, check := range checks {
			if err := check(args); err != nil {
				return nil, err
			}
		}
		return fn(target, args...)
	}
}

//------------------------------------------------------------------------------

func mapMethod(target Function, args ...interface{}) (Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected one argument, received: %v", len(args))
	}
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected function param, received %T", args[0])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		ctx.Value = &res
		return mapFn.Exec(ctx)
	}), nil
}

func forEachMethod(target Function, args ...interface{}) (Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected one argument, received: %v", len(args))
	}
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected function param, received %T", args[0])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		resSlice, ok := res.([]interface{})
		if !ok {
			return nil, &ErrRecoverable{
				Recovered: res,
				Err:       fmt.Errorf("expected array, found: %T", res),
			}
		}
		newSlice := make([]interface{}, 0, len(resSlice))
		for i, v := range resSlice {
			ctx.Value = &v
			var newV interface{}
			if newV, err = mapFn.Exec(ctx); err != nil {
				if recover, ok := err.(*ErrRecoverable); ok {
					newV = recover.Recovered
					err = xerrors.Errorf("failed to process element %v: %w", i, recover.Err)
				} else {
					return nil, err
				}
			}
			if _, isDelete := newV.(Delete); !isDelete {
				newSlice = append(newSlice, newV)
			}
		}
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: newSlice,
				Err:       err,
			}
		}
		return newSlice, nil
	}), nil
}

//------------------------------------------------------------------------------

type methodCtor func(target Function, args ...interface{}) (Function, error)

var methods = map[string]methodCtor{
	"apply": enableMethodDynamicArgs(checkMethodArgs(
		func(target Function, args ...interface{}) (Function, error) {
			targetMap := args[0].(string)
			return closureFn(func(ctx FunctionContext) (interface{}, error) {
				res, err := target.Exec(ctx)
				if err != nil {
					return nil, err
				}
				ctx.Value = &res

				if ctx.Maps == nil {
					return nil, &ErrRecoverable{
						Err:       errors.New("no maps were found"),
						Recovered: res,
					}
				}
				m, ok := ctx.Maps[targetMap]
				if !ok {
					return nil, &ErrRecoverable{
						Err:       fmt.Errorf("map %v was not found", targetMap),
						Recovered: res,
					}
				}

				// ISOLATED VARIABLES
				ctx.Vars = map[string]interface{}{}
				return m.Exec(ctx)
			}), nil
		},
		expectNArgs(1),
		expectStringArg(0),
	)),
	"catch": func(fn Function, args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("expected one argument, received: %v", len(args))
		}
		var catchFn Function
		switch t := args[0].(type) {
		case uint64, int64, float64, string, []byte, bool:
			catchFn = literalFunction(t)
		case Function:
			catchFn = t
		default:
			return nil, fmt.Errorf("expected function or literal param, received %T", args[0])
		}
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			res, err := fn.Exec(ctx)
			if err != nil {
				res, err = catchFn.Exec(ctx)
			}
			return res, err
		}), nil
	},
	"for_each": forEachMethod,
	"from": func(target Function, args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("expected one argument, received: %v", len(args))
		}
		i64, ok := args[0].(int64)
		if !ok {
			return nil, fmt.Errorf("expected int param, received %T", args[0])
		}
		return &fromMethod{
			index:  int(i64),
			target: target,
		}, nil
	},
	"from_all": func(target Function, _ ...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			values := make([]interface{}, ctx.Msg.Len())
			var err error
			for i := 0; i < ctx.Msg.Len(); i++ {
				subCtx := ctx
				subCtx.Index = i
				v, tmpErr := target.Exec(subCtx)
				if tmpErr != nil {
					if recovered, ok := tmpErr.(*ErrRecoverable); ok {
						values[i] = recovered.Recovered
					}
					err = tmpErr
				} else {
					values[i] = v
				}
			}
			if err != nil {
				return nil, &ErrRecoverable{
					Recovered: values,
					Err:       err,
				}
			}
			return values, nil
		}), nil
	},
	"map": mapMethod,
	"or": func(fn Function, args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("expected one argument, received: %v", len(args))
		}
		var orFn Function
		switch t := args[0].(type) {
		case uint64, int64, float64, string, []byte, bool:
			orFn = literalFunction(t)
		case Function:
			orFn = t
		default:
			return nil, fmt.Errorf("expected function or literal param, received %T", args[0])
		}
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			res, err := fn.Exec(ctx)
			if err != nil || IIsNull(res) {
				res, err = orFn.Exec(ctx)
			}
			return res, err
		}), nil
	},
	"sum": func(target Function, _ ...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			v, err := target.Exec(ctx)
			if err != nil {
				return nil, &ErrRecoverable{
					Recovered: int64(0),
					Err:       err,
				}
			}
			switch t := v.(type) {
			case float64:
				return t, nil
			case int64:
				return t, nil
			case uint64:
				return t, nil
			case []interface{}:
				var total float64
				for _, v := range t {
					n, nErr := IGetNumber(v)
					if nErr != nil {
						err = fmt.Errorf("unexpected type in array, expected number, found: %T", v)
					} else {
						total += n
					}
				}
				if err != nil {
					return nil, &ErrRecoverable{
						Recovered: total,
						Err:       err,
					}
				}
				return total, nil
			}
			return nil, &ErrRecoverable{
				Recovered: int64(0),
				Err:       fmt.Errorf("expected array value, received %T", v),
			}
		}), nil
	},
}

//------------------------------------------------------------------------------
