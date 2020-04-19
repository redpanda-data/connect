package query

import (
	"fmt"
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

//------------------------------------------------------------------------------

var methods = map[string]func(target Function, args ...interface{}) (Function, error){
	"map": mapMethod,
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
			if err != nil || res == nil {
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
					n, nErr := iGetNumber(v)
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
