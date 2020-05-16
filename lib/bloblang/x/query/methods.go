package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"apply", true, applyMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func applyMethod(target Function, args ...interface{}) (Function, error) {
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
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"catch", false, catchMethod,
	ExpectNArgs(1),
)

func catchMethod(fn Function, args ...interface{}) (Function, error) {
	var catchFn Function
	switch t := args[0].(type) {
	case uint64, int64, float64, string, []byte, bool, []interface{}, map[string]interface{}:
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
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"contains", true, containsMethod,
	ExpectNArgs(1),
)

func containsMethod(target Function, args ...interface{}) (Function, error) {
	compareRight := args[0]
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		switch t := v.(type) {
		case []interface{}:
			for _, compareLeft := range t {
				if compareRight == compareLeft {
					return true, nil
				}
			}
		case map[string]interface{}:
			for _, compareLeft := range t {
				if compareRight == compareLeft {
					return true, nil
				}
			}
		default:
			return nil, &ErrRecoverable{
				Recovered: false,
				Err:       fmt.Errorf("expected map or array target, found %T", v),
			}
		}
		return false, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"exists", true, existsMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func existsMethod(target Function, args ...interface{}) (Function, error) {
	pathStr := args[0].(string)
	path := gabs.DotPathToSlice(pathStr)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		return gabs.Wrap(v).Exists(path...), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"format", true, formatMethod,
)

func formatMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		switch t := v.(type) {
		case string:
			return fmt.Sprintf(t, args...), nil
		case []byte:
			return fmt.Sprintf(string(t), args...), nil
		default:
			return nil, fmt.Errorf("expected string value, received %T", v)
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"from", false, func(target Function, args ...interface{}) (Function, error) {
		i64 := args[0].(int64)
		return &fromMethod{
			index:  int(i64),
			target: target,
		}, nil
	},
	ExpectNArgs(1),
	ExpectIntArg(0),
)

type fromMethod struct {
	index  int
	target Function
}

func (f *fromMethod) Exec(ctx FunctionContext) (interface{}, error) {
	ctx.Index = f.index
	return f.target.Exec(ctx)
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"from_all", false, fromAllMethod,
	ExpectNArgs(0),
)

func fromAllMethod(target Function, _ ...interface{}) (Function, error) {
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
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"get", true, getMethodCtor,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

type getMethod struct {
	fn   Function
	path []string
}

func (g *getMethod) Exec(ctx FunctionContext) (interface{}, error) {
	v, err := g.fn.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return gabs.Wrap(v).S(g.path...).Data(), nil
}

func getMethodCtor(target Function, args ...interface{}) (Function, error) {
	path := gabs.DotPathToSlice(args[0].(string))
	switch t := target.(type) {
	case *getMethod:
		t.path = append(t.path, path...)
		return t, nil
	case *fieldFunction:
		t.path = append(t.path, path...)
		return t, nil
	}
	return &getMethod{
		fn:   target,
		path: path,
	}, nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"length", false, lengthMethod,
	ExpectNArgs(0),
)

func lengthMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		var length int64
		switch t := v.(type) {
		case string:
			length = int64(len(t))
		case []byte:
			length = int64(len(t))
		case []interface{}:
			length = int64(len(t))
		case map[string]interface{}:
			length = int64(len(t))
		default:
			return nil, &ErrRecoverable{
				Recovered: length,
				Err:       fmt.Errorf("expected string, array or object value, received %T", v),
			}
		}
		return length, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"lowercase", false, lowercaseMethod,
	ExpectNArgs(0),
)

func lowercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		switch t := v.(type) {
		case string:
			return strings.ToLower(t), nil
		case []byte:
			return bytes.ToLower(t), nil
		default:
			return nil, &ErrRecoverable{
				Recovered: strings.ToLower(IToString(v)),
				Err:       fmt.Errorf("expected string value, received %T", v),
			}
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"map", false, mapMethod,
	ExpectNArgs(1),
)

func mapMethod(target Function, args ...interface{}) (Function, error) {
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

var _ = RegisterMethod(
	"map_each", false, mapEachMethod,
	ExpectNArgs(1),
)

func mapEachMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected function param, received %T", args[0])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		var resValue interface{}
		switch t := res.(type) {
		case []interface{}:
			newSlice := make([]interface{}, 0, len(t))
			for i, v := range t {
				ctx.Value = &v
				newV, mapErr := mapFn.Exec(ctx)
				if mapErr != nil {
					if recover, ok := mapErr.(*ErrRecoverable); ok {
						newV = recover.Recovered
						err = xerrors.Errorf("failed to process element %v: %w", i, recover.Err)
					} else {
						return nil, mapErr
					}
				}
				if _, isDelete := newV.(Delete); !isDelete {
					newSlice = append(newSlice, newV)
				}
			}
			resValue = newSlice
		case map[string]interface{}:
			newMap := make(map[string]interface{}, len(t))
			for k, v := range t {
				var ctxMap interface{} = map[string]interface{}{
					"key":   k,
					"value": v,
				}
				ctx.Value = &ctxMap
				newV, mapErr := mapFn.Exec(ctx)
				if mapErr != nil {
					if recover, ok := mapErr.(*ErrRecoverable); ok {
						newV = recover.Recovered
						err = xerrors.Errorf("failed to process element %v: %w", k, recover.Err)
					} else {
						return nil, mapErr
					}
				}
				if _, isDelete := newV.(Delete); !isDelete {
					newMap[k] = newV
				}
			}
			resValue = newMap
		default:
			return nil, &ErrRecoverable{
				Recovered: res,
				Err:       fmt.Errorf("expected array, found: %T", res),
			}
		}
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: resValue,
				Err:       err,
			}
		}
		return resValue, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"merge", false, mergeMethod,
	ExpectNArgs(1),
)

func mergeMethod(target Function, args ...interface{}) (Function, error) {
	var mapFn Function
	switch t := args[0].(type) {
	case Function:
		mapFn = t
	default:
		mapFn = literalFunction(t)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		mergeInto, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		mergeFrom, err := mapFn.Exec(ctx)
		if err != nil {
			return nil, err
		}

		if root, isArray := mergeInto.([]interface{}); isArray {
			if rhs, isAlsoArray := mergeFrom.([]interface{}); isAlsoArray {
				return append(root, rhs...), nil
			}
			return append(root, mergeFrom), nil
		}

		if _, isObject := mergeInto.(map[string]interface{}); !isObject {
			return nil, &ErrRecoverable{
				Recovered: mergeInto,
				Err:       fmt.Errorf("expected object or array target, received %T", mergeInto),
			}
		}

		root := gabs.New()
		if err = root.Merge(gabs.Wrap(mergeInto)); err == nil {
			err = root.Merge(gabs.Wrap(mergeFrom))
		}
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: mergeInto,
				Err:       err,
			}
		}
		return root.Data(), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"or", false, orMethod,
	ExpectNArgs(1),
)

func orMethod(fn Function, args ...interface{}) (Function, error) {
	var orFn Function
	switch t := args[0].(type) {
	case uint64, int64, float64, string, []byte, bool, []interface{}, map[string]interface{}:
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
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"string", false, stringMethod,
	ExpectNArgs(0),
)

func stringMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		return IToString(v), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"substr", true, substrMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func substrMethod(target Function, args ...interface{}) (Function, error) {
	sub := args[0].(string)
	bsub := []byte(sub)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		switch t := v.(type) {
		case string:
			return strings.Contains(t, sub), nil
		case []byte:
			return bytes.Contains(t, bsub), nil
		default:
			return nil, &ErrRecoverable{
				Recovered: false,
				Err:       fmt.Errorf("expected string target, found %T", v),
			}
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"uppercase", false, uppercaseMethod,
	ExpectNArgs(0),
)

func uppercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		switch t := v.(type) {
		case string:
			return strings.ToUpper(t), nil
		case []byte:
			return bytes.ToUpper(t), nil
		default:
			return nil, &ErrRecoverable{
				Recovered: strings.ToUpper(IToString(v)),
				Err:       fmt.Errorf("expected string value, received %T", v),
			}
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"number", false, numberMethod,
	ExpectNArgs(0),
)

func numberMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: 0,
				Err:       err,
			}
		}
		f, err := IGetNumber(v)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: float64(0),
				Err:       err,
			}
		}
		return f, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"sum", false, sumMethod,
	ExpectNArgs(0),
)

func sumMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: int64(0),
				Err:       err,
			}
		}
		switch t := v.(type) {
		case float64, int64, uint64:
			return v, nil
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
}

//------------------------------------------------------------------------------
