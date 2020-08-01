package query

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"append", true, appendMethod,
	ExpectAtLeastOneArg(),
)

func appendMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		arr, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}
		return append(arr, args...), nil
	}), nil
}

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
	"bool", true, boolMethod,
	ExpectOneOrZeroArgs(),
	ExpectBoolArg(0),
)

func boolMethod(target Function, args ...interface{}) (Function, error) {
	defaultBool := false
	if len(args) > 0 {
		defaultBool = args[0].(bool)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			if len(args) > 0 {
				return defaultBool, nil
			}
			return nil, &ErrRecoverable{
				Recovered: defaultBool,
				Err:       err,
			}
		}
		f, err := IToBool(v)
		if err != nil {
			if len(args) > 0 {
				return defaultBool, nil
			}
			return nil, &ErrRecoverable{
				Recovered: defaultBool,
				Err:       err,
			}
		}
		return f, nil
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
	"collapse", false, collapseMethod,
	ExpectNArgs(0),
)

func collapseMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		return gabs.Wrap(v).Flatten()
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"contains", true, containsMethod,
	ExpectNArgs(1),
)

func containsMethod(target Function, args ...interface{}) (Function, error) {
	compareRight := args[0]
	sub := IToString(args[0])
	bsub := IToBytes(args[0])
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
				Err:       NewTypeError(v, ValueString, ValueArray, ValueObject),
			}
		}
		return false, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"enumerated", false, enumerateMethod,
	ExpectNArgs(0),
)

func enumerateMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		arr, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}
		enumerated := make([]interface{}, 0, len(arr))
		for i, ele := range arr {
			enumerated = append(enumerated, map[string]interface{}{
				"index": int64(i),
				"value": ele,
			})
		}
		return enumerated, nil
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
	"explode", true, explodeMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func explodeMethod(target Function, args ...interface{}) (Function, error) {
	path := gabs.DotPathToSlice(args[0].(string))

	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		target := gabs.Wrap(v).Search(path...)

		switch t := target.Data().(type) {
		case []interface{}:
			result := make([]interface{}, len(t))
			for i, ele := range t {
				gExploded := gabs.Wrap(IClone(v))
				gExploded.Set(ele, path...)
				result[i] = gExploded.Data()
			}
			return result, nil
		case map[string]interface{}:
			result := make(map[string]interface{}, len(t))
			for key, ele := range t {
				gExploded := gabs.Wrap(IClone(v))
				gExploded.Set(ele, path...)
				result[key] = gExploded.Data()
			}
			return result, nil
		}

		return nil, NewTypeError(v, ValueObject, ValueArray)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"flatten", false, flattenMethod,
	ExpectNArgs(0),
)

func flattenMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		array, isArray := v.([]interface{})
		if !isArray {
			return nil, NewTypeError(v, ValueArray)
		}
		result := make([]interface{}, 0, len(array))
		for _, child := range array {
			switch t := child.(type) {
			case []interface{}:
				result = append(result, t...)
			default:
				result = append(result, t)
			}
		}
		return result, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"fold", false, foldMethod,
	ExpectNArgs(2),
)

func foldMethod(target Function, args ...interface{}) (Function, error) {
	var foldTallyStart interface{}
	switch t := args[0].(type) {
	case *literal:
		foldTallyStart = t.Value
	default:
		foldTallyStart = t
	}
	foldFn, ok := args[1].(Function)
	if !ok {
		return nil, fmt.Errorf("expected function param, received %T", args[1])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		resArray, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}

		var tally interface{}
		switch t := foldTallyStart.(type) {
		case Function:
			if tally, err = t.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to extract tally initial value: %w", err)
			}
		default:
			tally = IClone(foldTallyStart)
		}

		tmpObj := map[string]interface{}{
			"tally": struct{}{},
			"value": struct{}{},
		}

		for _, v := range resArray {
			tmpObj["tally"] = tally
			tmpObj["value"] = v

			var tmpVal interface{} = tmpObj
			ctx.Value = &tmpVal

			newV, mapErr := foldFn.Exec(ctx)
			if mapErr != nil {
				return nil, mapErr
			}

			tally = newV
		}
		return tally, nil
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
		values := make([]interface{}, ctx.MsgBatch.Len())
		var err error
		for i := 0; i < ctx.MsgBatch.Len(); i++ {
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
		newPath := append([]string{}, t.path...)
		newPath = append(newPath, path...)
		return &getMethod{
			fn:   t.fn,
			path: newPath,
		}, nil
	case *fieldFunction:
		newPath := append([]string{}, t.path...)
		newPath = append(newPath, path...)
		return &fieldFunction{
			path: newPath,
		}, nil
	}
	return &getMethod{
		fn:   target,
		path: path,
	}, nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"index", true, indexMethod,
	ExpectNArgs(1),
	ExpectIntArg(0),
)

func indexMethod(target Function, args ...interface{}) (Function, error) {
	index := args[0].(int64)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		array, ok := v.([]interface{})
		if !ok {
			return nil, NewTypeError(v, ValueArray)
		}

		i := int(index)
		if i < 0 {
			i = len(array) + i
		}
		if i < 0 || i >= len(array) {
			return nil, fmt.Errorf("index '%v' was out of bounds for array size: %v", i, len(array))
		}
		return array[i], nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"keys", false, keysMethod,
	ExpectNArgs(0),
)

func keysMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if m, ok := v.(map[string]interface{}); ok {
			keys := make([]interface{}, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			return keys, nil
		}
		return nil, NewTypeError(v, ValueObject)
	}), nil
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
				Err:       NewTypeError(v, ValueString, ValueArray, ValueObject),
			}
		}
		return length, nil
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
				switch newV.(type) {
				case Delete:
				case Nothing:
					newSlice = append(newSlice, v)
				default:
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
				switch newV.(type) {
				case Delete:
				case Nothing:
					newMap[k] = v
				default:
					newMap[k] = newV
				}
			}
			resValue = newMap
		default:
			return nil, &ErrRecoverable{
				Recovered: res,
				Err:       NewTypeError(res, ValueArray),
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
				Err:       NewTypeError(mergeInto, ValueObject, ValueArray),
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
	"not", false, notMethodCtor,
	ExpectNArgs(0),
)

type notMethod struct {
	fn Function
}

func (n *notMethod) Exec(ctx FunctionContext) (interface{}, error) {
	v, err := n.fn.Exec(ctx)
	if err != nil {
		return nil, err
	}
	b, ok := v.(bool)
	if !ok {
		return nil, NewTypeError(v, ValueBool)
	}
	return !b, nil
}

func notMethodCtor(target Function, _ ...interface{}) (Function, error) {
	return &notMethod{fn: target}, nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"number", true, numberMethod,
	ExpectOneOrZeroArgs(),
	ExpectFloatArg(0),
)

func numberMethod(target Function, args ...interface{}) (Function, error) {
	defaultNum := 0.0
	if len(args) > 0 {
		defaultNum = args[0].(float64)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			if len(args) > 0 {
				return defaultNum, nil
			}
			return nil, &ErrRecoverable{
				Recovered: defaultNum,
				Err:       err,
			}
		}
		f, err := IToNumber(v)
		if err != nil {
			if len(args) > 0 {
				return defaultNum, nil
			}
			return nil, &ErrRecoverable{
				Recovered: defaultNum,
				Err:       err,
			}
		}
		return f, nil
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
	"sort", false, sortMethod,
	ExpectOneOrZeroArgs(),
)

func sortMethod(target Function, args ...interface{}) (Function, error) {
	compareFn := func(ctx FunctionContext, values []interface{}, i, j int) bool {
		switch values[i].(type) {
		case float64, int64, uint64:
			var lhs, rhs float64
			var err error
			if lhs, err = IGetNumber(values[i]); err == nil {
				rhs, err = IGetNumber(values[j])
			}
			if err != nil {
				return false
			}
			return lhs < rhs
		case string, []byte:
			var lhs, rhs string
			var err error
			if lhs, err = IGetString(values[i]); err == nil {
				rhs, err = IGetString(values[j])
			}
			if err != nil {
				return false
			}
			return lhs < rhs
		}
		return false
	}
	if len(args) > 0 {
		mapFn, ok := args[0].(Function)
		if !ok {
			return nil, fmt.Errorf("expected function param, received %T", args[0])
		}
		compareFn = func(ctx FunctionContext, values []interface{}, i, j int) bool {
			var ctxValue interface{} = map[string]interface{}{
				"left":  values[i],
				"right": values[j],
			}
			ctx.Value = &ctxValue
			v, err := mapFn.Exec(ctx)
			if err != nil {
				return false
			}
			b, _ := v.(bool)
			return b
		}
	}

	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if m, ok := v.([]interface{}); ok {
			values := make([]interface{}, 0, len(m))
			for _, e := range m {
				values = append(values, e)
			}
			sort.Slice(values, func(i, j int) bool {
				return compareFn(ctx, values, i, j)
			})
			return values, nil
		}
		return nil, NewTypeError(v, ValueArray)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"slice", true, sliceMethod,
	ExpectAtLeastOneArg(),
	ExpectIntArg(0),
	ExpectIntArg(1),
)

func sliceMethod(target Function, args ...interface{}) (Function, error) {
	start := args[0].(int64)
	var end *int64
	if len(args) > 1 {
		endV := args[1].(int64)
		end = &endV
		if endV > 0 && start >= endV {
			return nil, fmt.Errorf("lower slice bound %v must be lower than upper (%v)", start, endV)
		}
	}
	getBounds := func(l int64) (startV, endV int64, err error) {
		endV = l
		if end != nil {
			if *end < 0 {
				endV = endV + *end
			} else {
				endV = *end
			}
		}
		if endV > l {
			endV = l
		}
		if endV < 0 {
			endV = 0
		}
		startV = start
		if startV < 0 {
			startV = l + startV
			if startV < 0 {
				startV = 0
			}
		}
		if startV > endV {
			err = fmt.Errorf("lower slice bound %v must be lower than or equal to upper bound (%v) and target length (%v)", startV, endV, l)
		}
		return
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		switch t := v.(type) {
		case string:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		case []byte:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		case []interface{}:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		}
		return nil, NewTypeError(v, ValueArray, ValueString)
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
			for i, v := range t {
				n, nErr := IGetNumber(v)
				if nErr != nil {
					err = fmt.Errorf("index %v: %w", i, nErr)
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
			Err:       NewTypeError(v, ValueArray),
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"type", false, typeMethod,
	ExpectNArgs(0),
)

func typeMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		return string(ITypeOf(v)), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"unique", false, uniqueMethod,
	ExpectOneOrZeroArgs(),
)

func uniqueMethod(target Function, args ...interface{}) (Function, error) {
	var emitFn Function
	if len(args) > 0 {
		var ok bool
		emitFn, ok = args[0].(Function)
		if !ok {
			return nil, fmt.Errorf("expected function param, received %T", args[0])
		}
	}

	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		slice, ok := v.([]interface{})
		if !ok {
			return nil, NewTypeError(v, ValueArray)
		}

		var strCompares map[string]struct{}
		var numCompares map[float64]struct{}

		checkStr := func(str string) bool {
			if strCompares == nil {
				strCompares = make(map[string]struct{}, len(slice))
			}
			_, exists := strCompares[str]
			if !exists {
				strCompares[str] = struct{}{}
			}
			return !exists
		}

		checkNum := func(num float64) bool {
			if numCompares == nil {
				numCompares = make(map[float64]struct{}, len(slice))
			}
			_, exists := numCompares[num]
			if !exists {
				numCompares[num] = struct{}{}
			}
			return !exists
		}

		uniqueSlice := make([]interface{}, 0, len(slice))
		for i, v := range slice {
			check := v
			if emitFn != nil {
				ctx.Value = &v
				var err error
				if check, err = emitFn.Exec(ctx); err != nil {
					return nil, fmt.Errorf("index %v: %w", i, err)
				}
			}
			var unique bool
			switch t := check.(type) {
			case string:
				unique = checkStr(t)
			case []byte:
				unique = checkStr(string(t))
			case int64:
				unique = checkNum(float64(t))
			case uint64:
				unique = checkNum(float64(t))
			case float64:
				unique = checkNum(float64(t))
			default:
				return nil, fmt.Errorf("index %v: %w", i, NewTypeError(check, ValueString, ValueNumber))
			}
			if unique {
				uniqueSlice = append(uniqueSlice, v)
			}
		}
		return uniqueSlice, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"values", false, valuesMethod,
	ExpectNArgs(0),
)

func valuesMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if m, ok := v.(map[string]interface{}); ok {
			values := make([]interface{}, 0, len(m))
			for _, e := range m {
				values = append(values, e)
			}
			return values, nil
		}
		return nil, NewTypeError(v, ValueObject)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"without", true, withoutMethod,
	ExpectAtLeastOneArg(),
	ExpectAllStringArgs(),
)

func mapWithout(m map[string]interface{}, paths [][]string) map[string]interface{} {
	newMap := make(map[string]interface{}, len(m))
	for k, v := range m {
		excluded := false
		var nestedExclude [][]string
		for _, p := range paths {
			if p[0] == k {
				if len(p) > 1 {
					nestedExclude = append(nestedExclude, p[1:])
				} else {
					excluded = true
				}
			}
		}
		if !excluded {
			if len(nestedExclude) > 0 {
				vMap, ok := v.(map[string]interface{})
				if ok {
					newMap[k] = mapWithout(vMap, nestedExclude)
				} else {
					newMap[k] = v
				}
			} else {
				newMap[k] = v
			}
		}
	}
	return newMap
}

func withoutMethod(target Function, args ...interface{}) (Function, error) {
	excludeList := make([][]string, 0, len(args))
	for _, arg := range args {
		excludeList = append(excludeList, gabs.DotPathToSlice(arg.(string)))
	}

	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, NewTypeError(v, ValueObject)
		}
		return mapWithout(m, excludeList), nil
	}), nil
}

//------------------------------------------------------------------------------
