package query

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/gabs/v2"
	jsonschema "github.com/xeipuuv/gojsonschema"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"all",
		"Checks each element of an array against a query and returns true if all elements passed. An error occurs if the target is not an array, or if any element results in the provided query returning a non-boolean result. Returns false if the target array is empty.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.all_over_21 = this.patrons.all(this.age >= 21)`,
			`{"patrons":[{"id":"1","age":18},{"id":"2","age":23}]}`,
			`{"all_over_21":false}`,
			`{"patrons":[{"id":"1","age":45},{"id":"2","age":23}]}`,
			`{"all_over_21":true}`,
		),
	),
	false, allMethod,
	ExpectNArgs(1),
	ExpectFunctionArg(0),
)

func allMethod(target Function, args ...interface{}) (Function, error) {
	queryFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}

	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		arr, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}

		if len(arr) == 0 {
			return false, nil
		}

		for i, v := range arr {
			vCtx := ctx.WithValue(v)
			res, err := queryFn.Exec(vCtx)
			if err != nil {
				return nil, fmt.Errorf("element %v: %w", i, err)
			}
			b, ok := res.(bool)
			if !ok {
				return nil, fmt.Errorf("element %v: %w", i, NewTypeError(res, ValueBool))
			}
			if !b {
				return false, nil
			}
		}

		return true, nil
	}), nil
}

var _ = RegisterMethod(
	NewMethodSpec(
		"any",
		"Checks the elements of an array against a query and returns true if any element passes. An error occurs if the target is not an array, or if an element results in the provided query returning a non-boolean result. Returns false if the target array is empty.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.any_over_21 = this.patrons.any(this.age >= 21)`,
			`{"patrons":[{"id":"1","age":18},{"id":"2","age":23}]}`,
			`{"any_over_21":true}`,
			`{"patrons":[{"id":"1","age":10},{"id":"2","age":12}]}`,
			`{"any_over_21":false}`,
		),
	),
	false, anyMethod,
	ExpectNArgs(1),
	ExpectFunctionArg(0),
)

func anyMethod(target Function, args ...interface{}) (Function, error) {
	queryFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}

	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		arr, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}

		if len(arr) == 0 {
			return false, nil
		}

		for i, v := range arr {
			vCtx := ctx.WithValue(v)
			res, err := queryFn.Exec(vCtx)
			if err != nil {
				return nil, fmt.Errorf("element %v: %w", i, err)
			}
			b, ok := res.(bool)
			if !ok {
				return nil, fmt.Errorf("element %v: %w", i, NewTypeError(res, ValueBool))
			}
			if b {
				return true, nil
			}
		}

		return false, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"append",
		"Returns an array with new elements appended to the end.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.foo = this.foo.append("and", "this")`,
			`{"foo":["bar","baz"]}`,
			`{"foo":["bar","baz","and","this"]}`,
		),
	),
	true, appendMethod,
	ExpectAtLeastOneArg(),
)

func appendMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		arr, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}
		copied := make([]interface{}, 0, len(arr)+len(args))
		copied = append(copied, arr...)
		return append(copied, args...), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"apply",
		"Apply a declared map on a value.",
		NewExampleSpec("",
			`map thing {
  root.inner = this.first
}

root.foo = this.doc.apply("thing")`,
			`{"doc":{"first":"hello world"}}`,
			`{"foo":{"inner":"hello world"}}`,
		),
		NewExampleSpec("",
			`map create_foo {
  root.name = "a foo"
  root.purpose = "to be a foo"
}

root = this
root.foo = null.apply("create_foo")`,
			`{"id":"1234"}`,
			`{"foo":{"name":"a foo","purpose":"to be a foo"},"id":"1234"}`,
		),
	),
	true, applyMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func applyMethod(target Function, args ...interface{}) (Function, error) {
	targetMap := args[0].(string)

	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		ctx = ctx.WithValue(res)

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
	}, func(ctx TargetsContext) []TargetPath {
		m, ok := ctx.Maps[targetMap]
		if !ok {
			return target.QueryTargets(ctx)
		}
		return expandTargetPaths(target.QueryTargets(ctx), m.QueryTargets(ctx))
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec("bool", "").InCategory(
		MethodCategoryCoercion,
		"Attempt to parse a value into a boolean. An optional argument can be provided, in which case if the value cannot be parsed the argument will be returned instead.",
		NewExampleSpec("",
			`root.foo = this.thing.bool()
root.bar = this.thing.bool(true)`,
		),
	),
	true, boolMethod,
	ExpectOneOrZeroArgs(),
	ExpectBoolArg(0),
)

func boolMethod(target Function, args ...interface{}) (Function, error) {
	defaultBool := false
	if len(args) > 0 {
		defaultBool = args[0].(bool)
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"catch",
		"If the result of a target query fails (due to incorrect types, failed parsing, etc) the argument is returned instead.",
		NewExampleSpec("",
			`root.doc.id = this.thing.id.string().catch(uuid_v4())`,
		),
	),
	false, catchMethod,
	ExpectNArgs(1),
)

func catchMethod(fn Function, args ...interface{}) (Function, error) {
	var catchFn Function
	switch t := args[0].(type) {
	case uint64, int64, float64, string, []byte, bool, []interface{}, map[string]interface{}:
		catchFn = NewLiteralFunction(t)
	case Function:
		catchFn = t
	default:
		return nil, fmt.Errorf("expected query or literal argument, received %T", args[0])
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		res, err := fn.Exec(ctx)
		if err != nil {
			res, err = catchFn.Exec(ctx)
		}
		return res, err
	}, aggregateTargetPaths(fn, catchFn)), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"collapse", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Collapse an array or object into an object of key/value pairs for each field, where the key is the full path of the structured field in dot path notation. Empty arrays an objects are ignored by default.",
		NewExampleSpec("",
			`root.result = this.collapse()`,
			`{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`,
			`{"result":{"foo.0.bar":"1","foo.2.bar":"2"}}`,
		),
		NewExampleSpec(
			"An optional boolean parameter can be set to true in order to include empty objects and arrays.",
			`root.result = this.collapse(true)`,
			`{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`,
			`{"result":{"foo.0.bar":"1","foo.1.bar":{},"foo.2.bar":"2","foo.3.bar":[]}}`,
		),
	),
	true, collapseMethod,
	ExpectOneOrZeroArgs(),
	ExpectBoolArg(0),
)

func collapseMethod(target Function, args ...interface{}) (Function, error) {
	includeEmpty := false
	if len(args) > 0 {
		includeEmpty = args[0].(bool)
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		gObj := gabs.Wrap(v)
		if includeEmpty {
			return gObj.FlattenIncludeEmpty()
		}
		return gObj.Flatten()
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"contains", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Checks whether an array contains an element matching the argument, or an object contains a value matching the argument, and returns a boolean result.",
		NewExampleSpec("",
			`root.has_foo = this.thing.contains("foo")`,
			`{"thing":["this","foo","that"]}`,
			`{"has_foo":true}`,
			`{"thing":["this","bar","that"]}`,
			`{"has_foo":false}`,
		),
	).InCategory(
		MethodCategoryStrings,
		"Checks whether a string contains a substring and returns a boolean result.",
		NewExampleSpec("",
			`root.has_foo = this.thing.contains("foo")`,
			`{"thing":"this foo that"}`,
			`{"has_foo":true}`,
			`{"thing":"this bar that"}`,
			`{"has_foo":false}`,
		),
	),
	true, containsMethod,
	ExpectNArgs(1),
)

func containsMethod(target Function, args ...interface{}) (Function, error) {
	compareRight := args[0]
	sub := IToString(args[0])
	bsub := IToBytes(args[0])
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"enumerated",
		"Converts an array into a new array of objects, where each object has a field index containing the `index` of the element and a field `value` containing the original value of the element.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.foo = this.foo.enumerated()`,
			`{"foo":["bar","baz"]}`,
			`{"foo":[{"index":0,"value":"bar"},{"index":1,"value":"baz"}]}`,
		),
	),
	false, enumerateMethod,
	ExpectNArgs(0),
)

func enumerateMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"exists",
		"Checks that a field, identified via a [dot path][field_paths], exists in an object.",
		NewExampleSpec("",
			`root.result = this.foo.exists("bar.baz")`,
			`{"foo":{"bar":{"baz":"yep, I exist"}}}`,
			`{"result":true}`,
			`{"foo":{"bar":{}}}`,
			`{"result":false}`,
			`{"foo":{}}`,
			`{"result":false}`,
		),
	),
	true, existsMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func existsMethod(target Function, args ...interface{}) (Function, error) {
	pathStr := args[0].(string)
	path := gabs.DotPathToSlice(pathStr)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		return gabs.Wrap(v).Exists(path...), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"explode", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Explodes an array or object at a [field path][field_paths].",
		NewExampleSpec(`#### On arrays

Exploding arrays results in an array containing elements matching the original document, where the target field of each element is an element of the exploded array:`,
			`root = this.explode("value")`,
			`{"id":1,"value":["foo","bar","baz"]}`,
			`[{"id":1,"value":"foo"},{"id":1,"value":"bar"},{"id":1,"value":"baz"}]`,
		),
		NewExampleSpec(`#### On objects

Exploding objects results in an object where the keys match the target object, and the values match the original document but with the target field replaced by the exploded value:`,
			`root = this.explode("value")`,
			`{"id":1,"value":{"foo":2,"bar":[3,4],"baz":{"bev":5}}}`,
			`{"bar":{"id":1,"value":[3,4]},"baz":{"id":1,"value":{"bev":5}},"foo":{"id":1,"value":2}}`,
		),
	),
	true, explodeMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func explodeMethod(target Function, args ...interface{}) (Function, error) {
	path := gabs.DotPathToSlice(args[0].(string))

	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"filter", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Executes a mapping query argument for each element of an array or key/value pair of an object, and unless the mapping returns `true` the item is removed from the resulting array or object.",
		NewExampleSpec(``,
			`root.new_nums = this.nums.filter(this > 10)`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[11,17]}`,
		),
		NewExampleSpec(`#### On objects

When filtering objects the mapping query argument is provided a context with a field `+"`key`"+` containing the value key, and a field `+"`value`"+` containing the value.`,
			`root.new_dict = this.dict.filter(this.value.contains("foo"))`,
			`{"dict":{"first":"hello foo","second":"world","third":"this foo is great"}}`,
			`{"new_dict":{"first":"hello foo","third":"this foo is great"}}`,
		),
	),
	false, filterMethod,
	ExpectNArgs(1),
	ExpectFunctionArg(0),
)

func filterMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}

	// TODO: Query targets do not take the mapping function into account as it's
	// dynamic.
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		var resValue interface{}
		switch t := res.(type) {
		case []interface{}:
			newSlice := make([]interface{}, 0, len(t))
			for _, v := range t {
				f, err := mapFn.Exec(ctx.WithValue(v))
				if err != nil {
					return nil, err
				}
				if b, _ := f.(bool); b {
					newSlice = append(newSlice, v)
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
				f, err := mapFn.Exec(ctx.WithValue(ctxMap))
				if err != nil {
					return nil, err
				}
				if b, _ := f.(bool); b {
					newMap[k] = v
				}
			}
			resValue = newMap
		default:
			return nil, NewTypeError(res, ValueArray, ValueObject)
		}
		return resValue, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"flatten",
		"Iterates an array and any element that is itself an array is removed and has its elements inserted directly in the resulting array.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root.result = this.flatten()`,
			`["foo",["bar","baz"],"buz"]`,
			`{"result":["foo","bar","baz","buz"]}`,
		),
	),
	false, flattenMethod,
	ExpectNArgs(0),
)

func flattenMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"fold",
		"Takes two arguments: an initial value, and a mapping query. For each element of an array the mapping context is an object with two fields `tally` and `value`, where `tally` contains the current accumulated value and `value` is the value of the current element. The mapping must return the result of adding the value to the tally.\n\nThe first argument is the value that `tally` will have on the first call.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root.sum = this.foo.fold(0, this.tally + this.value)`,
			`{"foo":[3,8,11]}`,
			`{"sum":22}`,
		),
		NewExampleSpec(``,
			`root.result = this.foo.fold("", "%v%v".format(this.tally, this.value))`,
			`{"foo":["hello ", "world"]}`,
			`{"result":"hello world"}`,
		),
	),
	false, foldMethod,
	ExpectNArgs(2),
	ExpectFunctionArg(1),
)

func foldMethod(target Function, args ...interface{}) (Function, error) {
	var foldTallyStart interface{}
	switch t := args[0].(type) {
	case *Literal:
		foldTallyStart = t.Value
	default:
		foldTallyStart = t
	}
	foldFn, ok := args[1].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[1])
	}

	// TODO: Query targets do not take the fold function into account as it's
	// dynamic. We could work it out by expanding targets with the fold targets
	// less the value prefix.
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		resArray, ok := res.([]interface{})
		if !ok {
			return nil, NewTypeError(res, ValueArray)
		}

		var tally interface{}
		var err error
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

			newV, mapErr := foldFn.Exec(ctx.WithValue(tmpObj))
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
	NewMethodSpec(
		"from",
		"Execute a query from the context of another message in the batch. This allows you to mutate events based on the contents of other messages.",
		NewExampleSpec("For example, the following map extracts the contents of the JSON field `foo` specifically from message index `1` of a batch, effectively overriding the field `foo` for all messages of a batch to that of message 1:",
			`root = this
root.foo = json("foo").from(1)`,
		),
	),
	false, func(target Function, args ...interface{}) (Function, error) {
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

func (f *fromMethod) QueryTargets(ctx TargetsContext) []TargetPath {
	return f.target.QueryTargets(ctx)
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"from_all",
		"Execute a query for all messages of the batch, and return an array of all results.",
		NewExampleSpec("",
			`root = this
root.foo_summed = json("foo").from_all().sum()`,
		),
	),
	false, fromAllMethod,
	ExpectNArgs(0),
)

func fromAllMethod(target Function, _ ...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"get",
		"Extract a field value, identified via a [dot path][field_paths], from an object.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.result = this.foo.get(this.target)`,
			`{"foo":{"bar":"from bar","baz":"from baz"},"target":"bar"}`,
			`{"result":"from bar"}`,
			`{"foo":{"bar":"from bar","baz":"from baz"},"target":"baz"}`,
			`{"result":"from baz"}`,
		),
	),
	true, getMethodCtor,
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

func (g *getMethod) QueryTargets(ctx TargetsContext) []TargetPath {
	targets := g.fn.QueryTargets(ctx)
	for i, t := range targets {
		tmpPath := make([]string, 0, len(t.Path)+len(g.path))
		tmpPath = append(tmpPath, t.Path...)
		tmpPath = append(tmpPath, g.path...)
		targets[i].Path = tmpPath
	}
	return targets
}

// NewGetMethod creates a new get method.
func NewGetMethod(target Function, path string) (Function, error) {
	return getMethodCtor(target, path)
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
	NewMethodSpec(
		"index",
		"Extract an element from an array by an index. The index can be negative, and if so the element will be selected from the end counting backwards starting from -1. E.g. an index of -1 returns the last element, an index of -2 returns the element before the last, and so on.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.last_name = this.names.index(-1)`,
			`{"names":["rachel","stevens"]}`,
			`{"last_name":"stevens"}`,
		),
	),
	true, indexMethod,
	ExpectNArgs(1),
	ExpectIntArg(0),
)

func indexMethod(target Function, args ...interface{}) (Function, error) {
	index := args[0].(int64)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"json_schema",
		"Checks a [JSON schema](https://json-schema.org/) against a value and returns the value if it matches or throws and error if it does not.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root = this.json_schema("""{
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}""")`,
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			`{"foo":5}`,
			`Error("failed to execute mapping query at line 1: foo invalid type. expected: string, given: integer")`,
		),
		NewExampleSpec(
			"In order to load a schema from a file use the `file` function.",
			`root = this.json_schema(file(var("BENTHOS_TEST_BLOBLANG_SCHEMA_FILE")))`,
		),
	).Beta(),
	true, jsonSchemaMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func jsonSchemaMethod(target Function, args ...interface{}) (Function, error) {
	schema, err := jsonschema.NewSchema(jsonschema.NewStringLoader(args[0].(string)))
	if err != nil {
		return nil, fmt.Errorf("failed to parse json schema definition: %w", err)
	}
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		result, err := schema.Validate(jsonschema.NewGoLoader(res))
		if err != nil {
			return nil, err
		}
		if !result.Valid() {
			var errStr string
			for i, desc := range result.Errors() {
				if i > 0 {
					errStr = errStr + "\n"
				}
				description := strings.ToLower(desc.Description())
				if property := desc.Details()["property"]; property != nil {
					description = property.(string) + strings.TrimPrefix(description, strings.ToLower(property.(string)))
				}
				errStr = errStr + desc.Field() + " " + description
			}
			return nil, errors.New(errStr)
		}
		return res, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"keys",
		"Returns the keys of an object as an array. The order of the resulting array will be random.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.foo_keys = this.foo.keys()`,
			`{"foo":{"bar":1,"baz":2}}`,
			`{"foo_keys":["bar","baz"]}`,
		),
	),
	false, keysMethod,
	ExpectNArgs(0),
)

func keysMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		if m, ok := v.(map[string]interface{}); ok {
			keys := make([]interface{}, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool {
				return keys[i].(string) < keys[j].(string)
			})
			return keys, nil
		}
		return nil, NewTypeError(v, ValueObject)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"length", "",
	).InCategory(
		MethodCategoryStrings, "Returns the length of a string.",
		NewExampleSpec("",
			`root.foo_len = this.foo.length()`,
			`{"foo":"hello world"}`,
			`{"foo_len":11}`,
		),
	).InCategory(
		MethodCategoryObjectAndArray, "Returns the length of an array or object (number of keys).",
		NewExampleSpec("",
			`root.foo_len = this.foo.length()`,
			`{"foo":["first","second"]}`,
			`{"foo_len":2}`,
			`{"foo":{"first":"bar","second":"baz"}}`,
			`{"foo_len":2}`,
		),
	),
	false, lengthMethod,
	ExpectNArgs(0),
)

func lengthMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewDeprecatedMethodSpec("map"), false, mapMethod,
	ExpectNArgs(1),
	ExpectFunctionArg(0),
)

// NewMapMethod attempts to create a map method.
func NewMapMethod(target, mapArg Function) (Function, error) {
	return mapMethod(target, mapArg)
}

func mapMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		res, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		return mapFn.Exec(ctx.WithValue(res))
	}, func(ctx TargetsContext) []TargetPath {
		return expandTargetPaths(target.QueryTargets(ctx), mapFn.QueryTargets(ctx))
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"map_each", "",
	).InCategory(
		MethodCategoryObjectAndArray, "Returns the length of an array or object (number of keys).",
		NewExampleSpec(`#### On arrays

Apply a mapping to each element of an array and replace the element with the result. Within the argument mapping the context is the value of the element being mapped.`,
			`root.new_nums = this.nums.map_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[1,7]}`,
		),
		NewExampleSpec(`#### On objects

Apply a mapping to each value of an object and replace the value with the result. Within the argument mapping the context is an object with a field `+"`key`"+` containing the value key, and a field `+"`value`"+`.`,
			`root.new_dict = this.dict.map_each(this.value.uppercase())`,
			`{"dict":{"foo":"hello","bar":"world"}}`,
			`{"new_dict":{"bar":"WORLD","foo":"HELLO"}}`,
		),
	),
	false, mapEachMethod,
	ExpectNArgs(1),
	ExpectFunctionArg(0),
)

func mapEachMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}

	// TODO: Query targets do not take the mapping function into account as it's
	// dynamic.
	return simpleMethod(target, func(res interface{}, ctx FunctionContext) (interface{}, error) {
		var resValue interface{}
		var err error
		switch t := res.(type) {
		case []interface{}:
			newSlice := make([]interface{}, 0, len(t))
			for i, v := range t {
				newV, mapErr := mapFn.Exec(ctx.WithValue(v))
				if mapErr != nil {
					if recover, ok := mapErr.(*ErrRecoverable); ok {
						newV = recover.Recovered
						err = fmt.Errorf("failed to process element %v: %w", i, recover.Err)
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
				newV, mapErr := mapFn.Exec(ctx.WithValue(ctxMap))
				if mapErr != nil {
					if recover, ok := mapErr.(*ErrRecoverable); ok {
						newV = recover.Recovered
						err = fmt.Errorf("failed to process element %v: %w", k, recover.Err)
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
	NewMethodSpec(
		"merge", "Merge a source object into an existing destination object. When a collision is found within the merged structures (both a source and destination object contain the same non-object keys) the result will be an array containing both values, where values that are already arrays will be expanded into the resulting array.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root = this.foo.merge(this.bar)`,
			`{"foo":{"first_name":"fooer","likes":"bars"},"bar":{"second_name":"barer","likes":"foos"}}`,
			`{"first_name":"fooer","likes":["bars","foos"],"second_name":"barer"}`,
		),
	),
	false, mergeMethod,
	ExpectNArgs(1),
)

func mergeMethod(target Function, args ...interface{}) (Function, error) {
	var mapFn Function
	switch t := args[0].(type) {
	case Function:
		mapFn = t
	default:
		mapFn = NewLiteralFunction(t)
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, aggregateTargetPaths(target, mapFn)), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewDeprecatedMethodSpec("not"), false, notMethodCtor,
	ExpectNArgs(0),
)

type notMethod struct {
	fn Function
}

// Not returns a logical NOT of a child function.
func Not(fn Function) Function {
	return &notMethod{
		fn: fn,
	}
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

func (n *notMethod) QueryTargets(ctx TargetsContext) []TargetPath {
	return n.fn.QueryTargets(ctx)
}

func notMethodCtor(target Function, _ ...interface{}) (Function, error) {
	return &notMethod{fn: target}, nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"not_null", "",
	).InCategory(
		MethodCategoryCoercion,
		"Ensures that the given value is not `null`, and if so returns it, otherwise an error is returned.",
		NewExampleSpec("",
			`root.a = this.a.not_null()`,
			`{"a":"foobar","b":"barbaz"}`,
			`{"a":"foobar"}`,
			`{"b":"barbaz"}`,
			`Error("failed to execute mapping query at line 1: value is null")`,
		),
	),
	false, notNullMethod,
	ExpectNArgs(0),
)

func notNullMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		if v == nil {
			return nil, errors.New("value is null")
		}
		return v, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"number", "",
	).InCategory(
		MethodCategoryCoercion,
		"Attempt to parse a value into a number. An optional argument can be provided, in which case if the value cannot be parsed into a number the argument will be returned instead.",
		NewExampleSpec("",
			`root.foo = this.thing.number() + 10
root.bar = this.thing.number(5) * 10`,
		),
	),
	true, numberMethod,
	ExpectOneOrZeroArgs(),
	ExpectFloatArg(0),
)

func numberMethod(target Function, args ...interface{}) (Function, error) {
	defaultNum := 0.0
	if len(args) > 0 {
		defaultNum = args[0].(float64)
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"or", "If the result of the target query fails or resolves to `null`, returns the argument instead. This is an explicit method alternative to the coalesce pipe operator `|`.",
		NewExampleSpec("", `root.doc.id = this.thing.id.or(uuid_v4())`),
	),
	false, orMethod,
	ExpectNArgs(1),
)

func orMethod(fn Function, args ...interface{}) (Function, error) {
	var orFn Function
	switch t := args[0].(type) {
	case uint64, int64, float64, string, []byte, bool, []interface{}, map[string]interface{}:
		orFn = NewLiteralFunction(t)
	case Function:
		orFn = t
	default:
		return nil, fmt.Errorf("expected query or literal argument, received %T", args[0])
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		res, err := fn.Exec(ctx)
		if err != nil || IIsNull(res) {
			res, err = orFn.Exec(ctx)
		}
		return res, err
	}, aggregateTargetPaths(fn, orFn)), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"sort", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Attempts to sort the values of an array in increasing order. The type of all values must match in order for the ordering to be accurate. Supports string and number values.",
		NewExampleSpec("",
			`root.sorted = this.foo.sort()`,
			`{"foo":["bbb","ccc","aaa"]}`,
			`{"sorted":["aaa","bbb","ccc"]}`,
		),
		NewExampleSpec("It's also possible to specify a mapping argument, which is provided an object context with fields `left` and `right`, the mapping must return a boolean indicating whether the `left` value is less than `right`. This allows you to sort arrays containing non-string or non-number values.",
			`root.sorted = this.foo.sort(this.left.v < this.right.v)`,
			`{"foo":[{"id":"foo","v":"bbb"},{"id":"bar","v":"ccc"},{"id":"baz","v":"aaa"}]}`,
			`{"sorted":[{"id":"baz","v":"aaa"},{"id":"foo","v":"bbb"},{"id":"bar","v":"ccc"}]}`,
		),
	),
	false, sortMethod,
	ExpectOneOrZeroArgs(),
	ExpectFunctionArg(0),
)

func sortMethod(target Function, args ...interface{}) (Function, error) {
	compareFn := func(ctx FunctionContext, values []interface{}, i, j int) (bool, error) {
		switch values[i].(type) {
		case float64, int64, uint64:
			var lhs, rhs float64
			var err error
			if lhs, err = IGetNumber(values[i]); err == nil {
				rhs, err = IGetNumber(values[j])
			}
			if err != nil {
				return false, err
			}
			return lhs < rhs, nil
		case string, []byte:
			var lhs, rhs string
			var err error
			if lhs, err = IGetString(values[i]); err == nil {
				rhs, err = IGetString(values[j])
			}
			if err != nil {
				return false, err
			}
			return lhs < rhs, nil
		}
		return false, NewTypeError(values[i], ValueNumber, ValueString)
	}
	var mapFn Function
	if len(args) > 0 {
		var ok bool
		if mapFn, ok = args[0].(Function); !ok {
			return nil, fmt.Errorf("expected query argument, received %T", args[0])
		}
		compareFn = func(ctx FunctionContext, values []interface{}, i, j int) (bool, error) {
			var ctxValue interface{} = map[string]interface{}{
				"left":  values[i],
				"right": values[j],
			}
			v, err := mapFn.Exec(ctx.WithValue(ctxValue))
			if err != nil {
				return false, err
			}
			b, _ := v.(bool)
			return b, nil
		}
	}

	targets := target.QueryTargets
	if mapFn != nil {
		targets = aggregateTargetPaths(target, mapFn)
	}

	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
				if err == nil {
					var b bool
					b, err = compareFn(ctx, values, i, j)
					return b
				}
				return false
			})
			if err != nil {
				return nil, err
			}
			return values, nil
		}
		return nil, NewTypeError(v, ValueArray)
	}, targets), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"slice", "",
	).InCategory(
		MethodCategoryStrings,
		"Extract a slice from a string by specifying two indices, a low and high bound, which selects a half-open range that includes the first character, but excludes the last one. If the second index is omitted then it defaults to the length of the input sequence.",
		NewExampleSpec("",
			`root.beginning = this.value.slice(0, 2)
root.end = this.value.slice(4)`,
			`{"value":"foo bar"}`,
			`{"beginning":"fo","end":"bar"}`,
		),
		NewExampleSpec(`A negative low index can be used, indicating an offset from the end of the sequence. If the low index is greater than the length of the sequence then an empty result is returned.`,
			`root.last_chunk = this.value.slice(-4)
root.the_rest = this.value.slice(0, -4)`,
			`{"value":"foo bar"}`,
			`{"last_chunk":" bar","the_rest":"foo"}`,
		),
	).InCategory(
		MethodCategoryObjectAndArray,
		"Extract a slice from an array by specifying two indices, a low and high bound, which selects a half-open range that includes the first element, but excludes the last one. If the second index is omitted then it defaults to the length of the input sequence.",
		NewExampleSpec("",
			`root.beginning = this.value.slice(0, 2)
root.end = this.value.slice(4)`,
			`{"value":["foo","bar","baz","buz","bev"]}`,
			`{"beginning":["foo","bar"],"end":["bev"]}`,
		),
		NewExampleSpec(
			`A negative low index can be used, indicating an offset from the end of the sequence. If the low index is greater than the length of the sequence then an empty result is returned.`,
			`root.last_chunk = this.value.slice(-2)
root.the_rest = this.value.slice(0, -2)`,
			`{"value":["foo","bar","baz","buz","bev"]}`,
			`{"last_chunk":["buz","bev"],"the_rest":["foo","bar","baz"]}`,
		),
	),
	true, sliceMethod,
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
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"sum", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Sum the numerical values of an array.",
		NewExampleSpec("",
			`root.sum = this.foo.sum()`,
			`{"foo":[3,8,4]}`,
			`{"sum":15}`,
		),
	),
	false, sumMethod,
	ExpectNArgs(0),
)

func sumMethod(target Function, _ ...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"type", "",
	).InCategory(
		MethodCategoryCoercion,
		"Returns the type of a value as a string, providing one of the following values: `string`, `bytes`, `number`, `bool`, `array`, `object` or `null`.",
		NewExampleSpec("",
			`root.bar_type = this.bar.type()
root.foo_type = this.foo.type()`,
			`{"bar":10,"foo":"is a string"}`,
			`{"bar_type":"number","foo_type":"string"}`,
		),
	),
	false, typeMethod,
	ExpectNArgs(0),
)

func typeMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		return string(ITypeOf(v)), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"unique", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Attempts to remove duplicate values from an array. The array may contain a combination of different value types, but numbers and strings are checked separately (`\"5\"` is a different element to `5`).",
		NewExampleSpec("",
			`root.uniques = this.foo.unique()`,
			`{"foo":["a","b","a","c"]}`,
			`{"uniques":["a","b","c"]}`,
		),
	),
	false, uniqueMethod,
	ExpectOneOrZeroArgs(),
	ExpectFunctionArg(0),
)

func uniqueMethod(target Function, args ...interface{}) (Function, error) {
	var emitFn Function
	if len(args) > 0 {
		var ok bool
		emitFn, ok = args[0].(Function)
		if !ok {
			return nil, fmt.Errorf("expected query argument, received %T", args[0])
		}
	}

	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
				var err error
				if check, err = emitFn.Exec(ctx.WithValue(v)); err != nil {
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
	NewMethodSpec(
		"values", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Returns the values of an object as an array. The order of the resulting array will be random.",
		NewExampleSpec("",
			`root.foo_vals = this.foo.values().sort()`,
			`{"foo":{"bar":1,"baz":2}}`,
			`{"foo_vals":[1,2]}`,
		),
	),
	false, valuesMethod,
	ExpectNArgs(0),
)

func valuesMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	NewMethodSpec(
		"without", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		`Returns an object where one or more [field path][field_paths] arguments are removed. Each path specifies a specific field to be deleted from the input object, allowing for nested fields.

If a key within a nested path does not exist or is not an object then it is not removed.`,
		NewExampleSpec("",
			`root = this.without("inner.a","inner.c","d")`,
			`{"inner":{"a":"first","b":"second","c":"third"},"d":"fourth","e":"fifth"}`,
			`{"e":"fifth","inner":{"b":"second"}}`,
		),
	),
	true, withoutMethod,
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

	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, NewTypeError(v, ValueObject)
		}
		return mapWithout(m, excludeList), nil
	}), nil
}

//------------------------------------------------------------------------------
