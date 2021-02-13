package query

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Jeffail/gabs/v2"
)

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
		"Attempt to parse a value into a boolean. An optional argument can be provided, in which case if the value cannot be parsed the argument will be returned instead. If the value is a number then any non-zero value will resolve to `true`, if the value is a string then any of the following values are considered valid: `1, t, T, TRUE, true, True, 0, f, F, FALSE`.",
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
		NewExampleSpec("When the input document is not structured attempting to reference structured fields with `this` will result in an error. Therefore, a convenient way to delete non-structured data is with a catch.",
			`root = this.catch(deleted())`,
			`{"doc":{"foo":"bar"}}`,
			`{"doc":{"foo":"bar"}}`,
			`not structured data`,
			`<Message deleted>`,
		),
	),
	false, catchMethod,
	ExpectNArgs(1),
)

func catchMethod(fn Function, args ...interface{}) (Function, error) {
	catchVal := args[0]
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		res, err := fn.Exec(ctx)
		if err != nil {
			return catchVal, nil
		}
		return res, err
	}, fn.QueryTargets), nil
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
	NewHiddenMethodSpec("map"), false, mapMethod,
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
	NewHiddenMethodSpec("not"), false, notMethodCtor,
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
	true, numberCoerceMethod,
	ExpectOneOrZeroArgs(),
	ExpectFloatArg(0),
)

func numberCoerceMethod(target Function, args ...interface{}) (Function, error) {
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
	case uint64, int64, float64, json.Number, string, []byte, bool, []interface{}, map[string]interface{}:
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
