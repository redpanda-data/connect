package query

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

//------------------------------------------------------------------------------

type fieldFunction struct {
	namedContext string
	path         []string
}

func (f *fieldFunction) expand(path ...string) *fieldFunction {
	newFn := *f
	newPath := make([]string, 0, len(f.path)+len(path))
	newPath = append(newPath, f.path...)
	newPath = append(newPath, path...)
	newFn.path = newPath
	return &newFn
}

func (f *fieldFunction) Annotation() string {
	path := f.namedContext
	if path == "" {
		path = "this"
	}
	if len(f.path) > 0 {
		path = path + "." + SliceToDotPath(f.path...)
	}
	return "field `" + path + "`"
}

func (f *fieldFunction) Exec(ctx FunctionContext) (interface{}, error) {
	var target interface{}
	if f.namedContext == "" {
		v := ctx.Value()
		if v == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       ErrNoContext,
			}
		}
		target = *v
	} else {
		var ok bool
		if target, ok = ctx.NamedValue(f.namedContext); !ok {
			return ctx, fmt.Errorf("named context %v was not found", f.namedContext)
		}
	}
	if len(f.path) == 0 {
		return target, nil
	}
	return gabs.Wrap(target).S(f.path...).Data(), nil
}

func (f *fieldFunction) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	var basePaths []TargetPath
	if f.namedContext == "" {
		if basePaths = ctx.MainContext(); len(basePaths) == 0 {
			basePaths = []TargetPath{NewTargetPath(TargetValue)}
		}
	} else {
		basePaths = ctx.NamedContext(f.namedContext)
	}
	paths := make([]TargetPath, len(basePaths))
	for i, p := range basePaths {
		paths[i] = p
		paths[i].Path = append(paths[i].Path, f.path...)
	}
	ctx = ctx.WithValues(paths)
	return ctx, paths
}

func (f *fieldFunction) Close(ctx context.Context) error {
	return nil
}

// NewNamedContextFieldFunction creates a query function that attempts to
// return a field from a named context.
func NewNamedContextFieldFunction(namedContext, pathStr string) Function {
	var path []string
	if len(pathStr) > 0 {
		path = gabs.DotPathToSlice(pathStr)
	}
	return &fieldFunction{namedContext, path}
}

// NewFieldFunction creates a query function that returns a field from the
// current context.
func NewFieldFunction(pathStr string) Function {
	var path []string
	if len(pathStr) > 0 {
		path = gabs.DotPathToSlice(pathStr)
	}
	return &fieldFunction{
		path: path,
	}
}

//------------------------------------------------------------------------------

// Literal wraps a static value and returns it for each invocation of the
// function.
type Literal struct {
	annotation string
	Value      interface{}
}

// Annotation returns a token identifier of the function.
func (l *Literal) Annotation() string {
	if l.annotation == "" {
		return string(ITypeOf(l.Value)) + " literal"
	}
	return l.annotation
}

// Exec returns a literal value.
func (l *Literal) Exec(ctx FunctionContext) (interface{}, error) {
	return l.Value, nil
}

// QueryTargets returns nothing.
func (l *Literal) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return ctx, nil
}

// Close does nothing.
func (l *Literal) Close(ctx context.Context) error {
	return nil
}

// NewLiteralFunction creates a query function that returns a static, literal
// value.
func NewLiteralFunction(annotation string, v interface{}) *Literal {
	return &Literal{annotation, v}
}

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "batch_index",
		"Returns the index of the mapped message within a batch. This is useful for applying maps only on certain messages of a batch.",
		NewExampleSpec("",
			`root = if batch_index() > 0 { deleted() }`,
		),
	),
	func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.Index), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "batch_size",
		"Returns the size of the message batch.",
		NewExampleSpec("",
			`root.foo = batch_size()`,
		),
	),
	func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.MsgBatch.Len()), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "content",
		"Returns the full raw contents of the mapping target message as a byte array. When mapping to a JSON field the value should be encoded using the method [`encode`][methods.encode], or cast to a string directly using the method [`string`][methods.string], otherwise it will be base64 encoded by default.",
		NewExampleSpec("",
			`root.doc = content().string()`,
			`{"foo":"bar"}`,
			`{"doc":"{\"foo\":\"bar\"}"}`,
		),
	),
	func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Get(), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "count",
		"The `count` function is a counter starting at 1 which increments after each time it is called. Count takes an argument which is an identifier for the counter, allowing you to specify multiple unique counters in your configuration.",
		NewExampleSpec("",
			`root = this
root.id = count("bloblang_function_example")`,
			`{"message":"foo"}`,
			`{"id":1,"message":"foo"}`,
			`{"message":"bar"}`,
			`{"id":2,"message":"bar"}`,
		),
	),
	true, countFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func countFunction(args ...interface{}) (Function, error) {
	return ClosureFunction("function count", func(ctx FunctionContext) (interface{}, error) {
		name := args[0].(string)

		countersMux.Lock()
		defer countersMux.Unlock()

		var count int64
		var exists bool

		if count, exists = counters[name]; exists {
			count++
		} else {
			count = 1
		}
		counters[name] = count

		return count, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "deleted",
		"A function that returns a result indicating that the mapping target should be deleted.",
		NewExampleSpec("",
			`root = this
root.bar = deleted()`,
			`{"bar":"bar_value","baz":"baz_value","foo":"foo value"}`,
			`{"baz":"baz_value","foo":"foo value"}`,
		),
		NewExampleSpec(
			"Since the result is a value it can be used to do things like remove elements of an array within `map_each`.",
			`root.new_nums = this.nums.map_each(num -> if num < 10 { deleted() } else { num - 10 })`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[1,7]}`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return NewLiteralFunction("delete", Delete(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "env",
		"Returns the value of an environment variable, or an empty string if the environment variable does not exist.",
		NewExampleSpec("",
			`root.thing.key = env("key")`,
		),
	).MarkImpure(),
	true, envFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func envFunction(args ...interface{}) (Function, error) {
	key := os.Getenv(args[0].(string))
	return NewLiteralFunction("env "+key, key), nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "error",
		"If an error has occurred during the processing of a message this function returns the reported cause of the error. For more information about error handling patterns read [here][error_handling].",
		NewExampleSpec("",
			`root.doc.error = error()`,
		),
	),
	func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey), nil
	},
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "errored",
		"Returns a boolean value indicating whether an error has occurred during the processing of a message. For more information about error handling patterns read [here][error_handling].",
		NewExampleSpec("",
			`root.doc.status = if errored() { 400 } else { 200 }`,
		),
	),
	func(ctx FunctionContext) (interface{}, error) {
		return len(ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey)) > 0, nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "file",
		"Reads a file and returns its contents. Relative paths are resolved from the directory of the process executing the mapping.",
		NewExampleSpec("",
			`root.doc = file(env("BENTHOS_TEST_BLOBLANG_FILE")).parse_json()`,
			`{}`,
			`{"doc":{"foo":"bar"}}`,
		),
	).Beta().MarkImpure(),
	true, fileFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func fileFunction(args ...interface{}) (Function, error) {
	path := args[0].(string)
	pathBytes, err := ioutil.ReadFile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return NewLiteralFunction("file "+path, pathBytes), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "range",
		"The `range` function creates an array of integers following a range between a start, stop and optional step integer argument. If the step argument is omitted then it defaults to 1. A negative step can be provided as long as stop < start.",
		NewExampleSpec("",
			`root.a = range(0, 10)
root.b = range(0, this.max, 2)
root.c = range(0, -this.max, -2)`,
			`{"max":10}`,
			`{"a":[0,1,2,3,4,5,6,7,8,9],"b":[0,2,4,6,8],"c":[0,-2,-4,-6,-8]}`,
		),
	),
	true, rangeFunction,
	ExpectBetweenNAndMArgs(2, 3),
	ExpectIntArg(0),
	ExpectIntArg(1),
	ExpectIntArg(2),
)

func rangeFunction(args ...interface{}) (Function, error) {
	start, stop, step := args[0].(int64), args[1].(int64), int64(1)
	if len(args) > 2 {
		step = args[2].(int64)
	}
	if step < 0 && stop > start {
		return nil, fmt.Errorf("with negative step arg stop (%v) must be <= to start (%v)", stop, start)
	}
	r := make([]interface{}, (stop-start)/step)
	for i := 0; i < len(r); i++ {
		r[i] = start + step*int64(i)
	}
	return ClosureFunction("function range", func(ctx FunctionContext) (interface{}, error) {
		return r, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "hostname",
		"Returns a string matching the hostname of the machine running Benthos.",
		NewExampleSpec("",
			`root.thing.host = hostname()`,
		),
	).MarkImpure(),
	func(_ FunctionContext) (interface{}, error) {
		hn, err := os.Hostname()
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		return hn, err
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "json",
		"Returns the value of a field within a JSON message located by a [dot path][field_paths] argument. This function always targets the entire source JSON document regardless of the mapping context.",
		NewExampleSpec("",
			`root.mapped = json("foo.bar")`,
			`{"foo":{"bar":"hello world"}}`,
			`{"mapped":"hello world"}`,
		),
		NewExampleSpec(
			"The path argument is optional and if omitted the entire JSON payload is returned.",
			`root.doc = json()`,
			`{"foo":{"bar":"hello world"}}`,
			`{"doc":{"foo":{"bar":"hello world"}}}`,
		),
	),
	true, jsonFunction,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func jsonFunction(args ...interface{}) (Function, error) {
	var argPath []string
	if len(args) > 0 {
		argPath = gabs.DotPathToSlice(args[0].(string))
	}
	return ClosureFunction("json path `"+SliceToDotPath(argPath...)+"`", func(ctx FunctionContext) (interface{}, error) {
		jPart, err := ctx.MsgBatch.Get(ctx.Index).JSON()
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       err,
			}
		}
		gPart := gabs.Wrap(jPart)
		if len(argPath) > 0 {
			gPart = gPart.Search(argPath...)
		}
		return ISanitize(gPart.Data()), nil
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		paths := []TargetPath{
			NewTargetPath(TargetValue, argPath...),
		}
		ctx = ctx.WithValues(paths)
		return ctx, paths
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "meta",
		"Returns the value of a metadata key from the input message. Since values are extracted from the read-only input message they do NOT reflect changes made from within the map. In order to query metadata mutations made within a mapping use the [`root_meta` function](#root_meta). This function supports extracting metadata from other messages of a batch with the `from` method.",
		NewExampleSpec("",
			`root.topic = meta("kafka_topic")`,
		),
		NewExampleSpec(
			"If the target key does not exist an error is thrown, allowing you to use coalesce or catch methods to fallback to other queries.",
			`root.topic = meta("nope") | meta("also nope") | "default"`,
		),
		NewExampleSpec(
			"The parameter is optional and if omitted the entire metadata contents are returned as an object.",
			`root.all_metadata = meta()`,
		),
	),
	true,
	func(args ...interface{}) (Function, error) {
		if len(args) > 0 {
			field := args[0].(string)
			return ClosureFunction("meta field "+field, func(ctx FunctionContext) (interface{}, error) {
				v := ctx.MsgBatch.Get(ctx.Index).Metadata().Get(field)
				if v == "" {
					return nil, &ErrRecoverable{
						Recovered: "",
						Err:       fmt.Errorf("metadata value '%v' not found", field),
					}
				}
				return v, nil
			}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
				paths := []TargetPath{
					NewTargetPath(TargetMetadata, field),
				}
				ctx = ctx.WithValues(paths)
				return ctx, paths
			}), nil
		}
		return ClosureFunction("meta object", func(ctx FunctionContext) (interface{}, error) {
			kvs := map[string]interface{}{}
			ctx.MsgBatch.Get(ctx.Index).Metadata().Iter(func(k, v string) error {
				if len(v) > 0 {
					kvs[k] = v
				}
				return nil
			})
			return kvs, nil
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
			paths := []TargetPath{
				NewTargetPath(TargetMetadata),
			}
			ctx = ctx.WithValues(paths)
			return ctx, paths
		}), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "root_meta",
		"Returns the value of a metadata key from the new message being created. Changes made to metadata during a mapping will be reflected by this function.",
		NewExampleSpec("",
			`root.topic = root_meta("kafka_topic")`,
		),
		NewExampleSpec(
			"If the target key does not exist an error is thrown, allowing you to use coalesce or catch methods to fallback to other queries.",
			`root.topic = root_meta("nope") | root_meta("also nope") | "default"`,
		),
		NewExampleSpec(
			"The parameter is optional and if omitted the entire metadata contents are returned as an object.",
			`root.all_metadata = root_meta()`,
		),
	).Beta(),
	true,
	func(args ...interface{}) (Function, error) {
		if len(args) > 0 {
			field := args[0].(string)
			return ClosureFunction("root_meta field "+field, func(ctx FunctionContext) (interface{}, error) {
				if ctx.NewMsg == nil {
					return nil, errors.New("root metadata cannot be queried in this context")
				}
				v := ctx.NewMsg.Metadata().Get(field)
				if v == "" {
					return nil, fmt.Errorf("metadata value '%v' not found", field)
				}
				return v, nil
			}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
				paths := []TargetPath{
					NewTargetPath(TargetMetadata, field),
				}
				ctx = ctx.WithValues(paths)
				return ctx, paths
			}), nil
		}
		return ClosureFunction("root_meta object", func(ctx FunctionContext) (interface{}, error) {
			if ctx.NewMsg == nil {
				return nil, errors.New("root metadata cannot be queried in this context")
			}
			kvs := map[string]interface{}{}
			ctx.NewMsg.Metadata().Iter(func(k, v string) error {
				if len(v) > 0 {
					kvs[k] = v
				}
				return nil
			})
			return kvs, nil
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
			paths := []TargetPath{
				NewTargetPath(TargetMetadata),
			}
			ctx = ctx.WithValues(paths)
			return ctx, paths
		}), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewHiddenFunctionSpec("nothing"),
	false, func(...interface{}) (Function, error) {
		return NewLiteralFunction("nothing", Nothing(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "random_int",
		"Generates a non-negative pseudo-random 64-bit integer. An optional integer argument can be provided in order to seed the random number generator.",
		NewExampleSpec("",
			`root.first = random_int()
root.second = random_int(1)`,
		),
		NewExampleSpec("It is possible to specify a dynamic seed argument, in which case the argument will only be resolved once during the lifetime of the mapping.",
			`root.first = random_int(timestamp_unix_nano())`,
		),
	),
	false, randomIntFunction,
	ExpectOneOrZeroArgs(),
)

func randomIntFunction(args ...interface{}) (Function, error) {
	var seedFn Function
	var randMut sync.Mutex
	var r *rand.Rand
	if len(args) > 0 {
		var isDyn bool
		if seedFn, isDyn = args[0].(Function); !isDyn {
			seed, err := IGetInt(args[0])
			if err != nil {
				return nil, err
			}
			r = rand.New(rand.NewSource(seed))
		}
	} else {
		r = rand.New(rand.NewSource(0))
	}
	return ClosureFunction("function random_int", func(ctx FunctionContext) (interface{}, error) {
		randMut.Lock()
		defer randMut.Unlock()

		if r == nil {
			seedI, err := seedFn.Exec(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to seed random number generator: %v", err)
			}

			seed, err := IToInt(seedI)
			if err != nil {
				return nil, fmt.Errorf("failed to seed random number generator: %v", err)
			}

			r = rand.New(rand.NewSource(seed))
		}

		v := int64(r.Int())
		return v, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "now",
		"Returns the current timestamp as a string in ISO 8601 format with the local timezone. Use the method `format_timestamp` in order to change the format and timezone.",
		NewExampleSpec("",
			`root.received_at = now()`,
		),
		NewExampleSpec("",
			`root.received_at = now().format_timestamp("Mon Jan 2 15:04:05 -0700 MST 2006", "UTC")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		return ClosureFunction("function now", func(_ FunctionContext) (interface{}, error) {
			return time.Now().Format(time.RFC3339Nano), nil
		}, nil), nil
	},
	ExpectNArgs(0),
)

var _ = RegisterFunction(
	NewDeprecatedFunctionSpec(
		"timestamp",
		"Returns the current time in a custom format specified by the argument. The format is defined by showing how the reference time, defined to be `Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.\n\nA fractional second is represented by adding a period and zeros to the end of the seconds section of layout string, as in `15:04:05.000` to format a time stamp with millisecond precision. This has been deprecated in favour of the new `now` function.",
		NewExampleSpec("",
			`root.received_at = timestamp("15:04:05")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return ClosureFunction("function timestamp", func(_ FunctionContext) (interface{}, error) {
			return time.Now().Format(format), nil
		}, nil), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

var _ = RegisterFunction(
	NewDeprecatedFunctionSpec(
		"timestamp_utc",
		"The equivalent of `timestamp` except the time is printed as UTC instead of the local timezone. This has been deprecated in favour of the new `now` function.",
		NewExampleSpec("",
			`root.received_at = timestamp_utc("15:04:05")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return ClosureFunction("function timestamp_utc", func(_ FunctionContext) (interface{}, error) {
			return time.Now().In(time.UTC).Format(format), nil
		}, nil), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix",
		"Returns the current unix timestamp in seconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix()`,
		),
	),
	func(_ FunctionContext) (interface{}, error) {
		return time.Now().Unix(), nil
	},
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix_nano",
		"Returns the current unix timestamp in nanoseconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix_nano()`,
		),
	),
	func(_ FunctionContext) (interface{}, error) {
		return time.Now().UnixNano(), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "throw",
		"Throws an error similar to a regular mapping error. This is useful for abandoning a mapping entirely given certain conditions.",
		NewExampleSpec("",
			`root.doc.type = match {
  this.exists("header.id") => "foo"
  this.exists("body.data") => "bar"
  _ => throw("unknown type")
}
root.doc.contents = (this.body.content | this.thing.body)`,
			`{"header":{"id":"first"},"thing":{"body":"hello world"}}`,
			`{"doc":{"contents":"hello world","type":"foo"}}`,
			`{"nothing":"matches"}`,
			`Error("failed assignment (line 1): unknown type")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		msg := args[0].(string)
		return ClosureFunction("function throw", func(_ FunctionContext) (interface{}, error) {
			return nil, errors.New(msg)
		}, nil), nil
	},
	ExpectNArgs(1),
	ExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "uuid_v4",
		"Generates a new RFC-4122 UUID each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = uuid_v4()`),
	),
	func(_ FunctionContext) (interface{}, error) {
		u4, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		return u4.String(), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "nanoid",
		"Generates a new nanoid each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = nanoid()`),
		NewExampleSpec("It is possible to specify a length.", `root.id = nanoid(54)`),
		NewExampleSpec("It is possible to specify a length and custom alphabet.", `root.id = nanoid(54, "abcde")`),
	),
	true,
	nanoidFunction,
	ExpectBetweenNAndMArgs(0, 2),
)

func nanoidFunction(args ...interface{}) (Function, error) {
	return ClosureFunction("function nanoid", func(ctx FunctionContext) (interface{}, error) {
		switch len(args) {
		case 0:
			return gonanoid.New()
		case 1:
			return gonanoid.New(args[0].(int))
		case 2:
			return gonanoid.Generate(args[1].(string), args[0].(int))
		}
		return nil, errors.New("nanoid cannot take more than two arguments")
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	NewHiddenFunctionSpec("var"), true, varFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

// NewVarFunction creates a new variable function.
func NewVarFunction(path string) Function {
	fn, _ := varFunction(path)
	return fn
}

func varFunction(args ...interface{}) (Function, error) {
	name := args[0].(string)
	return ClosureFunction("variable "+name, func(ctx FunctionContext) (interface{}, error) {
		if ctx.Vars == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       errors.New("variables were undefined"),
			}
		}
		if res, ok := ctx.Vars[name]; ok {
			return res, nil
		}
		return nil, &ErrRecoverable{
			Recovered: nil,
			Err:       fmt.Errorf("variable '%v' undefined", name),
		}
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		paths := []TargetPath{
			NewTargetPath(TargetVariable, name),
		}
		ctx = ctx.WithValues(paths)
		return ctx, paths
	}), nil
}

//------------------------------------------------------------------------------
