package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/segmentio/ksuid"

	"github.com/benthosdev/benthos/v4/internal/tracing"
)

type fieldFunction struct {
	namedContext string
	fromRoot     bool
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
	if f.fromRoot {
		path = "root"
	} else if path == "" {
		path = "this"
	}
	if len(f.path) > 0 {
		path = path + "." + SliceToDotPath(f.path...)
	}
	return "field `" + path + "`"
}

func (f *fieldFunction) Exec(ctx FunctionContext) (any, error) {
	var target any
	if f.fromRoot {
		if ctx.NewValue == nil {
			return nil, errors.New("unable to reference `root` from this context")
		}
		target = *ctx.NewValue
	} else if f.namedContext == "" {
		v := ctx.Value()
		if v == nil {
			var fieldName string
			if len(f.path) > 0 {
				fieldName = SliceToDotPath(f.path...)
			}
			return nil, ErrNoContext{
				FieldName: fieldName,
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
	if f.fromRoot {
		basePaths = []TargetPath{NewTargetPath(TargetRoot)}
	} else if f.namedContext == "" {
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
	return &fieldFunction{namedContext: namedContext, fromRoot: false, path: path}
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

// NewRootFieldFunction creates a query function that returns a field from the
// root context.
func NewRootFieldFunction(pathStr string) Function {
	var path []string
	if len(pathStr) > 0 {
		path = gabs.DotPathToSlice(pathStr)
	}
	return &fieldFunction{
		fromRoot: true,
		path:     path,
	}
}

//------------------------------------------------------------------------------

// Literal wraps a static value and returns it for each invocation of the
// function.
type Literal struct {
	annotation string
	Value      any
}

// Annotation returns a token identifier of the function.
func (l *Literal) Annotation() string {
	if l.annotation == "" {
		return string(ITypeOf(l.Value)) + " literal"
	}
	return l.annotation
}

// Exec returns a literal value.
func (l *Literal) Exec(ctx FunctionContext) (any, error) {
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

// String returns a string representation of the literal function.
func (l *Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}

// NewLiteralFunction creates a query function that returns a static, literal
// value.
func NewLiteralFunction(annotation string, v any) *Literal {
	return &Literal{annotation: annotation, Value: v}
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
	func(ctx FunctionContext) (any, error) {
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
	func(ctx FunctionContext) (any, error) {
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
	func(ctx FunctionContext) (any, error) {
		return ctx.MsgBatch.Get(ctx.Index).AsBytes(), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "tracing_span",
		"Provides the message tracing span [(created via Open Telemetry APIs)](/docs/components/tracers/about) as an object serialised via text map formatting. The returned value will be `null` if the message does not have a span.",
		NewExampleSpec("",
			`root.headers.traceparent = tracing_span().traceparent`,
			`{"some_stuff":"just can't be explained by science"}`,
			`{"headers":{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}`,
		),
	).Experimental(),
	func(fCtx FunctionContext) (any, error) {
		span := tracing.GetSpan(fCtx.MsgBatch.Get(fCtx.Index))
		if span == nil {
			return nil, nil
		}
		return span.TextMap()
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "tracing_id",
		"Provides the message trace id. The returned value will be zeroed if the message does not contain a span.",
		NewExampleSpec("",
			`meta trace_id = tracing_id()`,
		),
	).Experimental(),
	func(fCtx FunctionContext) (any, error) {
		traceID := tracing.GetTraceID(fCtx.MsgBatch.Get(fCtx.Index))
		return traceID, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
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
	).Param(ParamString("name", "An identifier for the counter.")).MarkImpure(),
	countFunction,
)

var (
	counters    = map[string]int64{}
	countersMux = &sync.Mutex{}
)

func countFunction(args *ParsedParams) (Function, error) {
	name, err := args.FieldString("name")
	if err != nil {
		return nil, err
	}
	return ClosureFunction("function count", func(ctx FunctionContext) (any, error) {
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

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "deleted",
		"A function that returns a result indicating that the mapping target should be deleted. Deleting, also known as dropping, messages will result in them being acknowledged as successfully processed to inputs in a Benthos pipeline. For more information about error handling patterns read [here][error_handling].",
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
	func(*ParsedParams) (Function, error) {
		return NewLiteralFunction("delete", Delete(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "error",
		"If an error has occurred during the processing of a message this function returns the reported cause of the error as a string, otherwise `null`. For more information about error handling patterns read [here][error_handling].",
		NewExampleSpec("",
			`root.doc.error = error()`,
		),
	),
	func(ctx FunctionContext) (any, error) {
		v := ctx.MsgBatch.Get(ctx.Index).ErrorGet()
		if v != nil {
			return v.Error(), nil
		}
		return nil, nil
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
	func(ctx FunctionContext) (any, error) {
		return ctx.MsgBatch.Get(ctx.Index).ErrorGet() != nil, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "range",
		"The `range` function creates an array of integers following a range between a start, stop and optional step integer argument. If the step argument is omitted then it defaults to 1. A negative step can be provided as long as stop < start.",
		NewExampleSpec("",
			`root.a = range(0, 10)
root.b = range(start: 0, stop: this.max, step: 2) # Using named params
root.c = range(0, -this.max, -2)`,
			`{"max":10}`,
			`{"a":[0,1,2,3,4,5,6,7,8,9],"b":[0,2,4,6,8],"c":[0,-2,-4,-6,-8]}`,
		),
	).
		Param(ParamInt64("start", "The start value.")).
		Param(ParamInt64("stop", "The stop value.")).
		Param(ParamInt64("step", "The step value.").Default(1)),
	rangeFunction,
)

func rangeFunction(args *ParsedParams) (Function, error) {
	start, err := args.FieldInt64("start")
	if err != nil {
		return nil, err
	}
	stop, err := args.FieldInt64("stop")
	if err != nil {
		return nil, err
	}
	step, err := args.FieldInt64("step")
	if err != nil {
		return nil, err
	}
	if step == 0 {
		return nil, errors.New("step must be greater than or less than 0")
	}
	if step < 0 {
		if stop > start {
			return nil, fmt.Errorf("with negative step arg stop (%v) must be <= start (%v)", stop, start)
		}
	} else if start >= stop {
		return nil, fmt.Errorf("with positive step arg start (%v) must be < stop (%v)", start, stop)
	}
	r := make([]any, (stop-start)/step)
	for i := 0; i < len(r); i++ {
		r[i] = start + step*int64(i)
	}
	return ClosureFunction("function range", func(ctx FunctionContext) (any, error) {
		return r, nil
	}, nil), nil
}

var _ = registerFunction(
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
	).Param(ParamString("path", "An optional [dot path][field_paths] identifying a field to obtain.").Default("")),
	jsonFunction,
)

func jsonFunction(args *ParsedParams) (Function, error) {
	path, err := args.FieldString("path")
	if err != nil {
		return nil, err
	}
	var argPath []string
	if len(path) > 0 {
		argPath = gabs.DotPathToSlice(path)
	}
	return ClosureFunction("json path `"+SliceToDotPath(argPath...)+"`", func(ctx FunctionContext) (any, error) {
		jPart, err := ctx.MsgBatch.Get(ctx.Index).AsStructured()
		if err != nil {
			return nil, err
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

// NewMetaFunction creates a new function for obtaining a metadata value.
func NewMetaFunction(key string) Function {
	if len(key) > 0 {
		return ClosureFunction("meta field "+key, func(ctx FunctionContext) (any, error) {
			if ctx.NewMeta == nil {
				return nil, errors.New("metadata cannot be queried in this context")
			}
			v, exists := ctx.NewMeta.MetaGetMut(key)
			if !exists {
				return nil, nil
			}
			return v, nil
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
			paths := []TargetPath{
				NewTargetPath(TargetMetadata, key),
			}
			ctx = ctx.WithValues(paths)
			return ctx, paths
		})
	}
	return ClosureFunction("meta object", func(ctx FunctionContext) (any, error) {
		if ctx.NewMeta == nil {
			return nil, errors.New("metadata cannot be queried in this context")
		}
		kvs := map[string]any{}
		_ = ctx.NewMeta.MetaIterMut(func(k string, v any) error {
			kvs[k] = v
			return nil
		})
		return kvs, nil
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		paths := []TargetPath{
			NewTargetPath(TargetMetadata),
		}
		ctx = ctx.WithValues(paths)
		return ctx, paths
	})
}

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "meta",
		"Returns the value of a metadata key from the input message as a string, or `null` if the key does not exist. Since values are extracted from the read-only input message they do NOT reflect changes made from within the map. In order to query metadata mutations made within a mapping use the [`root_meta` function](#root_meta). This function supports extracting metadata from other messages of a batch with the `from` method.",
		NewExampleSpec("",
			`root.topic = meta("kafka_topic")`,
			`root.topic = meta("nope") | meta("also nope") | "default"`,
		),
		NewExampleSpec(
			"The key parameter is optional and if omitted the entire metadata contents are returned as an object.",
			`root.all_metadata = meta()`,
		),
	).Param(ParamString("key", "An optional key of a metadata value to obtain.").Default("")),
	func(args *ParsedParams) (Function, error) {
		key, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}
		if len(key) > 0 {
			return ClosureFunction("meta field "+key, func(ctx FunctionContext) (any, error) {
				v := ctx.MsgBatch.Get(ctx.Index).MetaGetStr(key)
				if v == "" {
					return nil, nil
				}
				return v, nil
			}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
				paths := []TargetPath{
					NewTargetPath(TargetMetadata, key),
				}
				ctx = ctx.WithValues(paths)
				return ctx, paths
			}), nil
		}
		return ClosureFunction("meta object", func(ctx FunctionContext) (any, error) {
			kvs := map[string]any{}
			_ = ctx.MsgBatch.Get(ctx.Index).MetaIterStr(func(k, v string) error {
				kvs[k] = v
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
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryMessage, "root_meta",
		"Returns the value of a metadata key from the new message being created as a string, or `null` if the key does not exist. Changes made to metadata during a mapping will be reflected by this function.",
		NewExampleSpec("",
			`root.topic = root_meta("kafka_topic")`,
			`root.topic = root_meta("nope") | root_meta("also nope") | "default"`,
		),
		NewExampleSpec(
			"The key parameter is optional and if omitted the entire metadata contents are returned as an object.",
			`root.all_metadata = root_meta()`,
		),
	).Beta().Param(ParamString("key", "An optional key of a metadata value to obtain.").Default("")),
	func(args *ParsedParams) (Function, error) {
		key, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}
		if len(key) > 0 {
			return ClosureFunction("root_meta field "+key, func(ctx FunctionContext) (any, error) {
				if ctx.NewMeta == nil {
					return nil, errors.New("root metadata cannot be queried in this context")
				}
				v := ctx.NewMeta.MetaGetStr(key)
				if v == "" {
					return nil, nil
				}
				return v, nil
			}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
				paths := []TargetPath{
					NewTargetPath(TargetMetadata, key),
				}
				ctx = ctx.WithValues(paths)
				return ctx, paths
			}), nil
		}
		return ClosureFunction("root_meta object", func(ctx FunctionContext) (any, error) {
			if ctx.NewMeta == nil {
				return nil, errors.New("root metadata cannot be queried in this context")
			}
			kvs := map[string]any{}
			_ = ctx.NewMeta.MetaIterStr(func(k, v string) error {
				kvs[k] = v
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
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewHiddenFunctionSpec("nothing"),
	func(*ParsedParams) (Function, error) {
		return NewLiteralFunction("nothing", Nothing(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "random_int",
		"Generates a non-negative pseudo-random 64-bit integer. An optional integer argument can be provided in order to seed the random number generator. Optional `min` and `max` arguments can be provided to make the generated numbers within a range.",
		NewExampleSpec("",
			`root.first = random_int()
root.second = random_int(1)
root.third = random_int(max:20)
root.fourth = random_int(min:10, max:20)
root.fifth = random_int(timestamp_unix_nano(), 5, 20)
root.sixth = random_int(seed:timestamp_unix_nano(), max:20)
`,
		),
		NewExampleSpec("It is possible to specify a dynamic seed argument, in which case the argument will only be resolved once during the lifetime of the mapping.",
			`root.first = random_int(timestamp_unix_nano())`,
			`root.second = random_int(timestamp_unix_nano(), 5, 20)`,
		),
	).
		Param(ParamQuery(
			"seed",
			"A seed to use, if a query is provided it will only be resolved once during the lifetime of the mapping.",
			true,
		).Default(NewLiteralFunction("", 0))).
		Param(ParamInt64("min", "The minimum value the random generated number will have. The default value is 0.").Default(0)).
		Param(ParamInt64("max", fmt.Sprintf("The maximum value the random generated number will have. The default value is %d (math.MaxInt64 - 1).", uint64(math.MaxInt64-1))).Default(int64(math.MaxInt64-1))),
	randomIntFunction,
)

func randomIntFunction(args *ParsedParams) (Function, error) {
	seedFn, err := args.FieldQuery("seed")
	if err != nil {
		return nil, err
	}
	min, err := args.FieldInt64("min")
	if err != nil {
		return nil, err
	}
	max, err := args.FieldInt64("max")
	if err != nil {
		return nil, err
	}
	if min < 0 {
		return nil, fmt.Errorf("min (%d) must be a positive number", min)
	}
	if max < min {
		return nil, fmt.Errorf("min (%d) must be smaller or equal than max (%d)", min, max)
	}
	if max == math.MaxInt64 {
		return nil, fmt.Errorf("max must be smaller than the max allowed for an int64 (%d)", uint64(math.MaxInt64))
	}
	var randMut sync.Mutex
	var r *rand.Rand

	return ClosureFunction("function random_int", func(ctx FunctionContext) (any, error) {
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
		// Int63n generates a random number within a half-open interval [0,n)
		v := r.Int63n(max-min+1) + min
		return v, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "now",
		"Returns the current timestamp as a string in RFC 3339 format with the local timezone. Use the method `ts_format` in order to change the format and timezone.",
		NewExampleSpec("",
			`root.received_at = now()`,
		),
		NewExampleSpec("",
			`root.received_at = now().ts_format("Mon Jan 2 15:04:05 -0700 MST 2006", "UTC")`,
		),
	),
	func(args *ParsedParams) (Function, error) {
		return ClosureFunction("function now", func(_ FunctionContext) (any, error) {
			return time.Now().Format(time.RFC3339Nano), nil
		}, nil), nil
	},
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix",
		"Returns the current unix timestamp in seconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix()`,
		),
	),
	func(_ FunctionContext) (any, error) {
		return time.Now().Unix(), nil
	},
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix_milli",
		"Returns the current unix timestamp in milliseconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix_milli()`,
		),
	),
	func(_ FunctionContext) (any, error) {
		return time.Now().UnixMilli(), nil
	},
)

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix_micro",
		"Returns the current unix timestamp in microseconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix_micro()`,
		),
	),
	func(_ FunctionContext) (any, error) {
		return time.Now().UnixMicro(), nil
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
	func(_ FunctionContext) (any, error) {
		return time.Now().UnixNano(), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
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
	).Param(ParamString("why", "A string explanation for why an error was thrown, this will be added to the resulting error message.")),
	func(args *ParsedParams) (Function, error) {
		msg, err := args.FieldString("why")
		if err != nil {
			return nil, err
		}
		return ClosureFunction("function throw", func(_ FunctionContext) (any, error) {
			return nil, errors.New(msg)
		}, nil), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "uuid_v4",
		"Generates a new RFC-4122 UUID each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = uuid_v4()`),
	),
	func(_ FunctionContext) (any, error) {
		u4, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		return u4.String(), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "nanoid",
		"Generates a new nanoid each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = nanoid()`),
		NewExampleSpec("It is possible to specify an optional length parameter.", `root.id = nanoid(54)`),
		NewExampleSpec("It is also possible to specify an optional custom alphabet after the length parameter.", `root.id = nanoid(54, "abcde")`),
	).
		Param(ParamInt64("length", "An optional length.").Optional()).
		Param(ParamString("alphabet", "An optional custom alphabet to use for generating IDs. When specified the field `length` must also be present.").Optional()),
	nanoidFunction,
)

func nanoidFunction(args *ParsedParams) (Function, error) {
	lenArg, err := args.FieldOptionalInt64("length")
	if err != nil {
		return nil, err
	}
	alphabetArg, err := args.FieldOptionalString("alphabet")
	if err != nil {
		return nil, err
	}
	if alphabetArg != nil && lenArg == nil {
		return nil, errors.New("field length must be specified when an alphabet is specified")
	}
	return ClosureFunction("function nanoid", func(ctx FunctionContext) (any, error) {
		if alphabetArg != nil {
			return gonanoid.Generate(*alphabetArg, int(*lenArg))
		}
		if lenArg != nil {
			return gonanoid.New(int(*lenArg))
		}
		return gonanoid.New()
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleFunction(
	NewFunctionSpec(
		FunctionCategoryGeneral, "ksuid",
		"Generates a new ksuid each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = ksuid()`),
	),
	func(_ FunctionContext) (any, error) {
		return ksuid.New().String(), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerFunction(
	NewHiddenFunctionSpec("var").Param(ParamString("name", "The name of the target variable.")),
	func(args *ParsedParams) (Function, error) {
		name, err := args.FieldString("name")
		if err != nil {
			return nil, err
		}
		return NewVarFunction(name), nil
	},
)

// NewVarFunction creates a new variable function.
func NewVarFunction(name string) Function {
	return ClosureFunction("variable "+name, func(ctx FunctionContext) (any, error) {
		if ctx.Vars == nil {
			return nil, errors.New("variables were undefined")
		}
		if res, ok := ctx.Vars[name]; ok {
			return res, nil
		}
		return nil, fmt.Errorf("variable '%v' undefined", name)
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		paths := []TargetPath{
			NewTargetPath(TargetVariable, name),
		}
		ctx = ctx.WithValues(paths)
		return ctx, paths
	})
}
