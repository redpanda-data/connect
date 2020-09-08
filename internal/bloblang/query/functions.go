package query

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
)

//------------------------------------------------------------------------------

type fieldFunction struct {
	path []string
}

func (f *fieldFunction) Exec(ctx FunctionContext) (interface{}, error) {
	if ctx.Value == nil {
		return nil, &ErrRecoverable{
			Recovered: nil,
			Err:       ErrNoContext,
		}
	}
	if len(f.path) == 0 {
		return *ctx.Value, nil
	}
	return gabs.Wrap(*ctx.Value).S(f.path...).Data(), nil
}

func (f *fieldFunction) QueryTargets(ctx TargetsContext) []TargetPath {
	return []TargetPath{
		NewTargetPath(TargetValue, f.path...),
	}
}

func (f *fieldFunction) Close(ctx context.Context) error {
	return nil
}

// NewFieldFunction creates a query function that returns a path from a JSON
// input document.
func NewFieldFunction(pathStr string) Function {
	var path []string
	if len(pathStr) > 0 {
		path = gabs.DotPathToSlice(pathStr)
	}
	return &fieldFunction{path}
}

func fieldFunctionCtor(args ...interface{}) (Function, error) {
	var path []string
	if len(args) > 0 {
		path = gabs.DotPathToSlice(args[0].(string))
	}
	return &fieldFunction{path}, nil
}

//------------------------------------------------------------------------------

// Literal wraps a static value and returns it for each invocation of the
// function.
type Literal struct {
	Value interface{}
}

// Exec returns a literal value.
func (l *Literal) Exec(ctx FunctionContext) (interface{}, error) {
	return l.Value, nil
}

// QueryTargets returns nothing.
func (l *Literal) QueryTargets(ctx TargetsContext) []TargetPath {
	return nil
}

// Close does nothing.
func (l *Literal) Close(ctx context.Context) error {
	return nil
}

// NewLiteralFunction creates a query function that returns a static, literal
// value.
func NewLiteralFunction(v interface{}) *Literal {
	return &Literal{v}
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "batch_index",
		"Returns the index of the mapped message within a batch. This is useful for applying maps only on certain messages of a batch.",
		NewExampleSpec("",
			`root = if batch_index() > 0 { deleted() }`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			return int64(ctx.Index), nil
		}, nil), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "batch_size",
		"Returns the size of the message batch.",
		NewExampleSpec("",
			`root.foo = batch_size()`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			return int64(ctx.MsgBatch.Len()), nil
		}, nil), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "content",
		"Returns the full raw contents of the mapping target message as a byte array. When mapping to a JSON field the value should be encoded using the method [`encode`][methods.encode], or cast to a string directly using the method [`string`][methods.string], otherwise it will be base64 encoded by default.",
		NewExampleSpec("",
			`root.doc = content().string()`,
			`{"foo":"bar"}`,
			`{"doc":"{\"foo\":\"bar\"}"}`,
		),
	),
	false, contentFunction,
)

func contentFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Get(), nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
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
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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

var _ = RegisterFunctionSpec(
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
			`root.new_nums = this.nums.map_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[1,7]}`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return NewLiteralFunction(Delete(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "env",
		"Returns the value of an environment variable.",
		NewExampleSpec("",
			`root.thing.key = env("key")`,
		),
	),
	true, envFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func envFunction(args ...interface{}) (Function, error) {
	key := args[0].(string)
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return os.Getenv(key), nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "error",
		"If an error has occurred during the processing of a message this function returns the reported cause of the error. For more information about error handling patterns read [here][error_handling].",
		NewExampleSpec("",
			`root.doc.error = error()`,
		),
	),
	false, errorFunction,
)

func errorFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey), nil
	}, nil), nil
}

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "errored",
		"Returns a boolean value indicating whether an error has occurred during the processing of a message. For more information about error handling patterns read [here][error_handling].",
		NewExampleSpec("",
			`root.doc.status = if errored() { 400 } else { 200 }`,
		),
	),
	false, erroredFunction,
)

func erroredFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return len(ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey)) > 0, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
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
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return r, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "hostname",
		"Returns a string matching the hostname of the machine running Benthos.",
		NewExampleSpec("",
			`root.thing.host = hostname()`,
		),
	),
	false, hostnameFunction,
)

func hostnameFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		hn, err := os.Hostname()
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		return hn, err
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
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
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, func(ctx TargetsContext) []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetValue, argPath...),
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryMessage, "meta",
		"Returns the value of a metadata key from a message identified by a key. Values are extracted from the referenced input message and therefore do NOT reflect changes made from within the map.",
		NewExampleSpec("",
			`root.topic = meta("kafka_topic")`,
		),
		NewExampleSpec(
			"The parameter is optional and if omitted the entire metadata contents are returned as a JSON object.",
			`root.all_metadata = meta()`,
		),
	),
	true, metadataFunction,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func metadataFunction(args ...interface{}) (Function, error) {
	if len(args) > 0 {
		field := args[0].(string)
		return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			v := ctx.MsgBatch.Get(ctx.Index).Metadata().Get(field)
			if len(v) == 0 {
				return nil, &ErrRecoverable{
					Recovered: "",
					Err:       errors.New("metadata value not found"),
				}
			}
			return v, nil
		}, func(ctx TargetsContext) []TargetPath {
			return []TargetPath{
				NewTargetPath(TargetMetadata, field),
			}
		}), nil
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		kvs := map[string]interface{}{}
		ctx.MsgBatch.Get(ctx.Index).Metadata().Iter(func(k, v string) error {
			if len(v) > 0 {
				kvs[k] = v
			}
			return nil
		})
		return kvs, nil
	}, func(ctx TargetsContext) []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetMetadata),
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewDeprecatedFunctionSpec("nothing"),
	false, func(...interface{}) (Function, error) {
		return NewLiteralFunction(Nothing(nil)), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryGeneral, "random_int",
		"Generates a non-negative pseudo-random 64-bit integer. An optional integer argument can be provided in order to seed the random number generator.",
		NewExampleSpec("",
			`root.first = random_int()
root.second = random_int(1)`,
		),
	),
	true, randomIntFunction,
	ExpectOneOrZeroArgs(),
	ExpectIntArg(1),
)

func randomIntFunction(args ...interface{}) (Function, error) {
	seed := int64(0)
	if len(args) > 0 {
		var err error
		if seed, err = IGetInt(args[0]); err != nil {
			return nil, err
		}
	}
	r := rand.New(rand.NewSource(seed))
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return int64(r.Int()), nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp",
		"Returns the current time in a custom format specified by the argument. The format is defined by showing how the reference time, defined to be `Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.\n\nA fractional second is represented by adding a period and zeros to the end of the seconds section of layout string, as in `15:04:05.000` to format a time stamp with millisecond precision.",
		NewExampleSpec("",
			`root.received_at = timestamp("15:04:05")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
			return time.Now().Format(format), nil
		}, nil), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_utc",
		"The equivalent of `timestamp` except the time is printed as UTC instead of the local timezone.",
		NewExampleSpec("",
			`root.received_at = timestamp_utc("15:04:05")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
			return time.Now().In(time.UTC).Format(format), nil
		}, nil), nil
	},
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix",
		"Returns the current unix timestamp in seconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix()`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
			return time.Now().Unix(), nil
		}, nil), nil
	},
)

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryEnvironment, "timestamp_unix_nano",
		"Returns the current unix timestamp in nanoseconds.",
		NewExampleSpec("",
			`root.received_at = timestamp_unix_nano()`,
		),
	),
	false, func(...interface{}) (Function, error) {
		return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
			return time.Now().UnixNano(), nil
		}, nil), nil
	},
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
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
			`Error("failed to execute mapping query at line 1: unknown type")`,
		),
	),
	true, func(args ...interface{}) (Function, error) {
		msg := args[0].(string)
		return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
			return nil, errors.New(msg)
		}, nil), nil
	},
	ExpectNArgs(1),
	ExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewFunctionSpec(
		FunctionCategoryGeneral, "uuid_v4",
		"Generates a new RFC-4122 UUID each time it is invoked and prints a string representation.",
		NewExampleSpec("", `root.id = uuid_v4()`),
	),
	false, uuidFunction,
)

func uuidFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		u4, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		return u4.String(), nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunctionSpec(
	NewDeprecatedFunctionSpec("var"), true, varFunction,
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
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
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
	}, func(ctx TargetsContext) []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetVariable, name),
		}
	}), nil
}

//------------------------------------------------------------------------------
