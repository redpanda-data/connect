package query

import (
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

func (f *fieldFunction) QueryTargets() []TargetPath {
	return []TargetPath{
		NewTargetPath(TargetValue, f.path...),
	}
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
func (l *Literal) QueryTargets() []TargetPath {
	return nil
}

// NewLiteralFunction creates a query function that returns a static, literal
// value.
func NewLiteralFunction(v interface{}) *Literal {
	return &Literal{v}
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("batch_index", false, func(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.Index), nil
	}, nil), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("batch_size", false, func(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.MsgBatch.Len()), nil
	}, nil), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("content", false, contentFunction)

func contentFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Get(), nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"count", true, countFunction,
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

var _ = RegisterFunction("deleted", false, func(...interface{}) (Function, error) {
	return NewLiteralFunction(Delete(nil)), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("error", false, errorFunction)

func errorFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey), nil
	}, nil), nil
}

var _ = RegisterFunction("errored", false, erroredFunction)

func erroredFunction(...interface{}) (Function, error) {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		return len(ctx.MsgBatch.Get(ctx.Index).Metadata().Get(types.FailFlagKey)) > 0, nil
	}, nil), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("hostname", false, hostnameFunction)

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

var _ = RegisterFunction(
	"json", true, jsonFunction,
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
	}, func() []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetValue, argPath...),
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"meta", true, metadataFunction,
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
		}, func() []TargetPath {
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
	}, func() []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetMetadata),
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("nothing", false, func(...interface{}) (Function, error) {
	return NewLiteralFunction(Nothing(nil)), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"random_int", true, randomIntFunction,
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

var _ = RegisterFunction("timestamp_unix", false, func(...interface{}) (Function, error) {
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return time.Now().Unix(), nil
	}, nil), nil
})

var _ = RegisterFunction("timestamp_unix_nano", false, func(...interface{}) (Function, error) {
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return time.Now().UnixNano(), nil
	}, nil), nil
})

var _ = RegisterFunction("timestamp", true, func(args ...interface{}) (Function, error) {
	format := "Mon Jan 2 15:04:05 -0700 MST 2006"
	if len(args) > 0 {
		format = args[0].(string)
	}
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return time.Now().Format(format), nil
	}, nil), nil
}, ExpectOneOrZeroArgs(), ExpectStringArg(0))

var _ = RegisterFunction("timestamp_utc", true, func(args ...interface{}) (Function, error) {
	format := "Mon Jan 2 15:04:05 -0700 MST 2006"
	if len(args) > 0 {
		format = args[0].(string)
	}
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return time.Now().In(time.UTC).Format(format), nil
	}, nil), nil
}, ExpectOneOrZeroArgs(), ExpectStringArg(0))

//------------------------------------------------------------------------------

var _ = RegisterFunction("throw", true, func(args ...interface{}) (Function, error) {
	msg := args[0].(string)
	return ClosureFunction(func(_ FunctionContext) (interface{}, error) {
		return nil, errors.New(msg)
	}, nil), nil
}, ExpectNArgs(1), ExpectStringArg(0))

//------------------------------------------------------------------------------

var _ = RegisterFunction("uuid_v4", false, uuidFunction)

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

var _ = RegisterFunction(
	"var", true, varFunction,
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
	}, func() []TargetPath {
		return []TargetPath{
			NewTargetPath(TargetVariable, name),
		}
	}), nil
}

//------------------------------------------------------------------------------
