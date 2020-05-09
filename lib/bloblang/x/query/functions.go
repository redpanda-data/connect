package query

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

// ErrRecoverable represents a function execution error that can optionally be
// recovered into a zero-value.
type ErrRecoverable struct {
	Recovered interface{}
	Err       error
}

// Error implements the standard error interface.
func (e *ErrRecoverable) Error() string {
	return e.Err.Error()
}

//------------------------------------------------------------------------------

type closureFn func(ctx FunctionContext) (interface{}, error)

func (f closureFn) Exec(ctx FunctionContext) (interface{}, error) {
	return f(ctx)
}

func (f closureFn) ToBytes(ctx FunctionContext) []byte {
	v, err := f(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToBytes(rec.Recovered)
		}
		return nil
	}
	return IToBytes(v)
}

func (f closureFn) ToString(ctx FunctionContext) string {
	v, err := f(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToString(rec.Recovered)
		}
		return ""
	}
	return IToString(v)
}

//------------------------------------------------------------------------------

func literalFunction(v interface{}) Function {
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return v, nil
	})
}

//------------------------------------------------------------------------------

func withDynamicArgs(args []interface{}, fn functionCtor) Function {
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
		dynFunc, err := fn(dynArgs...)
		if err != nil {
			return nil, err
		}
		return dynFunc.Exec(ctx)
	})
}

func enableDynamicArgs(fn functionCtor) functionCtor {
	return func(args ...interface{}) (Function, error) {
		for _, arg := range args {
			if _, isDyn := arg.(Function); isDyn {
				return withDynamicArgs(args, fn), nil
			}
		}
		return fn(args...)
	}
}

//------------------------------------------------------------------------------

type argCheckFn func(args []interface{}) error

func checkArgs(fn functionCtor, checks ...argCheckFn) functionCtor {
	return func(args ...interface{}) (Function, error) {
		for _, check := range checks {
			if err := check(args); err != nil {
				return nil, err
			}
		}
		return fn(args...)
	}
}

func expectOneOrZeroArgs() argCheckFn {
	return func(args []interface{}) error {
		if len(args) > 1 {
			return fmt.Errorf("expected one or zero parameters, received: %v", len(args))
		}
		return nil
	}
}

func expectNArgs(i int) argCheckFn {
	return func(args []interface{}) error {
		if len(args) != i {
			return fmt.Errorf("expected %v parameters, received: %v", i, len(args))
		}
		return nil
	}
}

func expectStringArg(i int) argCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		if _, isStr := args[i].(string); !isStr {
			return fmt.Errorf("expected string param, received %T", args[i])
		}
		return nil
	}
}

func expectIntArg(i int) argCheckFn {
	return func(args []interface{}) error {
		if len(args) <= i {
			return nil
		}
		switch t := args[i].(type) {
		case int64:
		case float64:
			args[i] = int64(t)
		default:
			return fmt.Errorf("expected string param, received %T", args[i])
		}
		return nil
	}
}

//------------------------------------------------------------------------------

func jsonFunction(args ...interface{}) (Function, error) {
	var argPath []string
	if len(args) > 0 {
		argPath = gabs.DotPathToSlice(args[0].(string))
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		jPart, err := ctx.Msg.Get(ctx.Index).JSON()
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
	}), nil
}

func metadataFunction(args ...interface{}) (Function, error) {
	if len(args) > 0 {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			field := args[0].(string)
			v := ctx.Msg.Get(ctx.Index).Metadata().Get(field)
			if len(v) == 0 {
				return nil, &ErrRecoverable{
					Recovered: "",
					Err:       errors.New("metadata value not found"),
				}
			}
			return v, nil
		}), nil
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		kvs := map[string]interface{}{}
		ctx.Msg.Get(ctx.Index).Metadata().Iter(func(k, v string) error {
			if len(v) > 0 {
				kvs[k] = v
			}
			return nil
		})
		return kvs, nil
	}), nil
}

func errorFunction(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return ctx.Msg.Get(ctx.Index).Metadata().Get(types.FailFlagKey), nil
	}), nil
}

func contentFunction(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return ctx.Msg.Get(ctx.Index).Get(), nil
	}), nil
}

func fieldFunction(args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		if ctx.Value == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       errors.New("context was undefined"),
			}
		}
		if len(args) == 0 {
			return *ctx.Value, nil
		}
		path := args[0].(string)
		return gabs.Wrap(*ctx.Value).Path(path).Data(), nil
	}), nil
}

func varFunction(args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		if ctx.Vars == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       errors.New("variables were undefined"),
			}
		}
		name := args[0].(string)
		if res, ok := ctx.Vars[name]; ok {
			return res, nil
		}
		return nil, &ErrRecoverable{
			Recovered: nil,
			Err:       fmt.Errorf("variable '%v' undefined", name),
		}
	}), nil
}

//------------------------------------------------------------------------------

type functionCtor func(args ...interface{}) (Function, error)

var functions = map[string]functionCtor{
	"json": enableDynamicArgs(checkArgs(
		jsonFunction,
		expectOneOrZeroArgs(),
		expectStringArg(0),
	)),
	"meta": enableDynamicArgs(checkArgs(
		metadataFunction,
		expectOneOrZeroArgs(),
		expectStringArg(0),
	)),
	"error":   errorFunction,
	"content": contentFunction,
	"deleted": func(...interface{}) (Function, error) {
		return literalFunction(Delete(nil)), nil
	},
	"nothing": func(...interface{}) (Function, error) {
		return literalFunction(Nothing(nil)), nil
	},
	"var": enableDynamicArgs(checkArgs(
		varFunction,
		expectNArgs(1),
		expectStringArg(0),
	)),
	"count": enableDynamicArgs(checkArgs(func(args ...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
		}), nil
	}, expectNArgs(1), expectStringArg(0))),
	"timestamp_unix": func(...interface{}) (Function, error) {
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().Unix(), nil
		}), nil
	},
	"timestamp_unix_nano": func(...interface{}) (Function, error) {
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().UnixNano(), nil
		}), nil
	},
	"timestamp": enableDynamicArgs(checkArgs(func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().Format(format), nil
		}), nil
	}, expectStringArg(0))),
	"timestamp_utc": enableDynamicArgs(checkArgs(func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			format = args[0].(string)
		}
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().In(time.UTC).Format(format), nil
		}), nil
	}, expectStringArg(0))),
	"hostname": func(...interface{}) (Function, error) {
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			hn, err := os.Hostname()
			if err != nil {
				return nil, &ErrRecoverable{
					Recovered: "",
					Err:       err,
				}
			}
			return hn, err
		}), nil
	},
	"batch_index": func(...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			return int64(ctx.Index), nil
		}), nil
	},
	"batch_size": func(...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			return int64(ctx.Msg.Len()), nil
		}), nil
	},
	"uuid_v4": func(...interface{}) (Function, error) {
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			u4, err := uuid.NewV4()
			if err != nil {
				panic(err)
			}
			return u4.String(), nil
		}), nil
	},
}

//------------------------------------------------------------------------------
