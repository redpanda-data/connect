package query

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
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
			return iToBytes(rec.Recovered)
		}
		return nil
	}
	return iToBytes(v)
}

func (f closureFn) ToString(ctx FunctionContext) string {
	v, err := f(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return iToString(rec.Recovered)
		}
		return ""
	}
	return iToString(v)
}

//------------------------------------------------------------------------------

func literalFunction(v interface{}) Function {
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return v, nil
	})
}

//------------------------------------------------------------------------------

func getIntArg(args ...interface{}) (func(ctx FunctionContext) (int64, error), error) {
	var intArgFn func(ctx FunctionContext) (int64, error)
	if len(args) != 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", len(args))
	}
	switch t := args[0].(type) {
	case int64:
		intArgFn = func(ctx FunctionContext) (int64, error) { return t, nil }
	case float64:
		intArgFn = func(ctx FunctionContext) (int64, error) { return int64(t), nil }
	case Function:
		intArgFn = func(ctx FunctionContext) (int64, error) {
			res, err := t.Exec(ctx)
			if err != nil {
				return 0, err
			}
			switch t2 := res.(type) {
			case float64:
				return int64(t2), nil
			case int64:
				return t2, nil
			}
			return 0, fmt.Errorf("expected int param, received %T", res)
		}
	default:
		return nil, fmt.Errorf("expected int param, received %T", args[0])
	}
	return intArgFn, nil
}

func getStringArgOrEmpty(args ...interface{}) (func(ctx FunctionContext) (string, error), error) {
	if len(args) == 0 {
		return func(ctx FunctionContext) (string, error) { return "", nil }, nil
	}
	var strArgFn func(ctx FunctionContext) (string, error)
	if len(args) != 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", len(args))
	}
	switch t := args[0].(type) {
	case string:
		strArgFn = func(ctx FunctionContext) (string, error) { return t, nil }
	case Function:
		strArgFn = func(ctx FunctionContext) (string, error) {
			res, err := t.Exec(ctx)
			if err != nil {
				return "", err
			}
			if str, ok := res.(string); ok {
				return str, nil
			}
			return "", fmt.Errorf("expected string param, received %T", res)
		}
	default:
		return nil, fmt.Errorf("expected string param, received %T", args[0])
	}
	return strArgFn, nil
}

func jsonFunction(args ...interface{}) (Function, error) {
	strFn, err := getStringArgOrEmpty(args...)
	if err != nil {
		return nil, err
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		argPath, err := strFn(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       err,
			}
		}
		jPart, err := ctx.Msg.Get(ctx.Index).JSON()
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       err,
			}
		}
		gPart := gabs.Wrap(jPart)
		if len(argPath) > 0 {
			gPart = gPart.Path(argPath)
		}
		return iSanitize(gPart.Data()), nil
	}), nil
}

func metadataFunction(args ...interface{}) (Function, error) {
	strFn, err := getStringArgOrEmpty(args...)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			field, err := strFn(ctx)
			if err != nil {
				return nil, &ErrRecoverable{
					Recovered: "",
					Err:       err,
				}
			}
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
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", len(args))
	}
	strFn, err := getStringArgOrEmpty(args...)
	if err != nil {
		return nil, err
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		if ctx.Value == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       errors.New("context was undefined"),
			}
		}
		path, err := strFn(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       err,
			}
		}
		if len(path) == 0 {
			return *ctx.Value, nil
		}
		return gabs.Wrap(*ctx.Value).Path(path).Data(), nil
	}), nil
}

//------------------------------------------------------------------------------

var functions = map[string]func(args ...interface{}) (Function, error){
	"json":    jsonFunction,
	"meta":    metadataFunction,
	"error":   errorFunction,
	"content": contentFunction,
	"deleted": func(...interface{}) (Function, error) {
		return literalFunction(Delete(nil)), nil
	},
	"field": fieldFunction,
	"count": func(args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, errors.New("expected one parameter")
		}
		strFn, err := getStringArgOrEmpty(args...)
		if err != nil {
			return nil, err
		}
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			name, err := strFn(ctx)
			if err != nil {
				return nil, &ErrRecoverable{
					Recovered: int64(0),
					Err:       err,
				}
			}

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
	},
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
	"timestamp": func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			var ok bool
			if format, ok = args[0].(string); !ok {
				return nil, fmt.Errorf("expected string param, received %T", args[0])
			}
		}
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().Format(format), nil
		}), nil
	},
	"timestamp_utc": func(args ...interface{}) (Function, error) {
		format := "Mon Jan 2 15:04:05 -0700 MST 2006"
		if len(args) > 0 {
			var ok bool
			if format, ok = args[0].(string); !ok {
				return nil, fmt.Errorf("expected string param, received %T", args[0])
			}
		}
		return closureFn(func(_ FunctionContext) (interface{}, error) {
			return time.Now().In(time.UTC).Format(format), nil
		}), nil
	},
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
	"batch_size": func(...interface{}) (Function, error) {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
			return ctx.Msg.Len(), nil
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
