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

func jsonFunction(args ...interface{}) (Function, error) {
	var argPath string
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", len(args))
	}
	if len(args) == 1 {
		var ok bool
		if argPath, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}
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
			gPart = gPart.Path(argPath)
		}
		return iSanitize(gPart.Data()), nil
	}), nil
}

func metadataFunction(args ...interface{}) (Function, error) {
	var field string
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", len(args))
	}
	if len(args) == 1 {
		var ok bool
		if field, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}
	}
	if len(field) > 0 {
		return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
	var pathSegments []string
	if len(args) > 0 {
		path, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}
		pathSegments = gabs.DotPathToSlice(path)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		if ctx.Value == nil {
			return nil, &ErrRecoverable{
				Recovered: nil,
				Err:       errors.New("context was undefined"),
			}
		}
		if len(pathSegments) == 0 {
			return *ctx.Value, nil
		}
		return gabs.Wrap(*ctx.Value).S(pathSegments...).Data(), nil
	}), nil
}

//------------------------------------------------------------------------------

var functions = map[string]func(args ...interface{}) (Function, error){
	"json":    jsonFunction,
	"meta":    metadataFunction,
	"error":   errorFunction,
	"content": contentFunction,
	"field":   fieldFunction,
	"count": func(args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, errors.New("expected one parameter")
		}
		name, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}

		return closureFn(func(_ FunctionContext) (interface{}, error) {
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
