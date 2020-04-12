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

type closureFn func(index int, msg Message, legacy bool) (interface{}, error)

func (f closureFn) Exec(index int, msg Message, legacy bool) (interface{}, error) {
	return f(index, msg, legacy)
}

func (f closureFn) ToBytes(index int, msg Message, legacy bool) []byte {
	v, err := f(index, msg, legacy)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return iToBytes(rec.Recovered)
		}
		return nil
	}
	return iToBytes(v)
}

func (f closureFn) ToString(index int, msg Message, legacy bool) string {
	v, err := f(index, msg, legacy)
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
	return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
		return v, nil
	})
}

func makeJSONFunction(argIndex *int, argPath string) Function {
	return closureFn(func(index int, msg Message, _ bool) (interface{}, error) {
		part := index
		if argIndex != nil {
			part = *argIndex
		}
		jPart, err := msg.Get(part).JSON()
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
	})
}

func jsonFunction(args ...interface{}) (Function, error) {
	var argPath string
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", args)
	}
	if len(args) == 1 {
		var ok bool
		if argPath, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}
	}
	return makeJSONFunction(nil, argPath), nil
}

func jsonFromFunction(args ...interface{}) (Function, error) {
	var argIndex *int
	var argPath string
	if len(args) != 2 {
		return nil, fmt.Errorf("expected two arguments, received: %v", args)
	}

	i64, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("expected int param, received %T", args[0])
	}

	i := int(i64)
	argIndex = &i
	if argPath, ok = args[1].(string); !ok {
		return nil, fmt.Errorf("expected string param, received %T", args[1])
	}
	return makeJSONFunction(argIndex, argPath), nil
}

func makeMetadataFunction(argIndex *int, field string) Function {
	if len(field) > 0 {
		return closureFn(func(index int, msg Message, _ bool) (interface{}, error) {
			part := index
			if argIndex != nil {
				part = *argIndex
			}
			meta := msg.Get(part).Metadata()
			return meta.Get(field), nil
		})
	}
	return closureFn(func(index int, msg Message, _ bool) (interface{}, error) {
		part := index
		if argIndex != nil {
			part = *argIndex
		}
		kvs := map[string]interface{}{}
		msg.Get(part).Metadata().Iter(func(k, v string) error {
			kvs[k] = v
			return nil
		})
		return kvs, nil
	})
}

func metadataFunction(args ...interface{}) (Function, error) {
	var argField string
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", args)
	}
	if len(args) == 1 {
		var ok bool
		if argField, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}
	}
	return makeMetadataFunction(nil, argField), nil
}

func metadataFromFunction(args ...interface{}) (Function, error) {
	var argIndex *int
	var argField string
	if len(args) < 1 {
		return nil, errors.New("expected one or two arguments, received none")
	}

	i64, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("expected int param, received %T", args[0])
	}

	i := int(i64)
	argIndex = &i
	if len(args) > 1 {
		if argField, ok = args[1].(string); !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[1])
		}
	}
	return makeMetadataFunction(argIndex, argField), nil
}

func errorFunction(...interface{}) (Function, error) {
	return closureFn(func(i int, msg Message, legacy bool) (interface{}, error) {
		return msg.Get(i).Metadata().Get(types.FailFlagKey), nil
	}), nil
}

func errorFromFunction(args ...interface{}) (Function, error) {
	if len(args) < 1 {
		return nil, errors.New("expected one or two arguments, received none")
	}

	i64, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("expected int param, received %T", args[0])
	}

	index := int(i64)
	return closureFn(func(_ int, msg Message, legacy bool) (interface{}, error) {
		return msg.Get(index).Metadata().Get(types.FailFlagKey), nil
	}), nil
}

func contentFunction(...interface{}) (Function, error) {
	return closureFn(func(i int, msg Message, legacy bool) (interface{}, error) {
		return msg.Get(i).Get(), nil
	}), nil
}

func contentFromFunction(args ...interface{}) (Function, error) {
	if len(args) < 1 {
		return nil, errors.New("expected one or two params, received none")
	}

	i64, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("expected int param, received %T", args[0])
	}

	index := int(i64)
	return closureFn(func(_ int, msg Message, legacy bool) (interface{}, error) {
		return msg.Get(index).Get(), nil
	}), nil
}

//------------------------------------------------------------------------------

var functions = map[string]func(args ...interface{}) (Function, error){
	"json":         jsonFunction,
	"json_from":    jsonFromFunction,
	"meta":         metadataFunction,
	"meta_from":    metadataFromFunction,
	"error":        errorFunction,
	"error_from":   errorFromFunction,
	"content":      contentFunction,
	"content_from": contentFromFunction,
	"count": func(args ...interface{}) (Function, error) {
		if len(args) != 1 {
			return nil, errors.New("expected one parameter")
		}
		name, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string param, received %T", args[0])
		}

		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
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
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
			return time.Now().Unix(), nil
		}), nil
	},
	"timestamp_unix_nano": func(...interface{}) (Function, error) {
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
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
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
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
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
			return time.Now().In(time.UTC).Format(format), nil
		}), nil
	},
	"hostname": func(...interface{}) (Function, error) {
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
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
		return closureFn(func(_ int, m Message, _ bool) (interface{}, error) {
			return m.Len(), nil
		}), nil
	},
	"uuid_v4": func(...interface{}) (Function, error) {
		return closureFn(func(_ int, _ Message, _ bool) (interface{}, error) {
			u4, err := uuid.NewV4()
			if err != nil {
				panic(err)
			}
			return u4.String(), nil
		}), nil
	},
}

//------------------------------------------------------------------------------
