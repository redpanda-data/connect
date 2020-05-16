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

type fieldFunction struct {
	path []string
}

func (g *fieldFunction) Exec(ctx FunctionContext) (interface{}, error) {
	if ctx.Value == nil {
		return nil, &ErrRecoverable{
			Recovered: nil,
			Err:       errors.New("context was undefined"),
		}
	}
	if len(g.path) == 0 {
		return *ctx.Value, nil
	}
	return gabs.Wrap(*ctx.Value).S(g.path...).Data(), nil
}

func fieldFunctionCtor(args ...interface{}) (Function, error) {
	var path []string
	if len(args) > 0 {
		path = gabs.DotPathToSlice(args[0].(string))
	}
	return &fieldFunction{path}, nil
}

//------------------------------------------------------------------------------

type literal struct {
	Value interface{}
}

func (l *literal) Exec(ctx FunctionContext) (interface{}, error) {
	return l.Value, nil
}

func literalFunction(v interface{}) Function {
	return &literal{v}
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("batch_index", false, func(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.Index), nil
	}), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("batch_size", false, func(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return int64(ctx.Msg.Len()), nil
	}), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("content", false, contentFunction)

func contentFunction(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return ctx.Msg.Get(ctx.Index).Get(), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"count", true, countFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func countFunction(args ...interface{}) (Function, error) {
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
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("deleted", false, func(...interface{}) (Function, error) {
	return literalFunction(Delete(nil)), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("error", false, errorFunction)

func errorFunction(...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return ctx.Msg.Get(ctx.Index).Metadata().Get(types.FailFlagKey), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction("hostname", false, hostnameFunction)

func hostnameFunction(...interface{}) (Function, error) {
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

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"meta", true, metadataFunction,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

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

//------------------------------------------------------------------------------

var _ = RegisterFunction("nothing", false, func(...interface{}) (Function, error) {
	return literalFunction(Nothing(nil)), nil
})

//------------------------------------------------------------------------------

var _ = RegisterFunction("timestamp_unix", false, func(...interface{}) (Function, error) {
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return time.Now().Unix(), nil
	}), nil
})

var _ = RegisterFunction("timestamp_unix_nano", false, func(...interface{}) (Function, error) {
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return time.Now().UnixNano(), nil
	}), nil
})

var _ = RegisterFunction("timestamp", true, func(args ...interface{}) (Function, error) {
	format := "Mon Jan 2 15:04:05 -0700 MST 2006"
	if len(args) > 0 {
		format = args[0].(string)
	}
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return time.Now().Format(format), nil
	}), nil
}, ExpectOneOrZeroArgs(), ExpectStringArg(0))

var _ = RegisterFunction("timestamp_utc", true, func(args ...interface{}) (Function, error) {
	format := "Mon Jan 2 15:04:05 -0700 MST 2006"
	if len(args) > 0 {
		format = args[0].(string)
	}
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		return time.Now().In(time.UTC).Format(format), nil
	}), nil
}, ExpectOneOrZeroArgs(), ExpectStringArg(0))

//------------------------------------------------------------------------------

var _ = RegisterFunction("uuid_v4", false, uuidFunction)

func uuidFunction(...interface{}) (Function, error) {
	return closureFn(func(_ FunctionContext) (interface{}, error) {
		u4, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		return u4.String(), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterFunction(
	"var", true, varFunction,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

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
