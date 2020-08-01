package query

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
)

//------------------------------------------------------------------------------

func wrapDeprecatedFunction(d deprecatedFunction) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		return d(ctx.Index, ctx.MsgBatch, ctx.Legacy), nil
	})
}

type deprecatedFunction func(int, MessageBatch, bool) []byte

func jsonFieldFunction(arg string) deprecatedFunction {
	return func(i int, msg MessageBatch, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
		arg := arg
		if argIndex := strings.LastIndex(arg, ","); argIndex > 0 && len(arg) > argIndex {
			partB, err := strconv.ParseInt(arg[argIndex+1:], 10, 64)
			if err == nil {
				part = int(partB)
			}
			arg = arg[:argIndex]
		}
		jPart, err := msg.Get(part).JSON()
		if err != nil {
			return []byte("null")
		}
		gPart := gabs.Wrap(jPart)
		if len(arg) > 0 {
			gPart = gPart.Path(arg)
		}
		switch t := gPart.Data().(type) {
		case string:
			return []byte(t)
		case nil:
			return []byte(`null`)
		}
		return gPart.Bytes()
	}
}

func deprecatedMetadataFunction(arg string) deprecatedFunction {
	return func(i int, msg MessageBatch, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
		arg := arg
		if argIndex := strings.LastIndex(arg, ","); argIndex > 0 && len(arg) > argIndex {
			partB, err := strconv.ParseInt(arg[argIndex+1:], 10, 64)
			if err == nil {
				part = int(partB)
			}
			arg = arg[:argIndex]
		}
		if len(arg) == 0 {
			return []byte("")
		}
		meta := msg.Get(part).Metadata()
		return []byte(meta.Get(arg))
	}
}

func deprecatedMetadataMapFunction(arg string) deprecatedFunction {
	return func(i int, msg MessageBatch, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
		if len(arg) > 0 {
			partB, err := strconv.ParseInt(arg, 10, 64)
			if err == nil {
				part = int(partB)
			}
		}
		kvs := map[string]string{}
		msg.Get(part).Metadata().Iter(func(k, v string) error {
			kvs[k] = v
			return nil
		})
		result, err := json.Marshal(kvs)
		if err != nil {
			return []byte("")
		}
		return result
	}
}

func deprecatedErrorFunction(arg string) deprecatedFunction {
	return func(i int, msg MessageBatch, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
		if len(arg) > 0 {
			partB, err := strconv.ParseInt(arg, 10, 64)
			if err == nil {
				part = int(partB)
			}
		}
		res := []byte(msg.Get(part).Metadata().Get(types.FailFlagKey))
		return res
	}
}

func deprecatedContentFunction(arg string) deprecatedFunction {
	return func(i int, msg MessageBatch, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
		if len(arg) > 0 {
			partB, err := strconv.ParseInt(arg, 10, 64)
			if err == nil {
				part = int(partB)
			}
		}
		return msg.Get(part).Get()
	}
}

//------------------------------------------------------------------------------

var counters = map[string]int64{}
var countersMux = &sync.Mutex{}

var deprecatedFunctions = map[string]func(arg string) deprecatedFunction{
	"timestamp_unix_nano": func(arg string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			return []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
		}
	},
	"timestamp_unix": func(arg string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			tNow := time.Now()
			precision, _ := strconv.ParseInt(arg, 10, 64)
			tStr := strconv.FormatInt(tNow.Unix(), 10)
			if precision > 0 {
				nanoStr := strconv.FormatInt(int64(tNow.Nanosecond()), 10)
				if lNano := int64(len(nanoStr)); precision >= lNano {
					precision = lNano - 1
				}
				tStr = tStr + "." + nanoStr[:precision]
			}
			return []byte(tStr)
		}
	},
	"timestamp": func(arg string) deprecatedFunction {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return func(_ int, _ MessageBatch, _ bool) []byte {
			return []byte(time.Now().Format(arg))
		}
	},
	"timestamp_utc": func(arg string) deprecatedFunction {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return func(_ int, _ MessageBatch, _ bool) []byte {
			return []byte(time.Now().In(time.UTC).Format(arg))
		}
	},
	"hostname": func(_ string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			hn, _ := os.Hostname()
			return []byte(hn)
		}
	},
	"echo": func(arg string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			return []byte(arg)
		}
	},
	"count": func(arg string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			countersMux.Lock()
			defer countersMux.Unlock()

			var count int64
			var exists bool

			if count, exists = counters[arg]; exists {
				count++
			} else {
				count = 1
			}
			counters[arg] = count

			return []byte(strconv.FormatInt(count, 10))
		}
	},
	"error":                deprecatedErrorFunction,
	"content":              deprecatedContentFunction,
	"json_field":           jsonFieldFunction,
	"metadata":             deprecatedMetadataFunction,
	"metadata_json_object": deprecatedMetadataMapFunction,
	"batch_size": func(_ string) deprecatedFunction {
		return func(_ int, m MessageBatch, _ bool) []byte {
			return strconv.AppendInt(nil, int64(m.Len()), 10)
		}
	},
	"uuid_v4": func(_ string) deprecatedFunction {
		return func(_ int, _ MessageBatch, _ bool) []byte {
			u4, err := uuid.NewV4()
			if err != nil {
				panic(err)
			}
			return []byte(u4.String())
		}
	},
}

//------------------------------------------------------------------------------
