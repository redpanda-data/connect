package expression

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

func deprecatedFunction(input []rune) parserResult {
	var targetFunc, arg string

	for i := 0; i < len(input); i++ {
		if input[i] == ':' {
			targetFunc = string(input[:i])
			arg = string(input[i+1:])
		}
	}
	if len(targetFunc) == 0 {
		targetFunc = string(input)
	}

	ftor, exists := deprecatedFunctions[targetFunc]
	if !exists {
		return parserResult{
			// Make no suggestions, we want users to move off of these functions
			Err:       expectedErr{},
			Remaining: input,
		}
	}
	return parserResult{
		Result:    dynamicResolver(ftor(arg)),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------

func jsonFieldFunction(arg string) dynamicResolverFunc {
	return func(i int, msg Message, escaped, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
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

func metadataFunction(arg string) dynamicResolverFunc {
	return func(i int, msg Message, escaped, legacy bool) []byte {
		part := 0
		if !legacy {
			part = i
		}
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

func metadataMapFunction(arg string) dynamicResolverFunc {
	return func(i int, msg Message, escaped, legacy bool) []byte {
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

func errorFunction(arg string) dynamicResolverFunc {
	return func(i int, msg Message, escaped, legacy bool) []byte {
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

func contentFunction(arg string) dynamicResolverFunc {
	return func(i int, msg Message, escaped, legacy bool) []byte {
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

var counters = map[string]uint64{}
var countersMux = &sync.Mutex{}

var deprecatedFunctions = map[string]func(arg string) dynamicResolverFunc{
	"timestamp_unix_nano": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			return []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
		}
	},
	"timestamp_unix": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
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
	"timestamp": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			if len(arg) == 0 {
				arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
			}
			return []byte(time.Now().Format(arg))
		}
	},
	"timestamp_utc": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			if len(arg) == 0 {
				arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
			}
			return []byte(time.Now().In(time.UTC).Format(arg))
		}
	},
	"hostname": func(_ string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			hn, _ := os.Hostname()
			return []byte(hn)
		}
	},
	"echo": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			return []byte(arg)
		}
	},
	"count": func(arg string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			countersMux.Lock()
			defer countersMux.Unlock()

			var count uint64
			var exists bool

			if count, exists = counters[arg]; exists {
				count++
			} else {
				count = 1
			}
			counters[arg] = count

			return []byte(strconv.FormatUint(count, 10))
		}
	},
	"error":                errorFunction,
	"content":              contentFunction,
	"json_field":           jsonFieldFunction,
	"metadata":             metadataFunction,
	"metadata_json_object": metadataMapFunction,
	"batch_size": func(_ string) dynamicResolverFunc {
		return func(_ int, m Message, _, _ bool) []byte {
			return strconv.AppendInt(nil, int64(m.Len()), 10)
		}
	},
	"uuid_v4": func(_ string) dynamicResolverFunc {
		return func(_ int, _ Message, _, _ bool) []byte {
			u4, err := uuid.NewV4()
			if err != nil {
				panic(err)
			}
			return []byte(u4.String())
		}
	},
}

//------------------------------------------------------------------------------
