package text

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a function interpolator, it
// allows the function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

//------------------------------------------------------------------------------

func jsonFieldFunction(msg Message, index int, arg string) []byte {
	part := index
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

func metadataFunction(msg Message, index int, arg string) []byte {
	part := index
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

func metadataMapFunction(msg Message, index int, arg string) []byte {
	part := index
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

func errorFunction(msg Message, index int, arg string) []byte {
	part := index
	if len(arg) > 0 {
		partB, err := strconv.ParseInt(arg, 10, 64)
		if err == nil {
			part = int(partB)
		}
	}
	return []byte(msg.Get(part).Metadata().Get(types.FailFlagKey))
}

func contentFunction(msg Message, index int, arg string) []byte {
	part := index
	if len(arg) > 0 {
		partB, err := strconv.ParseInt(arg, 10, 64)
		if err == nil {
			part = int(partB)
		}
	}
	return msg.Get(part).Get()
}

//------------------------------------------------------------------------------

var functionRegex *regexp.Regexp
var escapedFunctionRegex *regexp.Regexp

func init() {
	var err error
	if functionRegex, err = regexp.Compile(`\${![a-z0-9_]+(:[^}]+)?}`); err != nil {
		panic(err)
	}
	if escapedFunctionRegex, err = regexp.Compile(`\${({![a-z0-9_]+(:[^}]+)?})}`); err != nil {
		panic(err)
	}
}

var counters = map[string]uint64{}
var countersMux = &sync.Mutex{}

var functionVars = map[string]func(msg Message, index int, arg string) []byte{
	"timestamp_unix_nano": func(_ Message, _ int, arg string) []byte {
		return []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
	},
	"timestamp_unix": func(_ Message, _ int, arg string) []byte {
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
	},
	"timestamp": func(_ Message, _ int, arg string) []byte {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return []byte(time.Now().Format(arg))
	},
	"timestamp_utc": func(_ Message, _ int, arg string) []byte {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return []byte(time.Now().In(time.UTC).Format(arg))
	},
	"hostname": func(_ Message, _ int, arg string) []byte {
		hn, _ := os.Hostname()
		return []byte(hn)
	},
	"echo": func(_ Message, _ int, arg string) []byte {
		return []byte(arg)
	},
	"count": func(_ Message, _ int, arg string) []byte {
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
	},
	"error":                errorFunction,
	"content":              contentFunction,
	"json_field":           jsonFieldFunction,
	"metadata":             metadataFunction,
	"metadata_json_object": metadataMapFunction,
	"batch_size": func(m Message, _ int, _ string) []byte {
		return strconv.AppendInt(nil, int64(m.Len()), 10)
	},
	"uuid_v4": func(_ Message, _ int, _ string) []byte {
		u4, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		return []byte(u4.String())
	},
}

// ContainsFunctionVariables returns true if inBytes contains function variable
// replace patterns.
func ContainsFunctionVariables(inBytes []byte) bool {
	return functionRegex.Find(inBytes) != nil || escapedFunctionRegex.Find(inBytes) != nil
}

func escapeBytes(in []byte) []byte {
	quoted := strconv.Quote(string(in))
	if len(quoted) < 3 {
		return in
	}
	return []byte(quoted[1 : len(quoted)-1])
}

// ReplaceFunctionVariables will search a blob of data for the pattern
// `${!foo}`, where `foo` is a function name.
//
// For each aforementioned pattern found in the blob the contents of the
// respective function will be run and will replace the pattern.
//
// Some functions are able to extract contents and metadata from a message, and
// so a message must be supplied.
func ReplaceFunctionVariables(msg Message, inBytes []byte) []byte {
	return ReplaceFunctionVariablesFor(msg, 0, inBytes)
}

// ReplaceFunctionVariablesFor will search a blob of data for the pattern
// `${!foo}`, where `foo` is a function name.
//
// For each aforementioned pattern found in the blob the contents of the
// respective function will be run and will replace the pattern.
//
// Some functions are able to extract contents and metadata from a message, and
// so a message must be supplied along with the specific index of the message
// part that this function should be resolved for.
func ReplaceFunctionVariablesFor(msg Message, index int, inBytes []byte) []byte {
	return replaceFunctionVariables(msg, index, false, inBytes)
}

// ReplaceFunctionVariablesEscaped will search a blob of data for the pattern
// `${!foo}`, where `foo` is a function name.
//
// For each aforementioned pattern found in the blob the contents of the
// respective function will be run and will replace the pattern.
//
// The contents of the swapped pattern is escaped such that it can be safely
// injected within the contents of a JSON object.
//
// Some functions are able to extract contents and metadata from a message, and
// so a message must be supplied.
func ReplaceFunctionVariablesEscaped(msg Message, inBytes []byte) []byte {
	return ReplaceFunctionVariablesEscapedFor(msg, 0, inBytes)
}

// ReplaceFunctionVariablesEscapedFor will search a blob of data for the pattern
// `${!foo}`, where `foo` is a function name.
//
// For each aforementioned pattern found in the blob the contents of the
// respective function will be run and will replace the pattern.
//
// The contents of the swapped pattern is escaped such that it can be safely
// injected within the contents of a JSON object.
//
// Some functions are able to extract contents and metadata from a message, and
// so a message must be supplied along with the specific index of the message
// part that this function should be resolved for.
func ReplaceFunctionVariablesEscapedFor(msg Message, index int, inBytes []byte) []byte {
	return replaceFunctionVariables(msg, index, true, inBytes)
}

func replaceFunctionVariables(
	msg Message, index int, escape bool, inBytes []byte,
) []byte {
	replaced := functionRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		if len(content) > 4 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				targetFunc := string(content[3 : len(content)-1])
				if ftor, exists := functionVars[targetFunc]; exists {
					if escape {
						return escapeBytes(ftor(msg, index, ""))
					}
					return ftor(msg, index, "")
				}
			} else {
				targetFunc := string(content[3:colonIndex])
				argVal := string(content[colonIndex+1 : len(content)-1])
				if ftor, exists := functionVars[targetFunc]; exists {
					if escape {
						return escapeBytes(ftor(msg, index, argVal))
					}
					return ftor(msg, index, argVal)
				}
			}
		}
		return content
	})
	replaced = escapedFunctionRegex.ReplaceAll(replaced, []byte(`$$$1`))
	return replaced
}

//------------------------------------------------------------------------------
