// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

func jsonFieldFunction(msg Message, arg string) []byte {
	args := strings.Split(arg, ",")
	part := 0
	if len(args) == 2 {
		partB, err := strconv.ParseInt(args[1], 10, 64)
		if err == nil {
			part = int(partB)
		}
	}
	jPart, err := msg.Get(part).JSON()
	if err != nil {
		return []byte("null")
	}
	gPart := gabs.Wrap(jPart)
	if len(args) > 0 {
		gPart = gPart.Path(args[0])
	}
	switch t := gPart.Data().(type) {
	case string:
		return []byte(t)
	case nil:
		return []byte(`null`)
	}
	return gPart.Bytes()
}

func metadataFunction(msg Message, arg string) []byte {
	if len(arg) == 0 {
		return []byte("")
	}
	args := strings.Split(arg, ",")
	part := 0
	if len(args) == 2 {
		partB, err := strconv.ParseInt(args[1], 10, 64)
		if err == nil {
			part = int(partB)
		}
	}
	meta := msg.Get(part).Metadata()
	return []byte(meta.Get(args[0]))
}

func metadataMapFunction(msg Message, arg string) []byte {
	part := 0
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

func errorFunction(msg Message, arg string) []byte {
	part := 0
	if len(arg) > 0 {
		partB, err := strconv.ParseInt(arg, 10, 64)
		if err == nil {
			part = int(partB)
		}
	}
	return []byte(msg.Get(part).Metadata().Get(types.FailFlagKey))
}

func contentFunction(msg Message, arg string) []byte {
	part := 0
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

var functionVars = map[string]func(msg Message, arg string) []byte{
	"timestamp_unix_nano": func(_ Message, arg string) []byte {
		return []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
	},
	"timestamp_unix": func(_ Message, arg string) []byte {
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
	"timestamp": func(_ Message, arg string) []byte {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return []byte(time.Now().Format(arg))
	},
	"timestamp_utc": func(_ Message, arg string) []byte {
		if len(arg) == 0 {
			arg = "Mon Jan 2 15:04:05 -0700 MST 2006"
		}
		return []byte(time.Now().In(time.UTC).Format(arg))
	},
	"hostname": func(_ Message, arg string) []byte {
		hn, _ := os.Hostname()
		return []byte(hn)
	},
	"echo": func(_ Message, arg string) []byte {
		return []byte(arg)
	},
	"count": func(_ Message, arg string) []byte {
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
	"batch_size": func(m Message, _ string) []byte {
		return strconv.AppendInt(nil, int64(m.Len()), 10)
	},
	"uuid_v4": func(_ Message, _ string) []byte {
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
	quoted := strconv.QuoteToASCII(string(in))
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
	return replaceFunctionVariables(msg, false, inBytes)
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
	return replaceFunctionVariables(msg, true, inBytes)
}

func replaceFunctionVariables(msg Message, escape bool, inBytes []byte) []byte {
	replaced := functionRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		if len(content) > 4 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				targetFunc := string(content[3 : len(content)-1])
				if ftor, exists := functionVars[targetFunc]; exists {
					if escape {
						return escapeBytes(ftor(msg, ""))
					}
					return ftor(msg, "")
				}
			} else {
				targetFunc := string(content[3:colonIndex])
				argVal := string(content[colonIndex+1 : len(content)-1])
				if ftor, exists := functionVars[targetFunc]; exists {
					if escape {
						return escapeBytes(ftor(msg, argVal))
					}
					return ftor(msg, argVal)
				}
			}
		}
		return content
	})
	replaced = escapedFunctionRegex.ReplaceAll(replaced, []byte(`$$$1`))
	return replaced
}

//------------------------------------------------------------------------------
