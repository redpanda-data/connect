// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
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

package processor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/benhoyt/goawk/interp"
	"github.com/benhoyt/goawk/parser"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

var varInvalidRegexp *regexp.Regexp

func init() {
	varInvalidRegexp = regexp.MustCompile(`[^a-zA-Z0-9_]`)

	Constructors[TypeAWK] = TypeSpec{
		constructor: NewAWK,
		description: `
Executes an AWK program on messages by feeding contents as the input based on a
codec and replaces the contents with the result. If the result is empty (nothing
is printed by the program) then the original message contents remain unchanged.

Comes with a wide range of [custom functions](./awk_functions.md) for accessing
message metadata, json fields, printing logs, etc. These functions can be
overridden by functions within the program.

### Codecs

A codec can be specified that determines how the contents of the message are fed
into the program. This does not change the custom functions.

` + "`none`" + `

An empty string is fed into the program. Functions can still be used in order to
extract and mutate metadata and message contents. This is useful for when your
program only uses functions and doesn't need the full text of the message to be
parsed by the program.

` + "`text`" + `

The full contents of the message are fed into the program as a string, allowing
you to reference tokenised segments of the message with variables ($0, $1, etc).
Custom functions can still be used with this codec.

This is the default codec as it behaves most similar to typical usage of the awk
command line tool.

` + "`json`" + `

No contents are fed into the program. Instead, variables are extracted from the
message by walking the flattened JSON structure. Each value is converted into a
variable by taking its full path, e.g. the object:

` + "``` json" + `
{
	"foo": {
		"bar": {
			"value": 10
		},
		"created_at": "2018-12-18T11:57:32"
	}
}
` + "```" + `

Would result in the following variable declarations:

` + "```" + `
foo_bar_value = 10
foo_created_at = "2018-12-18T11:57:32"
` + "```" + `

Custom functions can also still be used with this codec.`,
	}
}

//------------------------------------------------------------------------------

// AWKConfig contains configuration fields for the AWK processor.
type AWKConfig struct {
	Parts   []int  `json:"parts" yaml:"parts"`
	Codec   string `json:"codec" yaml:"codec"`
	Program string `json:"program" yaml:"program"`
}

// NewAWKConfig returns a AWKConfig with default values.
func NewAWKConfig() AWKConfig {
	return AWKConfig{
		Parts:   []int{},
		Codec:   "text",
		Program: "BEGIN { x = 0 } { print $0, x; x++ }",
	}
}

//------------------------------------------------------------------------------

// AWK is a processor that executes AWK programs on a message part and replaces
// the contents with the result.
type AWK struct {
	parts   []int
	program *parser.Program

	conf  AWKConfig
	log   log.Modular
	stats metrics.Type
	mut   sync.Mutex

	functions map[string]interface{}

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewAWK returns a AWK processor.
func NewAWK(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	program, err := parser.ParseProgram([]byte(conf.AWK.Program), &parser.ParserConfig{
		Funcs: awkFunctionsMap,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to compile AWK program: %v", err)
	}
	switch conf.AWK.Codec {
	case "none":
	case "text":
	case "json":
	default:
		return nil, fmt.Errorf("unrecognised codec: %v", conf.AWK.Codec)
	}
	functionOverrides := make(map[string]interface{}, len(awkFunctionsMap))
	for k, v := range awkFunctionsMap {
		functionOverrides[k] = v
	}
	functionOverrides["print_log"] = func(value, level string) {
		switch level {
		default:
			fallthrough
		case "":
			fallthrough
		case "INFO":
			log.Infoln(value)
		case "TRACE":
			log.Traceln(value)
		case "DEBUG":
			log.Debugln(value)
		case "WARN":
			log.Warnln(value)
		case "ERROR":
			log.Errorln(value)
		case "FATAL":
			log.Fatalln(value)
		}
	}
	a := &AWK{
		parts:   conf.AWK.Parts,
		program: program,
		conf:    conf.AWK,
		log:     log,
		stats:   stats,

		functions: functionOverrides,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return a, nil
}

//------------------------------------------------------------------------------

func getTime(dateStr string, format string) (time.Time, error) {
	if len(dateStr) == 0 {
		return time.Now(), nil
	}
	if len(format) == 0 {
		var err error
		var parsed time.Time
	layoutIter:
		for _, layout := range []string{
			time.RubyDate,
			time.RFC1123Z,
			time.RFC1123,
			time.RFC3339,
			time.RFC822,
			time.RFC822Z,
			"Mon, 2 Jan 2006 15:04:05 -0700",
			"2006-01-02T15:04:05MST",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z0700",
			"2006-01-02",
		} {
			if parsed, err = time.Parse(layout, dateStr); err == nil {
				break layoutIter
			}
		}
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to detect datetime format of: %v", dateStr)
		}
		return parsed, nil
	}
	return time.Parse(format, dateStr)
}

var awkFunctionsMap = map[string]interface{}{
	"timestamp_unix": func(dateStr string, format string) (int64, error) {
		ts, err := getTime(dateStr, format)
		if err != nil {
			return 0, err
		}
		return ts.Unix(), nil
	},
	"timestamp_unix_nano": func(dateStr string, format string) (int64, error) {
		ts, err := getTime(dateStr, format)
		if err != nil {
			return 0, err
		}
		return ts.UnixNano(), nil
	},
	"timestamp_format": func(unix int64, formatArg string) string {
		format := time.RFC3339
		if len(formatArg) > 0 {
			format = formatArg
		}
		t := time.Unix(unix, 0).In(time.UTC)
		return t.Format(format)
	},
	"timestamp_format_nano": func(unixNano int64, formatArg string) string {
		format := time.RFC3339
		if len(formatArg) > 0 {
			format = formatArg
		}
		s := unixNano / 1000000000
		ns := unixNano - (s * 1000000000)
		t := time.Unix(s, ns).In(time.UTC)
		return t.Format(format)
	},
	"metadata_get": func(key string) string {
		// Do nothing, this is a placeholder for compilation.
		return ""
	},
	"metadata_set": func(key, value string) {
		// Do nothing, this is a placeholder for compilation.
	},
	"json_get": func(path string) (string, error) {
		// Do nothing, this is a placeholder for compilation.
		return "", errors.New("not implemented")
	},
	"json_set": func(path, value string) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_set_int": func(path string, value int) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_set_float": func(path string, value float64) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_set_bool": func(path string, value bool) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_delete": func(path string) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"create_json_object": func(vals ...string) string {
		pairs := map[string]string{}
		for i := 0; i < len(vals)-1; i += 2 {
			pairs[vals[i]] = vals[i+1]
		}
		bytes, _ := json.Marshal(pairs)
		if len(bytes) == 0 {
			return "{}"
		}
		return string(bytes)
	},
	"create_json_array": func(vals ...string) string {
		bytes, _ := json.Marshal(vals)
		if len(bytes) == 0 {
			return "[]"
		}
		return string(bytes)
	},
	"print_log": func(value, level string) {
		// Do nothing, this is a placeholder for compilation.
	},
}

//------------------------------------------------------------------------------

func flattenForAWK(path string, data interface{}) map[string]string {
	m := map[string]string{}

	switch t := data.(type) {
	case map[string]interface{}:
		for k, v := range t {
			newPath := k
			if len(path) > 0 {
				newPath = path + "." + k
			}
			for k2, v2 := range flattenForAWK(newPath, v) {
				m[k2] = v2
			}
		}
	case []interface{}:
		for _, ele := range t {
			for k, v := range flattenForAWK(path, ele) {
				m[k] = v
			}
		}
	default:
		m[path] = fmt.Sprintf("%v", t)
	}

	return m
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (a *AWK) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	a.mCount.Incr(1)
	newMsg := msg.Copy()
	mutableJSONParts := make([]interface{}, newMsg.Len())

	a.mut.Lock()
	customFuncs := make(map[string]interface{}, len(a.functions))
	for k, v := range a.functions {
		customFuncs[k] = v
	}
	a.mut.Unlock()

	proc := func(i int, span opentracing.Span, part types.Part) error {
		var outBuf, errBuf bytes.Buffer

		// Function overrides
		customFuncs["metadata_get"] = func(k string) string {
			return part.Metadata().Get(k)
		}
		customFuncs["metadata_set"] = func(k, v string) {
			part.Metadata().Set(k, v)
		}
		customFuncs["json_get"] = func(path string) (string, error) {
			jsonPart, err := part.JSON()
			if err != nil {
				return "", fmt.Errorf("failed to parse message into json: %v", err)
			}
			gPart := gabs.Wrap(jsonPart)
			gTarget := gPart.Path(path)
			if gTarget.Data() == nil {
				return "null", nil
			}
			if str, isString := gTarget.Data().(string); isString {
				return str, nil
			}
			return gTarget.String(), nil
		}
		getJSON := func() (*gabs.Container, error) {
			var err error
			jsonPart := mutableJSONParts[i]
			if jsonPart == nil {
				if jsonPart, err = part.JSON(); err == nil {
					jsonPart, err = message.CopyJSON(jsonPart)
				}
				if err == nil {
					mutableJSONParts[i] = jsonPart
				}
			}
			if err == nil {
			}
			if err != nil {
				return nil, fmt.Errorf("failed to parse message into json: %v", err)
			}
			gPart := gabs.Wrap(jsonPart)
			return gPart, nil
		}
		setJSON := func(path string, v interface{}) (int, error) {
			gPart, err := getJSON()
			if err != nil {
				return 0, err
			}
			gPart.SetP(v, path)
			part.SetJSON(gPart.Data())
			return 0, nil
		}
		customFuncs["json_set"] = func(path, v string) (int, error) {
			return setJSON(path, v)
		}
		customFuncs["json_set_int"] = func(path string, v int) (int, error) {
			return setJSON(path, v)
		}
		customFuncs["json_set_float"] = func(path string, v float64) (int, error) {
			return setJSON(path, v)
		}
		customFuncs["json_set_bool"] = func(path string, v bool) (int, error) {
			return setJSON(path, v)
		}
		customFuncs["json_delete"] = func(path string) (int, error) {
			gObj, err := getJSON()
			if err != nil {
				return 0, err
			}
			gObj.DeleteP(path)
			part.SetJSON(gObj.Data())
			return 0, nil
		}

		config := &interp.Config{
			Output: &outBuf,
			Error:  &errBuf,
			Funcs:  customFuncs,
		}

		if a.conf.Codec == "json" {
			jsonPart, err := part.JSON()
			if err != nil {
				a.mErr.Incr(1)
				a.log.Errorf("Failed to parse part into json: %v\n", err)
				return err
			}

			for k, v := range flattenForAWK("", jsonPart) {
				config.Vars = append(config.Vars, varInvalidRegexp.ReplaceAllString(k, "_"), v)
			}
			config.Stdin = bytes.NewReader([]byte(" "))
		} else if a.conf.Codec == "text" {
			config.Stdin = bytes.NewReader(part.Get())
		} else {
			config.Stdin = bytes.NewReader([]byte(" "))
		}

		if a.conf.Codec != "none" {
			part.Metadata().Iter(func(k, v string) error {
				config.Vars = append(config.Vars, varInvalidRegexp.ReplaceAllString(k, "_"), v)
				return nil
			})
		}

		if exitStatus, err := interp.ExecProgram(a.program, config); err != nil {
			a.mErr.Incr(1)
			a.log.Errorf("Non-fatal execution error: %v\n", err)
			return err
		} else if exitStatus != 0 {
			a.mErr.Incr(1)
			err = fmt.Errorf(
				"non-fatal execution error: awk interpreter returned non-zero exit code: %d", exitStatus,
			)
			a.log.Errorf("AWK: %v\n", err)
			return err
		}

		if errMsg, err := ioutil.ReadAll(&errBuf); err != nil {
			a.log.Errorf("Read err error: %v\n", err)
		} else if len(errMsg) > 0 {
			a.mErr.Incr(1)
			a.log.Errorf("Execution error: %s\n", errMsg)
			return errors.New(string(errMsg))
		}

		resMsg, err := ioutil.ReadAll(&outBuf)
		if err != nil {
			a.mErr.Incr(1)
			a.log.Errorf("Read output error: %v\n", err)
			return err
		}

		if len(resMsg) > 0 {
			// Remove trailing line break
			if resMsg[len(resMsg)-1] == '\n' {
				resMsg = resMsg[:len(resMsg)-1]
			}
			part.Set(resMsg)
		}
		return nil
	}

	IteratePartsWithSpan(TypeAWK, a.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	a.mBatchSent.Incr(1)
	a.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (a *AWK) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (a *AWK) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
