package processor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/benhoyt/goawk/interp"
	"github.com/benhoyt/goawk/parser"
)

//------------------------------------------------------------------------------

var varInvalidRegexp *regexp.Regexp

func init() {
	varInvalidRegexp = regexp.MustCompile(`[^a-zA-Z0-9_]`)

	Constructors[TypeAWK] = TypeSpec{
		constructor: NewAWK,
		Categories: []Category{
			CategoryMapping,
		},
		Summary: `
Executes an AWK program on messages. This processor is very powerful as it
offers a range of [custom functions](#awk-functions) for querying and mutating
message contents and metadata.`,
		Description: `
Works by feeding message contents as the program input based on a chosen
[codec](#codecs) and replaces the contents of each message with the result. If
the result is empty (nothing is printed by the program) then the original
message contents remain unchanged.

Comes with a wide range of [custom functions](#awk-functions) for accessing
message metadata, json fields, printing logs, etc. These functions can be
overridden by functions within the program.

Check out the [examples section](#examples) in order to see how this processor
can be used.

This processor uses [GoAWK][goawk], in order to understand the differences
in how the program works you can [read more about it here][goawk.differences].`,
		Footnotes: `
## Codecs

The chosen codec determines how the contents of the message are fed into the
program. Codecs only impact the input string and variables initialised for your
program, they do not change the range of custom functions available.

### ` + "`none`" + `

An empty string is fed into the program. Functions can still be used in order to
extract and mutate metadata and message contents.

This is useful for when your program only uses functions and doesn't need the
full text of the message to be parsed by the program, as it is significantly
faster.

### ` + "`text`" + `

The full contents of the message are fed into the program as a string, allowing
you to reference tokenised segments of the message with variables ($0, $1, etc).
Custom functions can still be used with this codec.

This is the default codec as it behaves most similar to typical usage of the awk
command line tool.

### ` + "`json`" + `

An empty string is fed into the program, and variables are automatically
initialised before execution of your program by walking the flattened JSON
structure. Each value is converted into a variable by taking its full path,
e.g. the object:

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

Custom functions can also still be used with this codec.

## AWK Functions

` + "### `json_get`" + `

Signature: ` + "`json_get(path)`" + `

Attempts to find a JSON value in the input message payload by a
[dot separated path](/docs/configuration/field_paths) and returns it as a string.

` + "### `json_set`" + `

Signature: ` + "`json_set(path, value)`" + `

Attempts to set a JSON value in the input message payload identified by a
[dot separated path](/docs/configuration/field_paths), the value argument will be interpreted
as a string.

In order to set non-string values use one of the following typed varieties:

` + "- `json_set_int(path, value)`" + `
` + "- `json_set_float(path, value)`" + `
` + "- `json_set_bool(path, value)`" + `

` + "### `json_append`" + `

Signature: ` + "`json_append(path, value)`" + `

Attempts to append a value to an array identified by a
[dot separated path](/docs/configuration/field_paths). If the target does not
exist it will be created. If the target exists but is not already an array then
it will be converted into one, with its original contents set to the first
element of the array.

The value argument will be interpreted as a string. In order to append
non-string values use one of the following typed varieties:

` + "- `json_append_int(path, value)`" + `
` + "- `json_append_float(path, value)`" + `
` + "- `json_append_bool(path, value)`" + `

` + "### `json_delete`" + `

Signature: ` + "`json_delete(path)`" + `

Attempts to delete a JSON field from the input message payload identified by a
[dot separated path](/docs/configuration/field_paths).

` + "### `json_length`" + `

Signature: ` + "`json_length(path)`" + `

Returns the size of the string or array value of JSON field from the input
message payload identified by a [dot separated path](/docs/configuration/field_paths).

If the target field does not exist, or is not a string or array type, then zero
is returned. In order to explicitly check the type of a field use ` + "`json_type`" + `.

` + "### `json_type`" + `

Signature: ` + "`json_type(path)`" + `

Returns the type of a JSON field from the input message payload identified by a
[dot separated path](/docs/configuration/field_paths).

Possible values are: "string", "int", "float", "bool", "undefined", "null",
"array", "object".

` + "### `create_json_object`" + `

Signature: ` + "`create_json_object(key1, val1, key2, val2, ...)`" + `

Generates a valid JSON object of key value pair arguments. The arguments are
variadic, meaning any number of pairs can be listed. The value will always
resolve to a string regardless of the value type. E.g. the following call:

` + "`create_json_object(\"a\", \"1\", \"b\", 2, \"c\", \"3\")`" + `

Would result in this string:

` + "`{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}`" + `

` + "### `create_json_array`" + `

Signature: ` + "`create_json_array(val1, val2, ...)`" + `

Generates a valid JSON array of value arguments. The arguments are variadic,
meaning any number of values can be listed. The value will always resolve to a
string regardless of the value type. E.g. the following call:

` + "`create_json_array(\"1\", 2, \"3\")`" + `

Would result in this string:

` + "`[\"1\",\"2\",\"3\"]`" + `

` + "### `metadata_set`" + `

Signature: ` + "`metadata_set(key, value)`" + `

Set a metadata key for the message to a value. The value will always resolve to
a string regardless of the value type.

` + "### `metadata_get`" + `

Signature: ` + "`metadata_get(key) string`" + `

Get the value of a metadata key from the message.

` + "### `timestamp_unix`" + `

Signature: ` + "`timestamp_unix() int`" + `

Returns the current unix timestamp (the number of seconds since 01-01-1970).

` + "### `timestamp_unix`" + `

Signature: ` + "`timestamp_unix(date) int`" + `

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp (the number of seconds since 01-01-1970).

` + "### `timestamp_unix`" + `

Signature: ` + "`timestamp_unix(date, format) int`" + `

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp (the number of seconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
` + "`Mon Jan 2 15:04:05 -0700 MST 2006`" + ` would be displayed if it were the value.

` + "### `timestamp_unix_nano`" + `

Signature: ` + "`timestamp_unix_nano() int`" + `

Returns the current unix timestamp in nanoseconds (the number of nanoseconds
since 01-01-1970).

` + "### `timestamp_unix_nano`" + `

Signature: ` + "`timestamp_unix_nano(date) int`" + `

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp in nanoseconds (the number of nanoseconds since
01-01-1970).

` + "### `timestamp_unix_nano`" + `

Signature: ` + "`timestamp_unix_nano(date, format) int`" + `

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp in nanoseconds (the number of nanoseconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
` + "`Mon Jan 2 15:04:05 -0700 MST 2006`" + ` would be displayed if it were the value.

` + "### `timestamp_format`" + `

Signature: ` + "`timestamp_format(unix, format) string`" + `

Formats a unix timestamp. The format is defined by showing how the reference
time, defined to be ` + "`Mon Jan 2 15:04:05 -0700 MST 2006`" + ` would be displayed if it
were the value.

The format is optional, and if omitted RFC3339 (` + "`2006-01-02T15:04:05Z07:00`" + `)
will be used.

` + "### `timestamp_format_nano`" + `

Signature: ` + "`timestamp_format_nano(unixNano, format) string`" + `

Formats a unix timestamp in nanoseconds. The format is defined by showing how
the reference time, defined to be ` + "`Mon Jan 2 15:04:05 -0700 MST 2006`" + ` would be
displayed if it were the value.

The format is optional, and if omitted RFC3339 (` + "`2006-01-02T15:04:05Z07:00`" + `)
will be used.

` + "### `print_log`" + `

Signature: ` + "`print_log(message, level)`" + `

Prints a Benthos log message at a particular log level. The log level is
optional, and if omitted the level ` + "`INFO`" + ` will be used.

[goawk]: https://github.com/benhoyt/goawk
[goawk.differences]: https://github.com/benhoyt/goawk#differences-from-awk`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("codec", "A [codec](#codecs) defines how messages should be inserted into the AWK program as variables. The codec does not change which [custom Benthos functions](#awk-functions) are available. The `text` codec is the closest to a typical AWK use case.").HasOptions("none", "text", "json"),
			docs.FieldCommon("program", "An AWK program to execute"),
			PartsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "JSON Mapping and Arithmetic",
				Summary: `
Because AWK is a full programming language it's much easier to map documents and
perform arithmetic with it than with other Benthos processors. For example, if
we were expecting documents of the form:

` + "```json" + `
{"doc":{"val1":5,"val2":10},"id":"1","type":"add"}
{"doc":{"val1":5,"val2":10},"id":"2","type":"multiply"}
` + "```" + `

And we wished to perform the arithmetic specified in the ` + "`type`" + ` field,
on the values ` + "`val1` and `val2`" + ` and, finally, map the result into the
document, giving us the following resulting documents:

` + "```json" + `
{"doc":{"result":15,"val1":5,"val2":10},"id":"1","type":"add"}
{"doc":{"result":50,"val1":5,"val2":10},"id":"2","type":"multiply"}
` + "```" + `

We can do that with the following:`,
				Config: `
pipeline:
  processors:
  - awk:
      program: |
        function map_add_vals() {
          json_set_int("doc.result", json_get("doc.val1") + json_get("doc.val2"));
        }
        function map_multiply_vals() {
          json_set_int("doc.result", json_get("doc.val1") * json_get("doc.val2"));
        }
        function map_unknown(type) {
          json_set("error","unknown document type");
          print_log("Document type not recognised: " type, "ERROR");
        }
        {
          type = json_get("type");
          if (type == "add")
            map_add_vals();
          else if (type == "multiply")
            map_multiply_vals();
          else
            map_unknown(type);
        }
`,
			},
			{
				Title: "Stuff With Arrays",
				Summary: `
It's possible to iterate JSON arrays by appending an index value to the path,
this can be used to do things like removing duplicates from arrays. For example,
given the following input document:

` + "```json" + `
{"path":{"to":{"foos":["one","two","three","two","four"]}}}
` + "```" + `

We could create a new array ` + "`foos_unique` from `foos`" + ` giving us the result:

` + "```json" + `
{"path":{"to":{"foos":["one","two","three","two","four"],"foos_unique":["one","two","three","four"]}}}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
  - awk:
      program: |
        {
          array_path = "path.to.foos"
          array_len = json_length(array_path)

          for (i = 0; i < array_len; i++) {
            ele = json_get(array_path "." i)
            if ( ! ( ele in seen ) ) {
              json_append(array_path "_unique", ele)
              seen[ele] = 1
            }
          }
        }
`,
			},
		},
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
		case "", "INFO":
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

func getTime(dateStr, format string) (time.Time, error) {
	if dateStr == "" {
		return time.Now(), nil
	}
	if format == "" {
		var err error
		var parsed time.Time
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
				break
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
	"json_append": func(path, value string) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_append_int": func(path string, value int) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_append_float": func(path string, value float64) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_append_bool": func(path string, value bool) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_delete": func(path string) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_length": func(path string) (int, error) {
		// Do nothing, this is a placeholder for compilation.
		return 0, errors.New("not implemented")
	},
	"json_type": func(path string) (string, error) {
		// Do nothing, this is a placeholder for compilation.
		return "", errors.New("not implemented")
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
func (a *AWK) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	a.mCount.Incr(1)
	newMsg := msg.Copy()
	mutableJSONParts := make([]interface{}, newMsg.Len())

	a.mut.Lock()
	customFuncs := make(map[string]interface{}, len(a.functions))
	for k, v := range a.functions {
		customFuncs[k] = v
	}
	a.mut.Unlock()

	proc := func(i int, span *tracing.Span, part *message.Part) error {
		var outBuf, errBuf bytes.Buffer

		// Function overrides
		customFuncs["metadata_get"] = func(k string) string {
			return part.MetaGet(k)
		}
		customFuncs["metadata_set"] = func(k, v string) {
			part.MetaSet(k, v)
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
		arrayAppendJSON := func(path string, v interface{}) (int, error) {
			gPart, err := getJSON()
			if err != nil {
				return 0, err
			}
			gPart.ArrayAppendP(v, path)
			part.SetJSON(gPart.Data())
			return 0, nil
		}
		customFuncs["json_append"] = func(path, v string) (int, error) {
			return arrayAppendJSON(path, v)
		}
		customFuncs["json_append_int"] = func(path string, v int) (int, error) {
			return arrayAppendJSON(path, v)
		}
		customFuncs["json_append_float"] = func(path string, v float64) (int, error) {
			return arrayAppendJSON(path, v)
		}
		customFuncs["json_append_bool"] = func(path string, v bool) (int, error) {
			return arrayAppendJSON(path, v)
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
		customFuncs["json_length"] = func(path string) (int, error) {
			gObj, err := getJSON()
			if err != nil {
				return 0, err
			}
			switch t := gObj.Path(path).Data().(type) {
			case string:
				return len(t), nil
			case []interface{}:
				return len(t), nil
			}
			return 0, nil
		}
		customFuncs["json_type"] = func(path string) (string, error) {
			gObj, err := getJSON()
			if err != nil {
				return "", err
			}
			if !gObj.ExistsP(path) {
				return "undefined", nil
			}
			switch t := gObj.Path(path).Data().(type) {
			case int:
				return "int", nil
			case float64:
				return "float", nil
			case json.Number:
				return "float", nil
			case string:
				return "string", nil
			case bool:
				return "bool", nil
			case []interface{}:
				return "array", nil
			case map[string]interface{}:
				return "object", nil
			case nil:
				return "null", nil
			default:
				return "", fmt.Errorf("type not recognised: %T", t)
			}
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
			_ = part.MetaIter(func(k, v string) error {
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

		if errMsg, err := io.ReadAll(&errBuf); err != nil {
			a.log.Errorf("Read err error: %v\n", err)
		} else if len(errMsg) > 0 {
			a.mErr.Incr(1)
			a.log.Errorf("Execution error: %s\n", errMsg)
			return errors.New(string(errMsg))
		}

		resMsg, err := io.ReadAll(&outBuf)
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

	IteratePartsWithSpanV2(TypeAWK, a.parts, newMsg, proc)

	msgs := [1]*message.Batch{newMsg}

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
