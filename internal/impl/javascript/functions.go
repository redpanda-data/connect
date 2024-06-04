package javascript

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dop251/goja"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type jsFunction func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error)

type jsFunctionParam struct {
	name    string
	typeStr string
	what    string
}

type jsFunctionDefinition struct {
	name        string
	description string
	params      []jsFunctionParam
	examples    []string
	ctor        func(r *vmRunner) jsFunction
}

func (j *jsFunctionDefinition) Param(name, typeStr, what string) *jsFunctionDefinition {
	j.params = append(j.params, jsFunctionParam{
		name:    name,
		typeStr: typeStr,
		what:    what,
	})
	return j
}

func (j *jsFunctionDefinition) Example(example string) *jsFunctionDefinition {
	j.examples = append(j.examples, example)
	return j
}

func (j *jsFunctionDefinition) FnCtor(ctor func(r *vmRunner) jsFunction) *jsFunctionDefinition {
	j.ctor = ctor
	return j
}

func (j *jsFunctionDefinition) String() string {
	var description strings.Builder

	_, _ = fmt.Fprintf(&description, "### `benthos.%v`\n\n", j.name)
	_, _ = description.WriteString(j.description + "\n\n")
	if len(j.params) > 0 {
		_, _ = description.WriteString("#### Parameters\n\n")
		for _, p := range j.params {
			_, _ = fmt.Fprintf(&description, "**`%v`** &lt;%v&gt; %v  \n", p.name, p.typeStr, p.what)
		}
		_, _ = description.WriteString("\n")
	}

	if len(j.examples) > 0 {
		_, _ = description.WriteString("#### Examples\n\n")
		for _, e := range j.examples {
			_, _ = description.WriteString("```javascript\n")
			_, _ = description.WriteString(strings.Trim(e, "\n"))
			_, _ = description.WriteString("\n```\n")
		}
	}

	return description.String()
}

var vmRunnerFunctionCtors = map[string]*jsFunctionDefinition{}

func registerVMRunnerFunction(name, description string) *jsFunctionDefinition {
	fn := &jsFunctionDefinition{
		name:        name,
		description: description,
	}
	vmRunnerFunctionCtors[name] = fn
	return fn
}

//------------------------------------------------------------------------------

var _ = registerVMRunnerFunction(
	"v0_fetch",
	`Executes an HTTP request synchronously and returns the result as an object of the form `+"`"+`{"status":200,"body":"foo"}`+"`"+`.`,
).
	Param("url", "string", "The URL to fetch").
	Param("headers", "object(string,string)", "An object of string/string key/value pairs to add the request as headers.").
	Param("method", "string", "The method of the request.").
	Param("body", "(optional) string", "A body to send.").
	Example(`
let result = benthos.v0_fetch("http://example.com", {}, "GET", "")
benthos.v0_msg_set_structured(result);
`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var (
				url         string
				httpHeaders map[string]any
				method      = "GET"
				payload     = ""
			)
			if err := parseArgs(call, &url, &httpHeaders, &method, &payload); err != nil {
				return nil, err
			}

			var payloadReader io.Reader
			if payload != "" {
				payloadReader = strings.NewReader(payload)
			}

			req, err := http.NewRequest(method, url, payloadReader)
			if err != nil {
				return nil, err
			}

			// Parse HTTP headers
			for k, v := range httpHeaders {
				vStr, _ := v.(string)
				req.Header.Add(k, vStr)
			}

			// Do request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			respBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"status": resp.StatusCode,
				"body":   string(respBody),
			}, nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_set_string", `Set the contents of the processed message to a given string.`).
	Param("value", "string", "The value to set it to.").
	Example(`benthos.v0_msg_set_string("hello world");`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var value string
			if err := parseArgs(call, &value); err != nil {
				return nil, err
			}

			r.targetMessage.SetBytes([]byte(value))
			return nil, nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_as_string", `Obtain the raw contents of the processed message as a string.`).
	Example(`let contents = benthos.v0_msg_as_string();`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			b, err := r.targetMessage.AsBytes()
			if err != nil {
				return nil, err
			}
			return string(b), nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_set_structured", `Set the root of the processed message to a given value of any type.`).
	Param("value", "anything", "The value to set it to.").
	Example(`
benthos.v0_msg_set_structured({
  "foo": "a thing",
  "bar": "something else",
  "baz": 1234
});
`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var value any
			if err := parseArgs(call, &value); err != nil {
				return nil, err
			}

			r.targetMessage.SetStructured(value)
			return nil, nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_as_structured", `Obtain the root of the processed message as a structured value. If the message is not valid JSON or has not already been expanded into a structured form this function will throw an error.`).
	Example(`let foo = benthos.v0_msg_as_structured().foo;`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			return r.targetMessage.AsStructured()
		}
	})

var _ = registerVMRunnerFunction("v0_msg_exists_meta", `Check that a metadata key exists.`).
	Param("name", "string", "The metadata key to search for.").
	Example(`if (benthos.v0_msg_exists_meta("kafka_key")) {}`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var name string
			if err := parseArgs(call, &name); err != nil {
				return nil, err
			}

			_, ok := r.targetMessage.MetaGet(name)
			if !ok {
				return false, nil
			}
			return true, nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_get_meta", `Get the value of a metadata key from the processed message.`).
	Param("name", "string", "The metadata key to search for.").
	Example(`let key = benthos.v0_msg_get_meta("kafka_key");`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var name string
			if err := parseArgs(call, &name); err != nil {
				return nil, err
			}

			result, ok := r.targetMessage.MetaGet(name)
			if !ok {
				return nil, errors.New("key not found")
			}
			return result, nil
		}
	})

var _ = registerVMRunnerFunction("v0_msg_set_meta", `Set a metadata key on the processed message to a value.`).
	Param("name", "string", "The metadata key to set.").
	Param("value", "anything", "The value to set it to.").
	Example(`benthos.v0_msg_set_meta("thing", "hello world");`).
	FnCtor(func(r *vmRunner) jsFunction {
		return func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
			var (
				name  string
				value any
			)
			if err := parseArgs(call, &name, &value); err != nil {
				return "", err
			}
			r.targetMessage.MetaSetMut(name, value)
			return nil, nil
		}
	})
