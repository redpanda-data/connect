package javascript

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

func getVM(message *service.Message, requireRegistry *require.Registry, logger *service.Logger) *goja.Runtime {
	vm := goja.New()

	requireRegistry.Enable(vm)
	console.Enable(vm)

	// Set functions on javascript VM

	// fetch
	//
	// Usage:
	// fetch(url : string, httpHeaders : string[optional], method : string[optional], payload : string[optional])
	//
	// Parameters:
	// - url[string]: The URL to use.
	// - httpHeaders[string]: HTTP headers that you want to set on the HTTP request. Notation is as follows: "Accept: application/json" will set the "Accept" header to value "application/json". If you'd like to set multiple HTTP headers, separate them by "\n", e.g. "Accept: application/json\nX-Foo: Bar". If you don't want to set headers, set an empty string (""). Default: ""
	// - method[string]: The method to use, e. g. "GET", "POST", "PUT", ... Default: "GET"
	// - payload[string]: The payload to use, e. g. '{"foo": "bar}'. If you don't want to send a payload, use an empty string (""). Default: ""
	//
	// Full example with all optional fields:
	// fetch("https://test.api.com", "Accept: application/json", "POST", '{"foo": "bar"}')
	//
	// Return value:
	// Map (object) with fields:
	// status[int]: The status code returned by the HTTP call.
	// body[string]: The body returned by the HTTP call.
	setFunction(vm, "fetch", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
		var (
			url         string
			httpHeaders = ""
			method      = "GET"
			payload     = ""
		)
		err := parseArgs(call, &url, &httpHeaders, &method, &payload)
		if err != nil {
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
		// prepare for textproto.ReadMIMEHeaders() by making sure it ends with a blank line
		normalizedHeaders := strings.TrimSpace(httpHeaders) + "\n\n"
		tpr := textproto.NewReader(bufio.NewReader(strings.NewReader(normalizedHeaders)))
		mimeHeader, err := tpr.ReadMIMEHeader()
		if err != nil {
			return nil, err
		}
		req.Header = http.Header(mimeHeader)

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

		return map[string]interface{}{"status": resp.StatusCode, "body": string(respBody)}, nil
	})

	// getMeta
	//
	// Usage:
	// getMeta(key : string)
	//
	// Parameters:
	// - key[string]: The key to access meta with.
	//
	// Return value:
	// String value associated with the key.
	setFunction(vm, "getMeta", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
		var (
			name string
		)
		err := parseArgs(call, &name)
		if err != nil {
			return nil, err
		}
		result, ok := message.MetaGet(name)
		if !ok {
			return nil, errors.New("not found")
		}

		return result, nil
	})

	// setMeta
	//
	// Usage:
	// setMeta(key : string, value : string)
	//
	// Parameters:
	// - key[string]: The key to set in meta.
	// - value[string]: The value to set in meta.
	setFunction(vm, "setMeta", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
		var (
			name, value string
		)
		err := parseArgs(call, &name, &value)
		if err != nil {
			return "", err
		}
		message.MetaSet(name, value) // TODO: Does this mutate the metadata correctly?
		return nil, nil
	})

	// setRoot
	//
	// Usage:
	// setRoot(value : any)
	//
	// Parameters:
	// - value[any]: The value that will be used to replace root.
	setFunction(vm, "setRoot", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
		var (
			value interface{}
		)
		err := parseArgs(call, &value)
		if err != nil {
			return nil, err
		}
		message.SetStructured(value)
		return nil, nil
	})

	// getRoot
	//
	// Usage:
	// getRoot()
	//
	// Return value:
	// The root data. It is safe to mutate the contents of the returned value. To mutate the message root, use `setRoot`.
	setFunction(vm, "getRoot", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *service.Logger) (interface{}, error) {
		return message.AsStructuredMut()
	})

	return vm
}

func setFunction(vm *goja.Runtime, name string, logger *service.Logger, function func(goja.FunctionCall, *goja.Runtime, *service.Logger) (interface{}, error)) {
	vm.Set(name, func(call goja.FunctionCall, rt *goja.Runtime) goja.Value {
		l := logger.With("function", name)
		result, err := function(call, rt, l)
		if err != nil {
			// TODO: Do we really want to log all errors that a function returns? E. g. `getMeta` will return an error if the key can't be found.
			l.Error(err.Error())
			return goja.Null()
		}
		return rt.ToValue(result)
	})
}

func parseArgs(call goja.FunctionCall, ptrs ...interface{}) error {
	if len(ptrs) < len(call.Arguments) {
		return fmt.Errorf("have %d arguments, but only %d pointers to parse into", len(call.Arguments), len(ptrs))
	}

	for i := 0; i < len(call.Arguments); i++ {
		arg, ptr := call.Argument(i), ptrs[i]
		var err error

		if goja.IsUndefined(arg) {
			return fmt.Errorf("argument at position %d is undefined", i)
		}

		switch p := ptr.(type) {
		case *string:
			*p = arg.String()
		case *int:
			*p = int(arg.ToInteger())
		case *int64:
			*p = arg.ToInteger()
		case *float64:
			*p = arg.ToFloat()
		case *map[string]interface{}:
			*p, err = getMapFromValue(arg)
		case *bool:
			*p = arg.ToBoolean()
		case *[]interface{}:
			*p, err = getSliceFromValue(arg)
		case *[]map[string]interface{}:
			*p, err = getMapSliceFromValue(arg)
		case *goja.Value:
			*p = arg
		case *interface{}:
			*p = arg.Export()
		default:
			return fmt.Errorf("encountered unhandled type %T while trying to parse %v into %v", arg.ExportType().String(), arg, p)
		}

		if err != nil {
			return fmt.Errorf("could not parse %v (%s) into %v (%T): %v", arg, arg.ExportType().String(), ptr, ptr, err)
		}
	}

	return nil
}
