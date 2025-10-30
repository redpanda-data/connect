// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ffi

import (
	"context"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/ffi/impl"
)

func init() {
	service.MustRegisterBatchProcessor(
		"ffi",
		ffiProcessorConfig(),
		makeProcessor,
	)
}

var (
	returnTypes = map[string]string{
		string(impl.ReturnTypeVoid):  "The function returns nothing",
		string(impl.ReturnTypeInt32): "A 32 bit signed integer is returned",
		string(impl.ReturnTypeInt64): "A 64 bit signed integer is returned",
	}
	paramTypes = map[string]string{
		string(impl.ParamTypeInt32):   "A 32 bit signed integer is provided as an argument",
		string(impl.ParamTypeInt64):   "A 64 bit signed integer is provided as an argument",
		string(impl.ParamTypeBytePtr): "A pointer to a byte array is provided as an argument. Note this byte array cannot be referenced once the function returns. `args_mapping` must return a byte array or string type for this argument, and the parameter in C for this should be `void*`.",
	}
)

func ffiProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Invoke a function within a shared library as a processing step.").
		Description("A processor that allows for dlopen'ing (or platform equivalent) and invoking functions dynamically at runtime. "+
			"The result from this processor is an array, where the first element is the return type if not void, and then each `out` parameter in parameter order.").
		Fields(
			service.NewStringField("library_path").
				Description("The path to the shared library (.so, .dylib or .dll) file to load dynamically.").
				Example("libbar.6.so").
				Example("libfoo.dylib"),
			service.NewStringField("function_name").
				Description("The name of the function to load from the shared library.").
				Example("MyExternCFunction"),
			service.NewBloblangField("args_mapping").
				Description("The bloblang expression that returns an array of arguments to pass into the foreign function.").
				Example("root = [42, now().ts_unix_nano(), content()]"),
			service.NewObjectField("signature",
				service.NewObjectField("return",
					service.NewStringAnnotatedEnumField("type", returnTypes).
						Description("The data type of function's return value"),
				).Description("The configuration for the function's result."),
				service.NewObjectListField(
					"parameters",
					service.NewStringAnnotatedEnumField("type", paramTypes).
						Description("The data type of the parameter."),
					service.NewBoolField("out").Default(false).
						Description("If the parameter provided is an 'out' parameter, meaning if the function mutates the value, and the resulting value should be returned. This is only valid for pointer types."),
				).Description("The parameters of the function."),
			).Description("The signature of the function."),
		).Example(
		"Call a libc function",
		"This is an example of loading libc.so and calling a function on linux.",
		`
pipeline:
  processors:
    - ffi:
        library_path: libc.6.so
        function_name: memcmp
        args_mapping: 'root = ["foo", "bar", 3]'
        signature:
          return:
            type: int32
          parameters:
            - type: byte*
            - type: byte*
            - type: int64
`)
}

func makeProcessor(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
	libPath, err := conf.FieldString("library_path")
	if err != nil {
		return nil, err
	}
	funcName, err := conf.FieldString("function_name")
	if err != nil {
		return nil, err
	}
	argsMapping, err := conf.FieldBloblang("args_mapping")
	if err != nil {
		return nil, err
	}
	retType, err := conf.FieldString("signature", "return", "type")
	if err != nil {
		return nil, err
	}
	if _, ok := returnTypes[retType]; !ok {
		return nil, fmt.Errorf("invalid return type %q", retType)
	}
	var sig impl.Signature
	sig.Return = impl.ReturnType(retType)
	parameters, err := conf.FieldObjectList("signature", "parameters")
	if err != nil {
		return nil, err
	}
	for _, paramConf := range parameters {
		pt, err := paramConf.FieldString("type")
		if err != nil {
			return nil, err
		}
		if _, ok := paramTypes[pt]; !ok {
			return nil, fmt.Errorf("invalid parameter type %q", pt)
		}
		out, err := paramConf.FieldBool("out")
		if err != nil {
			return nil, err
		}
		if out {
			// Require pointers only for out parameters
			if !strings.HasSuffix(pt, "*") {
				return nil, fmt.Errorf("unsupported out parameter type, only pointers may be out parameters: %q", pt)
			}
		}
		sig.Params = append(sig.Params, impl.ParameterSpec{
			Type: impl.ParamType(pt),
			Out:  out,
		})
	}

	so, err := impl.OpenSharedLibrary(libPath)
	if err != nil {
		return nil, err
	}
	handle, err := so.LookupSymbol(funcName)
	if err != nil {
		_ = so.Close()
		return nil, fmt.Errorf("unable to find symbol %q: %w", funcName, err)
	}
	impl, err := impl.MakeForeignFunc(sig, handle)
	if err != nil {
		_ = so.Close()
		return nil, err
	}
	return &ffiProcessor{so, impl, argsMapping}, nil
}

type ffiProcessor struct {
	so       *impl.SharedLibrary
	function impl.ForeignFunc
	args     *bloblang.Executor
}

var _ service.BatchProcessor = (*ffiProcessor)(nil)

// ProcessBatch implements service.BatchProcessor.
func (f *ffiProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	executor := batch.BloblangExecutor(f.args)
	out := make(service.MessageBatch, len(batch))
	for i, msg := range batch {
		queried, err := executor.Query(i)
		if err != nil {
			return nil, fmt.Errorf("failed to execute `args_mapping` bloblang: %w", err)
		}
		structured, err := queried.AsStructuredMut()
		if err != nil {
			return nil, fmt.Errorf("failed to extract structured result from `args_mapping` bloblang: %w", err)
		}
		args, ok := structured.([]any)
		if !ok {
			return nil, fmt.Errorf("failed to extract structured result from `args_mapping` bloblang: expected type []any, got %T", structured)
		}
		outs, err := f.function(args)
		if err != nil {
			msg.SetError(err)
		} else {
			msg.SetStructuredMut(outs)
		}
		out[i] = msg
	}
	return []service.MessageBatch{out}, nil
}

// Close implements service.Processor.
func (f *ffiProcessor) Close(context.Context) error {
	return f.so.Close()
}
