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

package impl

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// ForeignFunc invokes a C ABI function and returns the result.
type ForeignFunc func(args []any) ([]any, error)

// ReturnType the result of an FFI function
type ReturnType string

const (
	// ReturnTypeVoid is a void return type in C
	ReturnTypeVoid ReturnType = "void"
	// ReturnTypeInt32 is a int32_t in C
	ReturnTypeInt32 ReturnType = "int32"
	// ReturnTypeInt64 is a int64_t in C
	ReturnTypeInt64 ReturnType = "int64"
)

// ParamType is the type of a FFI function parameter
type ParamType string

const (
	// ParamTypeBytePtr is a void* type in C
	ParamTypeBytePtr ParamType = "byte*"
	// ParamTypeInt32 is a int32_t in C
	ParamTypeInt32 ParamType = "int32"
	// ParamTypeInt64 is a int64_t in C
	ParamTypeInt64 ParamType = "int64"
)

// ParameterSpec is a specification for a parameter of an FFI function.
type ParameterSpec struct {
	Type ParamType
	Out  bool
}

// Signature is a string that represents a specific ABI that is supported.
type Signature struct {
	Return ReturnType
	Params []ParameterSpec
}

// specialization is an implementation of given FFI signature
// from Bloblang/Connect to the foreign function and back.
type specialization struct {
	signature Signature
	// Given a symbol for a function that implements the signature,
	// return a processor that uses the function
	impl func(addr uintptr) ForeignFunc
}

// The reflection based fallback approach is very slow.
// For certain signatures we know will be called in high performance scenarios,
// inline them here so the compiler can optimize.
//
// Feel free to add more specializations here.
var optimizedSignatures = []specialization{
	{
		signature: Signature{
			Return: ReturnTypeVoid,
			Params: []ParameterSpec{{Type: ParamTypeInt64}},
		},
		impl: func(addr uintptr) ForeignFunc {
			var fn func(int64)
			registerFunc(&fn, addr)
			return func(args []any) ([]any, error) {
				if len(args) != 1 {
					return nil, fmt.Errorf("expected 1 arg, got %d", len(args))
				}
				v, err := bloblang.ValueAsInt64(args[0])
				if err != nil {
					return nil, err
				}
				fn(v)
				return []any{}, nil
			}
		},
	},
	{
		signature: Signature{
			Return: ReturnTypeInt64,
			Params: []ParameterSpec{},
		},
		impl: func(addr uintptr) ForeignFunc {
			var fn func() int64
			registerFunc(&fn, addr)
			return func(args []any) ([]any, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("expected 0 args, got %d", len(args))
				}
				return []any{fn()}, nil
			}
		},
	},
	{
		signature: Signature{
			Return: ReturnTypeInt32,
			Params: []ParameterSpec{{Type: ParamTypeInt64}},
		},
		impl: func(addr uintptr) ForeignFunc {
			var fn func(int64) int32
			registerFunc(&fn, addr)
			return func(args []any) ([]any, error) {
				if len(args) != 1 {
					return nil, fmt.Errorf("expected 1 args, got %d", len(args))
				}
				v, err := bloblang.ValueAsInt64(args[0])
				if err != nil {
					return nil, err
				}
				result := fn(v)
				return []any{result}, nil
			}
		},
	},
	{
		signature: Signature{
			Return: ReturnTypeInt32,
			Params: []ParameterSpec{
				{Type: ParamTypeBytePtr},
				{Type: ParamTypeBytePtr, Out: true},
				{Type: ParamTypeInt32},
			},
		},
		impl: func(addr uintptr) ForeignFunc {
			var fn func(unsafe.Pointer, unsafe.Pointer, int32) int32
			registerFunc(&fn, addr)
			return func(args []any) ([]any, error) {
				if len(args) != 3 {
					return nil, fmt.Errorf("expected 3 args, got %d", len(args))
				}
				inBytes, err := bloblang.ValueAsBytes(args[0])
				if err != nil {
					return nil, err
				}
				outBytes, err := bloblang.ValueAsBytes(args[1])
				if err != nil {
					return nil, err
				}
				v, err := bloblang.ValueAsInt64(args[2])
				if err != nil {
					return nil, err
				}
				inPtr := unsafe.SliceData(inBytes)
				outPtr := unsafe.SliceData(outBytes)
				ret := fn(unsafe.Pointer(inPtr), unsafe.Pointer(outPtr), int32(v))
				return []any{ret, outBytes}, nil
			}
		},
	},
}

// MakeForeignFunc creates a foreign function based on that signature for
// a symbol at `addr`.
func MakeForeignFunc(sig Signature, addr uintptr) (ForeignFunc, error) {
	for _, supported := range optimizedSignatures {
		if reflect.DeepEqual(supported.signature, sig) {
			return supported.impl(addr), nil
		}
	}
	// The fallback processor is slower, but works with all our supported types
	return makeFallbackProcessorImpl(sig, addr)
}

func makeFallbackProcessorImpl(sig Signature, addr uintptr) (ForeignFunc, error) {
	returnTypes := []reflect.Type{}
	switch sig.Return {
	case ReturnTypeVoid:
		// No return types in golang
	case ReturnTypeInt32:
		returnTypes = append(returnTypes, reflect.TypeFor[int32]())
	case ReturnTypeInt64:
		returnTypes = append(returnTypes, reflect.TypeFor[int64]())
	default:
		return nil, fmt.Errorf("unexpected return type: %q", sig.Return)
	}
	var paramTypes []reflect.Type
	var paramConverter []func(any) (any, error)
	outParameters := map[int]bool{}
	for i, param := range sig.Params {
		if param.Out {
			outParameters[i] = true
		}
		switch param.Type {
		case ParamTypeInt32:
			paramTypes = append(paramTypes, reflect.TypeFor[int32]())
			paramConverter = append(paramConverter, func(a any) (any, error) {
				v, err := bloblang.ValueAsInt64(a)
				return int32(v), err
			})
		case ParamTypeInt64:
			paramTypes = append(paramTypes, reflect.TypeFor[int64]())
			paramConverter = append(paramConverter, func(a any) (any, error) {
				return bloblang.ValueAsInt64(a)
			})
		case ParamTypeBytePtr:
			paramTypes = append(paramTypes, reflect.TypeFor[unsafe.Pointer]())
			paramConverter = append(paramConverter, func(a any) (any, error) {
				return bloblang.ValueAsBytes(a)
			})
		default:
			return nil, fmt.Errorf("unexpected parameter type: %q", param.Type)
		}
	}
	funcType := reflect.FuncOf(paramTypes, returnTypes, false)
	// We have to pass in a pointer to a function in `registerFunc`
	fnPtr := reflect.New(funcType)
	registerFunc(fnPtr.Interface(), addr)
	return func(args []any) ([]any, error) {
		if len(args) != len(paramConverter) {
			return nil, fmt.Errorf("expected %d args, got %d", len(paramConverter), len(args))
		}
		values := make([]reflect.Value, len(args))
		outs := make([]any, len(returnTypes), len(returnTypes)+len(outParameters))
		// Make sure we pin the pointers while invoking the C function
		// so the golang memory collector doesn't move anything on us.
		var pinner runtime.Pinner
		defer pinner.Unpin()
		for i, arg := range args {
			v, err := paramConverter[i](arg)
			if err != nil {
				return nil, err
			}
			switch t := v.(type) {
			case []byte:
				ptr := unsafe.Pointer(unsafe.SliceData(t))
				pinner.Pin(ptr)
				values[i] = reflect.ValueOf(ptr)
			default:
				values[i] = reflect.ValueOf(v)
			}
			if outParameters[i] {
				outs = append(outs, v)
			}
		}
		results := fnPtr.Elem().Call(values)
		for i, result := range results {
			outs[i] = result.Interface()
		}
		return outs, nil
	}, nil
}
