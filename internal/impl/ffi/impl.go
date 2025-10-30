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
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// processorImpl takes a set of executors (one for each parameter)
// and the message being operated on, and returns the output values
// or an error.
type processorImpl func(args []any) ([]any, error)

type returnType string

const (
	returnTypeVoid  returnType = "void"
	returnTypeInt32 returnType = "int32"
	returnTypeInt64 returnType = "int64"
)

type paramType string

const (
	paramTypeBytePtr paramType = "byte*"
	paramTypeInt32   paramType = "int32"
	paramTypeInt64   paramType = "int64"
)

type parameterSpec struct {
	Type paramType
	Out  bool
}

// signature is a string that represents a specific ABI that is supported.
type signature struct {
	Return returnType
	Params []parameterSpec
}

// signatureImpl is an implementation of given FFI signature
// from Bloblang/Connect to the foreign function and back.
type signatureImpl struct {
	signature signature
	// Given a symbol for a function that implements the signature,
	// return a processor that uses the function
	impl func(addr uintptr) processorImpl
}

// The reflection based fallback approach is very slow.
// For certain signatures we know will be called in high performance scenarios,
// inline them here so the compiler can optimize.
//
// Feel free to add more specializations here.
var optimizedSignatures = []signatureImpl{
	{
		signature: signature{
			Return: returnTypeVoid,
			Params: []parameterSpec{{Type: paramTypeInt64}},
		},
		impl: func(addr uintptr) processorImpl {
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
		signature: signature{
			Return: returnTypeInt64,
			Params: []parameterSpec{},
		},
		impl: func(addr uintptr) processorImpl {
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
		signature: signature{
			Return: returnTypeInt32,
			Params: []parameterSpec{{Type: paramTypeInt64}},
		},
		impl: func(addr uintptr) processorImpl {
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
		signature: signature{
			Return: returnTypeInt32,
			Params: []parameterSpec{
				{Type: paramTypeBytePtr},
				{Type: paramTypeBytePtr, Out: true},
				{Type: paramTypeInt32},
			},
		},
		impl: func(addr uintptr) processorImpl {
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

func makeProcessorImpl(sig signature, addr uintptr) (processorImpl, error) {
	for _, supported := range optimizedSignatures {
		if reflect.DeepEqual(supported.signature, sig) {
			return supported.impl(addr), nil
		}
	}
	// The fallback processor is slower, but works with all our supported types
	return makeFallbackProcessorImpl(sig, addr)
}

func makeFallbackProcessorImpl(sig signature, addr uintptr) (processorImpl, error) {
	returnTypes := []reflect.Type{}
	switch sig.Return {
	case returnTypeVoid:
		// No return types in golang
	case returnTypeInt32:
		returnTypes = append(returnTypes, reflect.TypeFor[int32]())
	case returnTypeInt64:
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
		case paramTypeInt32:
			paramTypes = append(paramTypes, reflect.TypeFor[int32]())
			paramConverter = append(paramConverter, func(a any) (any, error) {
				v, err := bloblang.ValueAsInt64(a)
				return int32(v), err
			})
		case paramTypeInt64:
			paramTypes = append(paramTypes, reflect.TypeFor[int64]())
			paramConverter = append(paramConverter, func(a any) (any, error) {
				return bloblang.ValueAsInt64(a)
			})
		case paramTypeBytePtr:
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
