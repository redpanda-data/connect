// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package qdrant

// This file contains methods to convert a generic map to map[string]*pb.Value.
// Qdrant uses this format for its JSON payload.
// Proto definition: https://github.com/qdrant/qdrant/blob/master/lib/api/src/grpc/proto/json_with_int.proto
// This is a custom implementatation based on "google.golang.org/protobuf/types/known/structpb".
// It extends the original implementation to support IntegerValue and DoubleValue as Qdrant requires.
//
// USAGE:
//
// jsonMap := map[string]interface{}{
// 	"some_null":    nil,
// 	"some_bool":    true,
// 	"some_int":     42,
// 	"some_float":   3.14,
// 	"some_string":  "hello",
// 	"some_bytes":   []byte("world"),
// 	"some_nested":  map[string]interface{}{"key": "value"},
// 	"some_list":    []interface{}{"foo", 32},
// }
//
// valueMap := NewValueMap(jsonMap)

import (
	"encoding/base64"
	"fmt"
	"unicode/utf8"

	pb "github.com/qdrant/go-client/qdrant"
)

// Converts a map of string to interface{} to a map of string to *grpc.Value
//
//	╔════════════════════════╤════════════════════════════════════════════╗
//	║ Go type                │ Conversion                                 ║
//	╠════════════════════════╪════════════════════════════════════════════╣
//	║ nil                    │ stored as NullValue                        ║
//	║ bool                   │ stored as BoolValue                        ║
//	║ int, int32, int64      │ stored as IntegerValue                     ║
//	║ uint, uint32, uint64   │ stored as IntegerValue                     ║
//	║ float32, float64       │ stored as DoubleValue                      ║
//	║ string                 │ stored as StringValue; must be valid UTF-8 ║
//	║ []byte                 │ stored as StringValue; base64-encoded      ║
//	║ map[string]interface{} │ stored as StructValue                      ║
//	║ []interface{}          │ stored as ListValue                        ║
//	╚════════════════════════╧════════════════════════════════════════════╝

func newValueMap(input any) (map[string]*pb.Value, error) {
	inputMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("payload is expected to be a map, got %T", input)
	}
	valueMap := make(map[string]*pb.Value, len(inputMap))
	for key, val := range inputMap {
		value, err := newValue(val)
		value.GetKind()
		if err != nil {
			return nil, fmt.Errorf("failed to convert value for key %q: %v", key, err)
		}
		valueMap[key] = value
	}
	return valueMap, nil
}

// newValue constructs a *grpc.Value from a general-purpose Go interface.
func newValue(v any) (*pb.Value, error) {
	switch v := v.(type) {
	case nil:
		return newNullValue(), nil
	case bool:
		return newBoolValue(v), nil
	case int:
		return newIntegerValue(int64(v)), nil
	case int32:
		return newIntegerValue(int64(v)), nil
	case int64:
		return newIntegerValue(v), nil
	case uint:
		return newIntegerValue(int64(v)), nil
	case uint32:
		return newIntegerValue(int64(v)), nil
	case uint64:
		return newIntegerValue(int64(v)), nil
	case float32:
		return newDoubleValue(float64(v)), nil
	case float64:
		return newDoubleValue(v), nil
	case string:
		if !utf8.ValidString(v) {
			return nil, fmt.Errorf("invalid UTF-8 in string: %q", v)
		}
		return newStringValue(v), nil
	case []byte:
		s := base64.StdEncoding.EncodeToString(v)
		return newStringValue(s), nil
	case map[string]interface{}:
		v2, err := newStruct(v)
		if err != nil {
			return nil, err
		}
		return newStructValue(v2), nil
	case []interface{}:
		v2, err := newList(v)
		if err != nil {
			return nil, err
		}
		return newListValue(v2), nil
	default:
		return nil, fmt.Errorf("invalid type: %T", v)
	}
}

// newNullValue constructs a new null Value.
func newNullValue() *pb.Value {
	return &pb.Value{Kind: &pb.Value_NullValue{NullValue: pb.NullValue_NULL_VALUE}}
}

// newBoolValue constructs a new boolean Value.
func newBoolValue(v bool) *pb.Value {
	return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: v}}
}

// newIntegerValue constructs a new integer Value.
func newIntegerValue(v int64) *pb.Value {
	return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: v}}
}

// newNumberValue constructs a new double Value.
func newDoubleValue(v float64) *pb.Value {
	return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: v}}
}

// newStringValue constructs a new string Value.
func newStringValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_StringValue{StringValue: v}}
}

// newStructValue constructs a new struct Value.
func newStructValue(v *pb.Struct) *pb.Value {
	return &pb.Value{Kind: &pb.Value_StructValue{StructValue: v}}
}

// newListValue constructs a new list Value.
func newListValue(v *pb.ListValue) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: v}}
}

// newList constructs a ListValue from a general-purpose Go slice.
// The slice elements are converted using NewValue.
func newList(v []interface{}) (*pb.ListValue, error) {
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		var err error
		x.Values[i], err = newValue(v)
		if err != nil {
			return nil, err
		}
	}
	return x, nil
}

// newStruct constructs a Struct from a general-purpose Go map.
// The map keys must be valid UTF-8.
// The map values are converted using NewValue.
func newStruct(v map[string]interface{}) (*pb.Struct, error) {
	x := &pb.Struct{Fields: make(map[string]*pb.Value, len(v))}
	for k, v := range v {
		if !utf8.ValidString(k) {
			return nil, fmt.Errorf("invalid UTF-8 in string: %q", k)
		}
		var err error
		x.Fields[k], err = newValue(v)
		if err != nil {
			return nil, err
		}
	}
	return x, nil
}
