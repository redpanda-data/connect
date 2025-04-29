/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package runtimepb

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MessageToProto converts a service.Message into proto form.
func MessageToProto(msg *service.Message) (*Message, error) {
	out := &Message{}
	if msg.HasBytes() {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		out.Payload = &Message_Serialized{b}
	} else {
		v, err := msg.AsStructured()
		if err != nil {
			return nil, err
		}
		val, err := AnyToProto(v)
		if err != nil {
			return nil, err
		}
		out.Payload = &Message_Structured{val}
	}
	out.Metadata = &StructValue{Fields: map[string]*Value{}}
	err := msg.MetaWalkMut(func(k string, v any) error {
		val, err := AnyToProto(v)
		out.Metadata.Fields[k] = val
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata: %w", err)
	}
	return out, nil
}

// AnyToProto converts an arbitrary value into a proto Value.
func AnyToProto(a any) (*Value, error) {
	switch v := a.(type) {
	case nil:
		return &Value{Kind: &Value_NullValue{}}, nil
	case []byte:
		return &Value{Kind: &Value_BytesValue{v}}, nil
	case string:
		return &Value{Kind: &Value_StringValue{v}}, nil
	case bool:
		return &Value{Kind: &Value_BoolValue{v}}, nil
	case time.Time:
		return &Value{Kind: &Value_TimestampValue{timestamppb.New(v)}}, nil
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			return &Value{Kind: &Value_IntegerValue{i}}, nil
		}
		f, err := v.Float64()
		if err != nil {
			return nil, err
		}
		return &Value{Kind: &Value_DoubleValue{f}}, nil
	case float32, float64:
		i, err := bloblang.ValueAsFloat64(a)
		if err != nil {
			return nil, err
		}
		return &Value{Kind: &Value_DoubleValue{i}}, nil
	case int, int64, int32, int16, int8, uint, uint32, uint16, uint8, uint64:
		i, err := bloblang.ValueAsInt64(a)
		if err != nil {
			return nil, err
		}
		return &Value{Kind: &Value_IntegerValue{i}}, nil
	case []any:
		out := &ListValue{Values: make([]*Value, len(v))}
		for i, item := range v {
			v, err := AnyToProto(item)
			if err != nil {
				return nil, err
			}
			out.Values[i] = v
		}
		return &Value{Kind: &Value_ListValue{out}}, nil
	case map[string]any:
		out := &StructValue{Fields: make(map[string]*Value, len(v))}
		for k, item := range v {
			v, err := AnyToProto(item)
			if err != nil {
				return nil, err
			}
			out.Fields[k] = v
		}
		return &Value{Kind: &Value_StructValue{out}}, nil
	}
	return nil, fmt.Errorf("unsupported type: %T", a)
}

// ProtoToMessage converts a service.Message from proto form.
func ProtoToMessage(msg *Message) (*service.Message, error) {
	var out *service.Message
	switch p := msg.Payload.(type) {
	case *Message_Serialized:
		out = service.NewMessage(p.Serialized)
	case *Message_Structured:
		out = service.NewMessage(nil)
		v, err := ValueToAny(p.Structured)
		if err != nil {
			return nil, err
		}
		out.SetStructuredMut(v)
	}
	for k, v := range msg.Metadata.Fields {
		val, err := ValueToAny(v)
		if err != nil {
			return nil, err
		}
		out.MetaSetMut(k, val)
	}
	return out, nil
}

// ValueToAny converts a proto Value into an arbitrary value.
func ValueToAny(val *Value) (any, error) {
	switch v := val.Kind.(type) {
	case *Value_NullValue:
		return nil, nil
	case *Value_BytesValue:
		return v.BytesValue, nil
	case *Value_StringValue:
		return v.StringValue, nil
	case *Value_BoolValue:
		return v.BoolValue, nil
	case *Value_TimestampValue:
		return v.TimestampValue.AsTime(), nil
	case *Value_IntegerValue:
		return v.IntegerValue, nil
	case *Value_DoubleValue:
		return v.DoubleValue, nil
	case *Value_ListValue:
		out := make([]any, len(v.ListValue.Values))
		for i, item := range v.ListValue.Values {
			val, err := ValueToAny(item)
			if err != nil {
				return nil, err
			}
			out[i] = val
		}
		return out, nil
	case *Value_StructValue:
		out := make(map[string]any, len(v.StructValue.Fields))
		for k, item := range v.StructValue.Fields {
			val, err := ValueToAny(item)
			if err != nil {
				return nil, err
			}
			out[k] = val
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported type: %T", val.Kind)
}
