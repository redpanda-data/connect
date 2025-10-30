/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ToMessageFn is an abstraction between ToMessageFast and ToMessageSlow
type ToMessageFn = func(protoreflect.Message, protojson.MarshalOptions, *service.Message) error

// ToMessageFast converts a protobuf message into a benthos message using protobuf JSON encoding rules.
//
// This encoder converts the protobuf message into a Golang `any` type compatible with Redpanda Connect and
// "encoding/json", then calls sMsg.SetStructuredMut, which means further changes to the message do not require
// JSON deserialization.
//
// The only places this diverges from `ToMessageSlow` in bloblang is:
// - google.protobuf.Timestamp and bytes types are preserved instead of converting into string
// - NaN, Infinity and -Infinity are preserved as float instead of string
// - 64 bit integers (signed and unsigned) are preserved as raw numbers instead of strings
// - unknown enum values are emitted as default string values instead of numbers
func ToMessageFast(pbMsg protoreflect.Message, opts protojson.MarshalOptions, sMsg *service.Message) error {
	m := &marshaller{opts}
	v, err := m.messageToStructured(pbMsg)
	if err != nil {
		return err
	}
	sMsg.SetStructuredMut(v)
	return nil
}

// ToMessageSlow converts a protobuf message into a benthos message using protobuf JSON encoding rules.
//
// It literally converts the message to JSON then calls sMsg.SetBytes.
func ToMessageSlow(pbMsg protoreflect.Message, opts protojson.MarshalOptions, sMsg *service.Message) error {
	b, err := opts.Marshal(pbMsg.Interface())
	if err != nil {
		return err
	}
	sMsg.SetBytes(b)
	return nil
}

type marshaller struct {
	opts protojson.MarshalOptions
}

func (m *marshaller) valueToStructured(f protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	if f.IsList() {
		return m.listToStructured(f, v.List())
	} else if f.IsMap() {
		return m.mapToStructured(f, v.Map())
	} else {
		return m.singularValueToStructured(f, v)
	}
}

func (m *marshaller) listToStructured(f protoreflect.FieldDescriptor, v protoreflect.List) (any, error) {
	var out []any
	for i := range v.Len() {
		e, err := m.singularValueToStructured(f, v.Get(i))
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (m *marshaller) mapToStructured(f protoreflect.FieldDescriptor, v protoreflect.Map) (any, error) {
	out := make(map[string]any)
	for k, v := range v.Range {
		v, err := m.singularValueToStructured(f.MapValue(), v)
		if err != nil {
			return nil, err
		}
		out[k.String()] = v
	}
	return out, nil
}

func (m *marshaller) singularValueToStructured(f protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	switch f.Kind() {
	case protoreflect.BoolKind:
		return v.Bool(), nil
	case protoreflect.BytesKind:
		return v.Bytes(), nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v.Float(), nil
	case protoreflect.EnumKind:
		if f.Enum().FullName() == "google.protobuf.NullValue" {
			return nil, nil
		}
		if m.opts.UseEnumNumbers {
			return int32(v.Enum()), nil
		} else {
			enumVal := f.Enum().Values().ByNumber(v.Enum())
			if enumVal == nil {
				enumVal = f.DefaultEnumValue()
			}
			if enumVal == nil {
				// Fallback to the first enum value if default is not available
				enumVal = f.Enum().Values().Get(0)
			}
			return string(enumVal.Name()), nil
		}
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return v.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return v.Uint(), nil
	case protoreflect.GroupKind, protoreflect.MessageKind:
		return m.messageToStructured(v.Message())
	case protoreflect.StringKind:
		return v.String(), nil
	default:
		return nil, fmt.Errorf("unknown field kind: %v", f.Kind())
	}
}

func (m *marshaller) messageToStructured(msg protoreflect.Message) (any, error) {
	if v, err := m.wellKnownType(msg); !errors.Is(err, errNotWellKnown) {
		return v, err
	}
	structured := map[string]any{}
	emit := func(field protoreflect.FieldDescriptor, value protoreflect.Value) error {
		v, err := m.valueToStructured(field, value)
		if err != nil {
			return err
		}
		if m.opts.UseProtoNames {
			structured[field.TextName()] = v
		} else {
			structured[field.JSONName()] = v
		}
		return nil
	}
	for field, value := range msg.Range {
		if err := emit(field, value); err != nil {
			return nil, err
		}
	}
	if m.opts.EmitUnpopulated || m.opts.EmitDefaultValues {
		fds := msg.Descriptor().Fields()
		for i := range fds.Len() {
			fd := fds.Get(i)
			if msg.Has(fd) || fd.ContainingOneof() != nil {
				continue // ignore populated and oneofs
			}
			v := msg.Get(fd)
			if fd.HasPresence() {
				if !m.opts.EmitUnpopulated {
					continue
				}
				v = protoreflect.Value{}
			}
			if err := emit(fd, v); err != nil {
				return nil, err
			}
		}
	}
	return structured, nil
}

var errNotWellKnown = errors.New("not well known type")

func (m *marshaller) wellKnownType(msg protoreflect.Message) (any, error) {
	desc := msg.Descriptor()
	if desc.FullName().Parent() != "google.protobuf" {
		return nil, errNotWellKnown
	}
	switch desc.Name() {
	case "Timestamp":
		secsVal := msg.Get(desc.Fields().ByNumber(1))
		nanosVal := msg.Get(desc.Fields().ByNumber(2))
		return time.Unix(secsVal.Int(), nanosVal.Int()).UTC(), nil
	case "Duration",
		"BoolValue",
		"Int32Value",
		"Int64Value",
		"UInt32Value",
		"UInt64Value",
		"FloatValue",
		"DoubleValue",
		"StringValue",
		"BytesValue",
		"List",
		"Struct",
		"Value",
		"FieldMask",
		"Empty",
		"Any":
		// Reuse the existing JSON serialization mechanism for these less
		// common well known types
		b, err := m.opts.Marshal(msg.Interface())
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var v any
		err = dec.Decode(&v)
		return v, err
	default:
		return nil, errNotWellKnown
	}
}
