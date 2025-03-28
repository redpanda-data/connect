/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package starlark

import (
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/redpanda-data/benthos/v4/public/service"
	starlarkjson "go.starlark.net/lib/json"
	"go.starlark.net/starlark"
)

const (
	kindScalar  = "scalar"
	kindArray   = "array"
	kind2DArray = "2darray"
	kindMap     = "map"
)

type fieldSpec struct {
	Name     string      `json:"name"`
	Kind     string      `json:"kind"`
	Type     string      `json:"type"`
	Children []fieldSpec `json:"children"`
}

func extractFieldSpec(conf *service.ConfigView) (*fieldSpec, error) {
	b, err := conf.FormatJSON()
	if err != nil {
		return nil, err
	}
	var spec struct {
		Config *fieldSpec `json:"config"`
	}
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	if spec.Config == nil {
		return nil, fmt.Errorf("config field not found: %v", b)
	}
	return spec.Config, nil
}

// try and while are both python keywords, so we replace them with other names :)
var identifierReplacements = map[string]string{
	"try":   "attempt",
	"while": "loop",
}

type componentCallback func(name string, json []byte) error

func toBuiltinMethod(name string, spec *fieldSpec) (*starlark.Builtin, error) {
	switch spec.Kind {
	case kindScalar:
		if spec.Type == "object" {
			return toKeywordBuiltinMethod(name, spec)
		}
		return toArgBuiltinMethod(name, spec)
	case kindArray, kind2DArray:
		return toArgsBuiltinMethod(name, spec)
	case kindMap:
		return toKeywordBuiltinMethod(name, spec)
	default:
		return nil, fmt.Errorf("unsupported field kind: %v", spec.Kind)
	}
}

func toKeywordBuiltinMethod(name string, spec *fieldSpec) (*starlark.Builtin, error) {
	fn := func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("unexpected positional arguments for %s", name)
		}
		dict := starlark.NewDict(len(kwargs))
		for _, kwarg := range kwargs {
			key, value := kwarg.Index(0).(starlark.String), kwarg.Index(1)
			if err := dict.SetKey(key, value); err != nil {
				return nil, fmt.Errorf("unable to serialize configuration in component %s for key %v: %w", name, key, err)
			}
		}
		b, err := serializeStarlarkToJSON(thread, dict)
		if err != nil {
			return nil, fmt.Errorf("unable to serialize configuration for %s: %w", name, err)
		}
		return &starlarkComponent{name, b}, nil
	}
	return starlark.NewBuiltin(name, fn), nil
}

func toArgsBuiltinMethod(name string, spec *fieldSpec) (*starlark.Builtin, error) {
	fn := func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(kwargs) != 0 {
			return nil, fmt.Errorf("unexpected keyword arguments for %s", name, spec)
		}
		b, err := serializeStarlarkToJSON(thread, args)
		if err != nil {
			return nil, fmt.Errorf("unable to serialize configuration for %s: %v", name, err)
		}
		return &starlarkComponent{name, b}, nil
	}
	return starlark.NewBuiltin(name, fn), nil
}

func toArgBuiltinMethod(name string, spec *fieldSpec) (*starlark.Builtin, error) {
	fn := func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(kwargs) != 0 {
			return nil, fmt.Errorf("unexpected keyword arguments for %s: %+v", name, spec)
		}
		if args.Len() != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d for %s", args.Len(), name)
		}
		b, err := serializeStarlarkToJSON(thread, args.Index(0))
		if err != nil {
			return nil, fmt.Errorf("unable to serialize configuration for %s: %v", name, err)
		}
		return &starlarkComponent{name, b}, nil
	}
	return starlark.NewBuiltin(name, fn), nil
}

type starlarkComponent struct {
	Name             string
	SerializedConfig json.RawMessage
}

var _ starlark.Value = (*starlarkComponent)(nil)
var _ json.Marshaler = (*starlarkComponent)(nil)

// MarshalJSON implements json.Marshaler.
func (s *starlarkComponent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{s.Name: s.SerializedConfig})
}

// Freeze implements starlark.Value.
func (s *starlarkComponent) Freeze() {
	// Noop, we're immutable.
}

// Hash implements starlark.Value.
func (s *starlarkComponent) Hash() (uint32, error) {
	hash := fnv.New32()
	_, _ = hash.Write([]byte(s.Name))
	_, _ = hash.Write(s.SerializedConfig)
	return hash.Sum32(), nil
}

// String implements starlark.Value.
func (s *starlarkComponent) String() string {
	return fmt.Sprintf("StarlarkComponent(name=%q, config=%q)", s.Name, s.SerializedConfig)
}

// Truth implements starlark.Value.
func (s *starlarkComponent) Truth() starlark.Bool {
	return starlark.True
}

// Type implements starlark.Value.
func (s *starlarkComponent) Type() string {
	return "benthos.component"
}

func serializeStarlarkToJSON(thread *starlark.Thread, value starlark.Value) ([]byte, error) {
	encode := starlarkjson.Module.Members["encode"]
	encoded, err := starlark.Call(thread, encode, starlark.Tuple{value}, nil)
	if err != nil {
		return nil, err
	}
	str, ok := encoded.(starlark.String)
	if !ok {
		return nil, fmt.Errorf("unable to encode json, expected string, got: %T", encoded)
	}
	return []byte(str.GoString()), nil
}
