// Copyright 2026 Redpanda Data, Inc.
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

package rpcn

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	runtimepb "github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/runtimepb"
)

// ----------------------------------------------------------------------
// internal descriptor ↔ proto
// ----------------------------------------------------------------------

func (d *specDescriptor) toProto() *runtimepb.ConfigSpec {
	if d == nil {
		return nil
	}
	fields := make([]*runtimepb.ConfigField, len(d.fields))
	for i, f := range d.fields {
		fields[i] = f.toProto()
	}
	return &runtimepb.ConfigSpec{
		Summary:     d.summary,
		Description: d.description,
		Version:     d.version,
		Categories:  d.categories,
		Fields:      fields,
	}
}

func (f *fieldDescriptor) toProto() *runtimepb.ConfigField {
	if f == nil {
		return nil
	}
	children := make([]*runtimepb.ConfigField, len(f.children))
	for i, c := range f.children {
		children[i] = c.toProto()
	}
	pb := &runtimepb.ConfigField{
		Name:        f.name,
		Description: f.description,
		Type:        f.fieldType.toProto(),
		Kind:        f.fieldKind.toProto(),
		Optional:    f.optional,
		Advanced:    f.advanced,
		Deprecated:  f.deprecated,
		Children:    children,
	}
	if f.hasDefault {
		pb.DefaultJson = f.defaultJSON
	}
	return pb
}

func (t fieldTypeCode) toProto() runtimepb.FieldType {
	switch t {
	case fieldTypeString:
		return runtimepb.FieldType_FIELD_TYPE_STRING
	case fieldTypeInt:
		return runtimepb.FieldType_FIELD_TYPE_INT
	case fieldTypeFloat:
		return runtimepb.FieldType_FIELD_TYPE_FLOAT
	case fieldTypeBool:
		return runtimepb.FieldType_FIELD_TYPE_BOOL
	case fieldTypeObject:
		return runtimepb.FieldType_FIELD_TYPE_OBJECT
	}
	return runtimepb.FieldType_FIELD_TYPE_UNSPECIFIED
}

func (k fieldKindCode) toProto() runtimepb.FieldKind {
	switch k {
	case fieldKindScalar:
		return runtimepb.FieldKind_FIELD_KIND_SCALAR
	case fieldKindList:
		return runtimepb.FieldKind_FIELD_KIND_LIST
	case fieldKindMap:
		return runtimepb.FieldKind_FIELD_KIND_MAP
	}
	return runtimepb.FieldKind_FIELD_KIND_UNSPECIFIED
}

// ----------------------------------------------------------------------
// descriptor → real *service.ConfigSpec
// ----------------------------------------------------------------------

// descriptorToServiceSpec walks our internal descriptor and builds a
// real *service.ConfigSpec using the public benthos builder. The result
// is functionally indistinguishable from a spec built directly in-tree
// (modulo Go-closure linters and Go-closure custom-omit-when callbacks,
// which by definition can't cross process boundaries).
//
// Used by both the plugin's own runtime (to parse incoming config
// values into a *service.ParsedConfig) and the host adapter (to
// register the component on its real service.Environment).
func descriptorToServiceSpec(d *specDescriptor) (*service.ConfigSpec, error) {
	spec := service.NewConfigSpec()
	if d == nil {
		return spec, nil
	}
	if d.summary != "" {
		spec = spec.Summary(d.summary)
	}
	if d.description != "" {
		spec = spec.Description(d.description)
	}
	if d.version != "" {
		spec = spec.Version(d.version)
	}
	if len(d.categories) > 0 {
		spec = spec.Categories(d.categories...)
	}
	for _, f := range d.fields {
		sf, err := fieldToServiceField(f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.name, err)
		}
		spec = spec.Field(sf)
	}
	return spec, nil
}

func fieldToServiceField(f *fieldDescriptor) (*service.ConfigField, error) {
	if f == nil {
		return nil, errors.New("nil field descriptor")
	}
	var sf *service.ConfigField
	switch f.fieldKind {
	case fieldKindScalar, fieldKindUnspecified:
		switch f.fieldType {
		case fieldTypeString:
			sf = service.NewStringField(f.name)
		case fieldTypeInt:
			sf = service.NewIntField(f.name)
		case fieldTypeFloat:
			sf = service.NewFloatField(f.name)
		case fieldTypeBool:
			sf = service.NewBoolField(f.name)
		case fieldTypeObject:
			children := make([]*service.ConfigField, 0, len(f.children))
			for _, c := range f.children {
				cs, err := fieldToServiceField(c)
				if err != nil {
					return nil, err
				}
				children = append(children, cs)
			}
			sf = service.NewObjectField(f.name, children...)
		default:
			return nil, fmt.Errorf("unsupported scalar field type %v", f.fieldType)
		}
	default:
		return nil, fmt.Errorf("unsupported field kind %v (PoC supports scalar + object only)", f.fieldKind)
	}
	if f.description != "" {
		sf = sf.Description(f.description)
	}
	if f.optional {
		sf = sf.Optional()
	}
	if f.advanced {
		sf = sf.Advanced()
	}
	if f.hasDefault {
		var v any
		if err := json.Unmarshal([]byte(f.defaultJSON), &v); err != nil {
			return nil, fmt.Errorf("decoding default for field %q: %w", f.name, err)
		}
		sf = sf.Default(v)
	}
	return sf, nil
}
