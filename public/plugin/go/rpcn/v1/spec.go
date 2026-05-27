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
)

// ConfigSpec mirrors the public surface of *service.ConfigSpec. The
// builder methods on this type accumulate into an internal descriptor
// that's serialised to the host via the PluginRuntime protocol.
// At plugin-construction time the host transforms the descriptor back
// into a real *service.ConfigSpec for linting and config parsing.
type ConfigSpec struct {
	desc *specDescriptor
}

// ConfigField mirrors the public surface of *service.ConfigField.
type ConfigField struct {
	desc *fieldDescriptor
}

// specDescriptor is the internal representation that round-trips over
// the wire as runtimepb.ConfigSpec.
type specDescriptor struct {
	summary     string
	description string
	version     string
	categories  []string
	fields      []*fieldDescriptor
}

// fieldDescriptor is the internal representation of a single field.
// Maps 1:1 to runtimepb.ConfigField.
type fieldDescriptor struct {
	name        string
	description string
	fieldType   fieldTypeCode
	fieldKind   fieldKindCode
	defaultJSON string // JSON-encoded default; empty means "no default"
	hasDefault  bool
	optional    bool
	advanced    bool
	deprecated  bool
	children    []*fieldDescriptor
}

type fieldTypeCode int

const (
	fieldTypeUnspecified fieldTypeCode = iota
	fieldTypeString
	fieldTypeInt
	fieldTypeFloat
	fieldTypeBool
	fieldTypeObject
)

type fieldKindCode int

const (
	fieldKindUnspecified fieldKindCode = iota
	fieldKindScalar
	fieldKindList
	fieldKindMap
)

// ----------------------------------------------------------------------
// ConfigSpec — builder methods mirroring *service.ConfigSpec
// ----------------------------------------------------------------------

// NewConfigSpec creates an empty plugin configuration spec. Mirrors
// service.NewConfigSpec.
func NewConfigSpec() *ConfigSpec {
	return &ConfigSpec{desc: &specDescriptor{}}
}

// Summary sets a one-line component summary. Mirrors
// (*service.ConfigSpec).Summary.
func (c *ConfigSpec) Summary(text string) *ConfigSpec {
	c.desc.summary = text
	return c
}

// Description sets a longer markdown component description. Mirrors
// (*service.ConfigSpec).Description.
func (c *ConfigSpec) Description(text string) *ConfigSpec {
	c.desc.description = text
	return c
}

// Version stamps the version this component was introduced in.
// Mirrors (*service.ConfigSpec).Version.
func (c *ConfigSpec) Version(v string) *ConfigSpec {
	c.desc.version = v
	return c
}

// Categories tags the component with one or more documentation
// categories. Mirrors (*service.ConfigSpec).Categories.
func (c *ConfigSpec) Categories(cats ...string) *ConfigSpec {
	c.desc.categories = append(c.desc.categories, cats...)
	return c
}

// Field appends a field to the spec. Mirrors
// (*service.ConfigSpec).Field.
func (c *ConfigSpec) Field(f *ConfigField) *ConfigSpec {
	if f != nil {
		c.desc.fields = append(c.desc.fields, f.desc)
	}
	return c
}

// ----------------------------------------------------------------------
// ConfigField constructors — mirror service.NewXxxField
// ----------------------------------------------------------------------

// NewStringField mirrors service.NewStringField.
func NewStringField(name string) *ConfigField {
	return &ConfigField{desc: &fieldDescriptor{
		name:      name,
		fieldType: fieldTypeString,
		fieldKind: fieldKindScalar,
	}}
}

// NewIntField mirrors service.NewIntField.
func NewIntField(name string) *ConfigField {
	return &ConfigField{desc: &fieldDescriptor{
		name:      name,
		fieldType: fieldTypeInt,
		fieldKind: fieldKindScalar,
	}}
}

// NewFloatField mirrors service.NewFloatField.
func NewFloatField(name string) *ConfigField {
	return &ConfigField{desc: &fieldDescriptor{
		name:      name,
		fieldType: fieldTypeFloat,
		fieldKind: fieldKindScalar,
	}}
}

// NewBoolField mirrors service.NewBoolField.
func NewBoolField(name string) *ConfigField {
	return &ConfigField{desc: &fieldDescriptor{
		name:      name,
		fieldType: fieldTypeBool,
		fieldKind: fieldKindScalar,
	}}
}

// NewObjectField mirrors service.NewObjectField. Children describe the
// nested record's fields.
func NewObjectField(name string, children ...*ConfigField) *ConfigField {
	childDescs := make([]*fieldDescriptor, len(children))
	for i, c := range children {
		childDescs[i] = c.desc
	}
	return &ConfigField{desc: &fieldDescriptor{
		name:      name,
		fieldType: fieldTypeObject,
		fieldKind: fieldKindScalar,
		children:  childDescs,
	}}
}

// ----------------------------------------------------------------------
// ConfigField — modifier methods mirroring *service.ConfigField
// ----------------------------------------------------------------------

// Description annotates the field with documentation. Mirrors
// (*service.ConfigField).Description.
func (f *ConfigField) Description(text string) *ConfigField {
	f.desc.description = text
	return f
}

// Default sets a default value. Mirrors (*service.ConfigField).Default.
// The value is JSON-encoded for transport; supported value types are
// the standard Go scalars (string / int / float / bool) and slices /
// maps thereof.
func (f *ConfigField) Default(v any) *ConfigField {
	data, err := json.Marshal(v)
	if err != nil {
		// Encoding the default at construction time can't be deferred —
		// fall back to no default and let the host see the unencoded
		// field. Plugin authors get an error during config parsing if
		// the field ends up required.
		f.desc.hasDefault = false
		f.desc.defaultJSON = ""
		return f
	}
	f.desc.hasDefault = true
	f.desc.defaultJSON = string(data)
	return f
}

// Optional marks the field as optional. Mirrors
// (*service.ConfigField).Optional.
func (f *ConfigField) Optional() *ConfigField {
	f.desc.optional = true
	return f
}

// Advanced marks the field as advanced. Mirrors
// (*service.ConfigField).Advanced.
func (f *ConfigField) Advanced() *ConfigField {
	f.desc.advanced = true
	return f
}

// Deprecated marks the field as deprecated.
func (f *ConfigField) Deprecated() *ConfigField {
	f.desc.deprecated = true
	return f
}
