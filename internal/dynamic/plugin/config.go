/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package plugin

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

// FieldType describes the type of field.
type FieldType string

// Validate checks that the field type is valid.
func (f FieldType) Validate() error {
	switch f {
	case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeUnknown:
		return nil
	}
	return fmt.Errorf("invalid field kind: %q", f)
}

const (
	FieldTypeString  FieldType = "string"
	FieldTypeInt     FieldType = "int"
	FieldTypeFloat   FieldType = "float"
	FieldTypeBool    FieldType = "bool"
	FieldTypeUnknown FieldType = "unknown"
)

// FieldKind describes the kind of field.
type FieldKind string

// Validate checks that the field kind is valid.
func (f FieldKind) Validate() error {
	switch f {
	case FieldKindScalar, FieldKindMap, FieldKindList:
		return nil
	}
	return fmt.Errorf("invalid field kind: %q", f)
}

const (
	FieldKindScalar FieldKind = "scalar"
	FieldKindMap    FieldKind = "map"
	FieldKindList   FieldKind = "list"
)

// FieldConfig describes a configuration field used in the template.
type FieldConfig struct {
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	Type        *FieldType `yaml:"type,omitempty"`
	Kind        *FieldKind `yaml:"kind,omitempty"`
	Default     *any       `yaml:"default,omitempty"`
	Advanced    bool       `yaml:"advanced"`
}

func (c FieldConfig) toSpec() (*service.ConfigField, error) {
	fieldType := FieldTypeUnknown
	if c.Type != nil {
		fieldType = *c.Type
	}
	fieldKind := FieldKindScalar
	if c.Type != nil {
		fieldKind = *c.Kind
	}
	var f *service.ConfigField
	switch fieldKind {
	case FieldKindScalar:
		switch fieldType {
		case FieldTypeBool:
			f = service.NewBoolField(c.Name)
		case FieldTypeFloat:
			f = service.NewFloatField(c.Name)
		case FieldTypeInt:
			f = service.NewIntField(c.Name)
		case FieldTypeString:
			f = service.NewStringField(c.Name)
		case FieldTypeUnknown:
			f = service.NewAnyField(c.Name)
		default:
			return nil, fmt.Errorf("unexpected plugin.FieldType: %#v", fieldType)
		}
	case FieldKindList:
		switch fieldType {
		case FieldTypeBool:
			// TODO: This should be a BoolListField, but we don't have one yet.
			f = service.NewAnyListField(c.Name)
		case FieldTypeFloat:
			f = service.NewFloatListField(c.Name)
		case FieldTypeInt:
			f = service.NewIntListField(c.Name)
		case FieldTypeString:
			f = service.NewStringListField(c.Name)
		case FieldTypeUnknown:
			f = service.NewAnyListField(c.Name)
		default:
			return nil, fmt.Errorf("unexpected plugin.FieldType: %#v", fieldType)
		}
	case FieldKindMap:
		switch fieldType {
		case FieldTypeBool:
			// TODO: This should be a BoolMapField, but we don't have one yet.
			f = service.NewAnyMapField(c.Name)
		case FieldTypeFloat:
			f = service.NewFloatMapField(c.Name)
		case FieldTypeInt:
			f = service.NewIntMapField(c.Name)
		case FieldTypeString:
			f = service.NewStringMapField(c.Name)
		case FieldTypeUnknown:
			f = service.NewAnyMapField(c.Name)
		default:
			return nil, fmt.Errorf("unexpected plugin.FieldType: %#v", fieldType)
		}
	default:
		return nil, fmt.Errorf("unexpected plugin.FieldKind: %#v", fieldKind)
	}
	if c.Default != nil {
		f = f.Default(*c.Default)
	}
	if c.Advanced {
		f = f.Advanced()
	}
	if c.Description != "" {
		f = f.Description(c.Description)
	}
	return f, nil
}

// Validate checks that the field config is valid.
func (c *FieldConfig) Validate() error {
	if c.Name == "" {
		return errors.New("field name is required")
	}
	if c.Type != nil {
		if err := c.Type.Validate(); err != nil {
			return err
		}
	}
	if c.Kind != nil {
		if err := c.Kind.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// PluginType describes the type of plugin.
type PluginType string

// Validate checks that the plugin type is valid.
func (p PluginType) Validate() error {
	switch p {
	case PluginTypeInput, PluginTypeProcessor, PluginTypeOutput:
		return nil
	}
	return fmt.Errorf("invalid plugin type: %q", p)
}

const (
	PluginTypeInput     PluginType = "input"
	PluginTypeProcessor PluginType = "processor"
	PluginTypeOutput    PluginType = "output"
)

// Config describes a dynamic plugin over gRPC.
type Config struct {
	Name        string `yaml:"name"`
	Summary     string `yaml:"summary"`
	Description string `yaml:"description"`
	// The command to run for the plugin.
	Cmd    []string      `yaml:"command"`
	Type   PluginType    `yaml:"type"`
	Fields []FieldConfig `yaml:"fields"`
}

// Validate checks that the config is valid.
func (c *Config) Validate() error {
	if c.Name == "" {
		return errors.New("plugin name is required")
	}
	if len(c.Cmd) == 0 {
		return errors.New("plugin command is required")
	}
	if err := c.Type.Validate(); err != nil {
		return err
	}
	for _, field := range c.Fields {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) toSpec() (*service.ConfigSpec, error) {
	spec := service.NewConfigSpec()
	if c.Summary != "" {
		spec = spec.Summary(c.Summary)
	}
	if c.Description != "" {
		spec = spec.Description(c.Description)
	}
	for _, field := range c.Fields {
		fieldSpec, err := field.toSpec()
		if err != nil {
			return nil, err
		}
		spec = spec.Field(fieldSpec)
	}
	return spec, nil
}

func DiscoverAndRegisterPlugins(fs fs.FS, env *service.Environment, paths []string) error {
	paths, err := service.Globs(fs, paths...)
	if err != nil {
		return fmt.Errorf("failed to resolve template glob pattern: %w", err)
	}
	for _, path := range paths {
		b, err := service.ReadFile(fs, path)
		if err != nil {
			return fmt.Errorf("failed to read plugin config file %s: %w", path, err)
		}
		var cfg Config
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return fmt.Errorf("failed to unmarshal plugin config file %s: %w", path, err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("failed to validate plugin config file %s: %w", path, err)
		}
		spec, err := cfg.toSpec()
		if err != nil {
			return err
		}
		switch cfg.Type {
		case PluginTypeInput:
			err = RegisterInputPlugin(env, InputConfig{
				Name: cfg.Name,
				Cmd:  cfg.Cmd,
				Env:  environMap(),
				Spec: spec,
			})
		case PluginTypeOutput:
			err = RegisterOutputPlugin(env, OutputConfig{
				Name: cfg.Name,
				Cmd:  cfg.Cmd,
				Env:  environMap(),
				Spec: spec,
			})
		case PluginTypeProcessor:
			err = RegisterProcessorPlugin(env, ProcessorConfig{
				Name: cfg.Name,
				Cmd:  cfg.Cmd,
				Env:  environMap(),
				Spec: spec,
			})
		default:
			// Validated above
			panic("unreachable")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func environMap() map[string]string {
	env := make(map[string]string)
	for _, e := range os.Environ() {
		kv := strings.SplitN(e, "=", 2)
		if len(kv) == 2 {
			env[kv[0]] = kv[1]
		}
	}
	return env
}
