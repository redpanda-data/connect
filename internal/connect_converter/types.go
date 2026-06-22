// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import "gopkg.in/yaml.v3"

// ConnectConfig is a normalized Kafka Connect connector configuration.
type ConnectConfig struct {
	Name  string
	Class string
	Props map[string]any
}

// Warning records something the converter could not fully map.
type Warning struct {
	Field   string
	Message string
}

// Result is the output of a conversion.
type Result struct {
	YAML     []byte
	Warnings []Warning
}

// Component is the input and/or output a connector maps to. A source sets
// Input, a sink sets Output, MirrorMaker sets both. A nil side becomes a
// commented TODO stub during assembly.
type Component struct {
	Input  *yaml.Node
	Output *yaml.Node
}

// ConverterRole distinguishes key vs value converters.
type ConverterRole int

const (
	KeyConverter ConverterRole = iota
	ValueConverter
)

// Prefix returns the Kafka Connect property prefix for this role.
func (r ConverterRole) Prefix() string {
	if r == KeyConverter {
		return "key.converter"
	}
	return "value.converter"
}

// SMTConfig is a single resolved transform (transforms.<alias>.*).
type SMTConfig struct {
	Alias string
	Type  string
	Props map[string]any // keys with the "transforms.<alias>." prefix stripped
}
