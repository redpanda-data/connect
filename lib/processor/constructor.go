// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// typeSpec Constructor and a usage description for each processor type.
type typeSpec struct {
	constructor func(conf Config, log log.Modular, stats metrics.Type) (Type, error)
	description string
}

var constructors = map[string]typeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all processor types.
type Config struct {
	Type        string            `json:"type" yaml:"type"`
	BlobToMulti struct{}          `json:"blob_to_multi" yaml:"blob_to_multi"`
	BoundsCheck BoundsCheckConfig `json:"bounds_check" yaml:"bounds_check"`
	Combine     CombineConfig     `json:"combine" yaml:"combine"`
	Decompress  DecompressConfig  `json:"decompress" yaml:"decompress"`
	HashSample  HashSampleConfig  `json:"hash_sample" yaml:"hash_sample"`
	InsertPart  InsertPartConfig  `json:"insert_part" yaml:"insert_part"`
	MultiToBlob struct{}          `json:"multi_to_blob" yaml:"multi_to_blob"`
	Sample      SampleConfig      `json:"sample" yaml:"sample"`
	SelectParts SelectPartsConfig `json:"select_parts" yaml:"select_parts"`
	SetJSON     SetJSONConfig     `json:"set_json" yaml:"set_json"`
	Unarchive   UnarchiveConfig   `json:"unarchive" yaml:"unarchive"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:        "bounds_check",
		BlobToMulti: struct{}{},
		BoundsCheck: NewBoundsCheckConfig(),
		Combine:     NewCombineConfig(),
		Decompress:  NewDecompressConfig(),
		HashSample:  NewHashSampleConfig(),
		InsertPart:  NewInsertPartConfig(),
		MultiToBlob: struct{}{},
		Sample:      NewSampleConfig(),
		SelectParts: NewSelectPartsConfig(),
		SetJSON:     NewSetJSONConfig(),
		Unarchive:   NewUnarchiveConfig(),
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a slice the
// default values are still applied.
func (m *Config) UnmarshalJSON(bytes []byte) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*m = Config(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (m *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*m = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our buffer types alphabetically
	names := []string{}
	for name := range constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("PROCESSORS\n")
	buf.WriteString(strings.Repeat("=", 10))
	buf.WriteString("\n\n")
	buf.WriteString("This document has been generated with `benthos --list-processors`.")
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		buf.WriteString(constructors[name].description)
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

// New creates a processor type based on a processor configuration.
func New(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	if c, ok := constructors[conf.Type]; ok {
		return c.constructor(conf, log, stats)
	}
	return nil, types.ErrInvalidProcessorType
}

//------------------------------------------------------------------------------
