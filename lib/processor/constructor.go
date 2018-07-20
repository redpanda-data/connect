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
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/config"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// TypeSpec Constructor and a usage description for each processor type.
type TypeSpec struct {
	constructor func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
	) (Type, error)
	description        string
	sanitiseConfigFunc func(conf Config) (interface{}, error)
}

// Constructors is a map of all processor types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all processor types.
type Config struct {
	Type         string             `json:"type" yaml:"type"`
	Archive      ArchiveConfig      `json:"archive" yaml:"archive"`
	Batch        BatchConfig        `json:"batch" yaml:"batch"`
	BoundsCheck  BoundsCheckConfig  `json:"bounds_check" yaml:"bounds_check"`
	Combine      CombineConfig      `json:"combine" yaml:"combine"`
	Compress     CompressConfig     `json:"compress" yaml:"compress"`
	Conditional  ConditionalConfig  `json:"conditional" yaml:"conditional"`
	Decode       DecodeConfig       `json:"decode" yaml:"decode"`
	Decompress   DecompressConfig   `json:"decompress" yaml:"decompress"`
	Dedupe       DedupeConfig       `json:"dedupe" yaml:"dedupe"`
	Encode       EncodeConfig       `json:"encode" yaml:"encode"`
	Filter       FilterConfig       `json:"filter" yaml:"filter"`
	FilterParts  FilterPartsConfig  `json:"filter_parts" yaml:"filter_parts"`
	Grok         GrokConfig         `json:"grok" yaml:"grok"`
	HashSample   HashSampleConfig   `json:"hash_sample" yaml:"hash_sample"`
	HTTP         HTTPConfig         `json:"http" yaml:"http"`
	InsertPart   InsertPartConfig   `json:"insert_part" yaml:"insert_part"`
	JMESPath     JMESPathConfig     `json:"jmespath" yaml:"jmespath"`
	JSON         JSONConfig         `json:"json" yaml:"json"`
	MergeJSON    MergeJSONConfig    `json:"merge_json" yaml:"merge_json"`
	ProcessField ProcessFieldConfig `json:"process_field" yaml:"process_field"`
	ProcessMap   ProcessMapConfig   `json:"process_map" yaml:"process_map"`
	Sample       SampleConfig       `json:"sample" yaml:"sample"`
	SelectParts  SelectPartsConfig  `json:"select_parts" yaml:"select_parts"`
	Split        struct{}           `json:"split" yaml:"split"`
	Text         TextConfig         `json:"text" yaml:"text"`
	Unarchive    UnarchiveConfig    `json:"unarchive" yaml:"unarchive"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:         "bounds_check",
		Archive:      NewArchiveConfig(),
		Batch:        NewBatchConfig(),
		BoundsCheck:  NewBoundsCheckConfig(),
		Combine:      NewCombineConfig(),
		Compress:     NewCompressConfig(),
		Conditional:  NewConditionalConfig(),
		Decode:       NewDecodeConfig(),
		Decompress:   NewDecompressConfig(),
		Dedupe:       NewDedupeConfig(),
		Encode:       NewEncodeConfig(),
		Filter:       NewFilterConfig(),
		FilterParts:  NewFilterPartsConfig(),
		Grok:         NewGrokConfig(),
		HashSample:   NewHashSampleConfig(),
		HTTP:         NewHTTPConfig(),
		InsertPart:   NewInsertPartConfig(),
		JMESPath:     NewJMESPathConfig(),
		JSON:         NewJSONConfig(),
		MergeJSON:    NewMergeJSONConfig(),
		ProcessField: NewProcessFieldConfig(),
		ProcessMap:   NewProcessMapConfig(),
		Sample:       NewSampleConfig(),
		SelectParts:  NewSelectPartsConfig(),
		Split:        struct{}{},
		Text:         NewTextConfig(),
		Unarchive:    NewUnarchiveConfig(),
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := config.Sanitised{}
	outputMap["type"] = conf.Type
	if sfunc := Constructors[conf.Type].sanitiseConfigFunc; sfunc != nil {
		if outputMap[conf.Type], err = sfunc(conf); err != nil {
			return nil, err
		}
	} else {
		outputMap[conf.Type] = hashMap[conf.Type]
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

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

var header = "This document was generated with `benthos --list-processors`." + `

Benthos processors are functions that will be applied to each message passing
through a pipeline. The function signature allows a processor to mutate or drop
messages depending on the content of the message.

Processors are set via config, and depending on where in the config they are
placed they will be run either immediately after a specific input (set in the
input section), on all messages (set in the pipeline section) or before a
specific output (set in the output section).

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Batching and Multiple Part Messages

All Benthos processors support mulitple part messages, which are synonymous with
batches. Some processors such as [combine](#combine), [batch](#batch) and
[split](#split) are able to create, expand and break down batches.

Many processors are able to perform their behaviours on specific parts of a
message batch, or on all parts, and have a field ` + "`parts`" + ` for
specifying and array of part indexes they should apply to. If the list of target
parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element with be selected, and so on.`

var footer = `
[0]: ./examples.md`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our buffer types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Processors\n")
	buf.WriteString(strings.Repeat("=", 10))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = yaml.Marshal(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(Constructors[name].description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n")
		}
	}
	buf.WriteString(footer)
	return buf.String()
}

// New creates a processor type based on a processor configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	return nil, types.ErrInvalidProcessorType
}

//------------------------------------------------------------------------------
