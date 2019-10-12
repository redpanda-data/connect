// Copyright (c) 2018 Ashley Jeffs
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

package condition

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// TypeSpec Constructor and a usage description for each condition type.
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

// Constructors is a map of all condition types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each condition type.
var (
	TypeAll                = "all"
	TypeAnd                = "and"
	TypeAny                = "any"
	TypeBoundsCheck        = "bounds_check"
	TypeCheckField         = "check_field"
	TypeCheckInterpolation = "check_interpolation"
	TypeCount              = "count"
	TypeJMESPath           = "jmespath"
	TypeJSONSchema         = "json_schema"
	TypeNot                = "not"
	TypeNumber             = "number"
	TypeMetadata           = "metadata"
	TypeOr                 = "or"
	TypeProcessorFailed    = "processor_failed"
	TypeResource           = "resource"
	TypeStatic             = "static"
	TypeText               = "text"
	TypeXor                = "xor"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all condition types.
type Config struct {
	Type               string                   `json:"type" yaml:"type"`
	All                AllConfig                `json:"all" yaml:"all"`
	And                AndConfig                `json:"and" yaml:"and"`
	Any                AnyConfig                `json:"any" yaml:"any"`
	BoundsCheck        BoundsCheckConfig        `json:"bounds_check" yaml:"bounds_check"`
	CheckField         CheckFieldConfig         `json:"check_field" yaml:"check_field"`
	CheckInterpolation CheckInterpolationConfig `json:"check_interpolation" yaml:"check_interpolation"`
	Count              CountConfig              `json:"count" yaml:"count"`
	JMESPath           JMESPathConfig           `json:"jmespath" yaml:"jmespath"`
	JSONSchema         JSONSchemaConfig         `json:"json_schema" yaml:"json_schema"`
	Not                NotConfig                `json:"not" yaml:"not"`
	Number             NumberConfig             `json:"number" yaml:"number"`
	Metadata           MetadataConfig           `json:"metadata" yaml:"metadata"`
	Or                 OrConfig                 `json:"or" yaml:"or"`
	Plugin             interface{}              `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ProcessorFailed    ProcessorFailedConfig    `json:"processor_failed" yaml:"processor_failed"`
	Resource           string                   `json:"resource" yaml:"resource"`
	Static             bool                     `json:"static" yaml:"static"`
	Text               TextConfig               `json:"text" yaml:"text"`
	Xor                XorConfig                `json:"xor" yaml:"xor"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:               "text",
		And:                NewAndConfig(),
		BoundsCheck:        NewBoundsCheckConfig(),
		CheckField:         NewCheckFieldConfig(),
		CheckInterpolation: NewCheckInterpolationConfig(),
		Count:              NewCountConfig(),
		JMESPath:           NewJMESPathConfig(),
		Not:                NewNotConfig(),
		Number:             NewNumberConfig(),
		Metadata:           NewMetadataConfig(),
		Or:                 NewOrConfig(),
		Plugin:             nil,
		ProcessorFailed:    NewProcessorFailedConfig(),
		Resource:           "",
		Static:             true,
		Text:               NewTextConfig(),
		Xor:                NewXorConfig(),
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
		if _, exists := hashMap[conf.Type]; exists {
			outputMap[conf.Type] = hashMap[conf.Type]
		}
		if spec, exists := pluginSpecs[conf.Type]; exists {
			var plugSanit interface{}
			if spec.confSanitiser != nil {
				plugSanit = spec.confSanitiser(conf.Plugin)
			} else {
				plugSanit = hashMap["plugin"]
			}
			if plugSanit != nil {
				outputMap["plugin"] = plugSanit
			}
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (m *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("line %v: unable to infer type, multiple candidates '%v' and '%v'", value.Line, inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("line %v: unable to infer type, candidates were: %v", value.Line, typeCandidates)
		}
		aliased.Type = inferredType
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = conf
	} else {
		aliased.Plugin = nil
	}

	*m = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-conditions`" + `

Conditions are boolean queries that can be executed based on the contents of a
message. Some [processors][processors] such as ` + "[`filter`][filter]" + ` use
conditions for expressing their logic.

Conditions themselves can modify ` + "(`not`) and combine (`and`, `or`)" + `
other conditions, and can therefore be used to create complex boolean
expressions.

The format of a condition is similar to other Benthos types:

` + "``` yaml" + `
condition:
  type: text
  text:
    operator: equals
    part: 0
    arg: hello world
` + "```" + `

And using boolean condition types we can combine multiple conditions together:

` + "``` yaml" + `
condition:
  and:
  - text:
      operator: contains
      arg: hello world
  - or:
    - text:
        operator: contains
        arg: foo
    - not:
        text:
          operator: contains
          arg: bar
` + "```" + `

The above example could be summarised as 'text contains "hello world" and also
either contains "foo" or does _not_ contain "bar"'.

### Batching and Multipart Messages

All conditions can be applied to a multipart message, which is synonymous with a
batch. Some conditions target a specific part of a message batch, and require
you specify the target index with the field ` + "`part`" + `.

Some processors such as ` + "[`filter`][filter]" + ` apply its conditions across
the whole batch. Whereas other processors such as
 ` + "[`filter_parts`][filter_parts]" + ` will apply its conditions on each part
of a batch individually, in which case the condition acts as if it were
referencing a single message batch.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element with be selected, and so on.

### Reusing Conditions

Sometimes large chunks of logic are reused across processors, or nested multiple
times as branches of a larger condition. It is possible to avoid writing
duplicate condition configs by using the [resource condition][resource].`

var footer = `
[processors]: ../processors/README.md
[filter]: ../processors/README.md#filter
[filter_parts]: ../processors/README.md#filter_parts
[resource]: #resource`

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
	buf.WriteString("Conditions\n")
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
			confBytes, _ = config.MarshalYAML(confSanit)
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

// New creates a condition type based on a condition configuration.
func New(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log.NewModule("."+conf.Type), stats)
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		return c.constructor(conf.Plugin, mgr, log.NewModule("."+conf.Type), stats)
	}
	return nil, types.ErrInvalidConditionType
}

//------------------------------------------------------------------------------
