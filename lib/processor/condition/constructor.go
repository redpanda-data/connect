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

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/config"
	"github.com/Jeffail/benthos/lib/log"
	yaml "gopkg.in/yaml.v2"
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
	description string
}

// Constructors is a map of all condition types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all condition types.
type Config struct {
	Type     string         `json:"type" yaml:"type"`
	And      AndConfig      `json:"and" yaml:"and"`
	Content  ContentConfig  `json:"content" yaml:"content"`
	Count    CountConfig    `json:"count" yaml:"count"`
	JMESPath JMESPathConfig `json:"jmespath" yaml:"jmespath"`
	Not      NotConfig      `json:"not" yaml:"not"`
	Or       OrConfig       `json:"or" yaml:"or"`
	Resource string         `json:"resource" yaml:"resource"`
	Static   bool           `json:"static" yaml:"static"`
	Xor      XorConfig      `json:"xor" yaml:"xor"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:     "content",
		And:      NewAndConfig(),
		Content:  NewContentConfig(),
		Count:    NewCountConfig(),
		JMESPath: NewJMESPathConfig(),
		Not:      NewNotConfig(),
		Or:       NewOrConfig(),
		Resource: "",
		Static:   true,
		Xor:      NewXorConfig(),
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
	outputMap["type"] = hashMap["type"]
	outputMap[conf.Type] = hashMap[conf.Type]

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

var header = "This document was generated with `benthos --list-conditions`" + `

Within the list of Benthos [processors][0] you will find the [filter][1]
processor, which applies a condition to every message and only propagates them
if the condition passes. Conditions themselves can modify ('not') and combine
('and', 'or') other conditions, and can therefore be used to create complex
filters.

The format of a condition is similar to other Benthos types:

` + "``` yaml" + `
condition:
  type: content
  content:
    operator: equals
    part: 0
    arg: hello world
` + "```" + `

And using boolean condition types we can combine multiple conditions together:

` + "``` yaml" + `
condition:
  type: and
  and:
  - type: content
    content:
      operator: contains
      arg: hello world
  - type: or
    or:
    - type: content
      content:
        operator: contains
        arg: foo
    - type: not
      not:
        type: content
        content:
          operator: contains
          arg: bar
` + "```" + `

The above example could be summarised as 'content contains "hello world" and
also either contains "foo" or does _not_ contain "bar"'.

Conditions can be extremely useful for creating filters on an output. By using a
fan out output broker with 'filter' processors on the brokered outputs it is
possible to build
[curated data streams](../concepts.md#content-based-multiplexing) that filter on
the content of each message.

### Reusing Conditions

Sometimes large chunks of logic are reused across processors, or nested multiple
times as branches of a larger condition. It is possible to avoid writing
duplicate condition configs by using the [resource condition][2].`

var footer = `
[0]: ../processors/README.md
[1]: ../processors/README.md#filter
[2]: #resource`

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

// New creates a condition type based on a condition configuration.
func New(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	return nil, types.ErrInvalidConditionType
}

//------------------------------------------------------------------------------
