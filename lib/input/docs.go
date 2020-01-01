// Copyright (c) 2014 Ashley Jeffs
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

package input

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func sanitiseWithBatch(
	componentConfig interface{},
	batchConfig batch.PolicyConfig,
) (map[string]interface{}, error) {
	batchSanit, err := batch.SanitisePolicyConfig(batchConfig)
	if err != nil {
		return nil, err
	}

	cBytes, err := yaml.Marshal(componentConfig)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = yaml.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	hashMap["batching"] = batchSanit
	return hashMap, nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-inputs`" + `

An input is a source of data piped through an array of optional
[processors](../processors). Only one input is configured at the root of a
Benthos config. However, the root input can be a [broker](#broker) which
combines multiple inputs.

An input config section looks like this:

` + "``` yaml" + `
input:
  type: foo
  foo:
    bar: baz
  processors:
  - type: qux
` + "```" + ``

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Inputs\n")
	buf.WriteString(strings.Repeat("=", 6))
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
		def := Constructors[name]
		if def.Deprecated {
			continue
		}

		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		conf.Processors = nil
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
		buf.WriteString(def.Description)
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
