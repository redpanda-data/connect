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

package output

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

var header = "This document was generated with `benthos --list-outputs`" + `

An output is a sink where we wish to send our consumed data after applying an
optional array of [processors](../processors). Only one output is configured at
the root of a Benthos config. However, the output can be a [broker](#broker)
which combines multiple outputs under a chosen brokering pattern.

An output config section looks like this:

` + "``` yaml" + `
output:
  type: foo
  foo:
    bar: baz
  processors:
  - type: qux
` + "```" + `

### Back Pressure

Benthos outputs apply back pressure to components upstream. This means if your
output target starts blocking traffic Benthos will gracefully stop consuming
until the issue is resolved.

### Retries

When a Benthos output fails to send a message the error is propagated back up to
the input, where depending on the protocol it will either be pushed back to the
source as a Noack (e.g. AMQP) or will be reattempted indefinitely with the
commit withheld until success (e.g. Kafka).

It's possible to instead have Benthos indefinitely retry an output until success
with a [` + "`retry`" + `](#retry) output. Some other outputs, such as the
[` + "`broker`" + `](#broker), might also retry indefinitely depending on their
configuration.

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific
outputs either by using the ` + "[`switch`](#switch)" + ` output, or a
` + "[`broker`](#broker)" + ` with the ` + "`fan_out`" + ` pattern and a
[filter processor](../processors/README.md#filter_parts) on each output, which
is a processor that drops messages if the condition does not pass.
Conditions are content aware logical operators that can be combined using
boolean logic.

For more information regarding conditions, including a full list of available
conditions please [read the docs here](../conditions/README.md).

### Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using
a ` + "[`try`](#try)" + ` output.`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our output types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Outputs\n")
	buf.WriteString(strings.Repeat("=", 7))
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
		def := Constructors[name]
		buf.WriteString(def.Description)

		hasPerformanced := false
		performance := func() {
			if !hasPerformanced {
				buf.WriteString("\n\n### Performance\n")
				hasPerformanced = true
			} else {
				buf.WriteString("\n")
			}
		}
		if def.Async {
			performance()
			buf.WriteString(`
This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field ` + "`max_in_flight`" + `.`)
		}
		if def.Batches {
			performance()
			buf.WriteString(`
This output benefits from sending messages as a batch for improved performance.
Batches can be formed at both the input and output level. You can find out more
[in this doc](../batching.md).`)
		}
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
