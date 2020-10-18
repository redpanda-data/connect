package output

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
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

// DocsBatches returns a documentation paragraph regarding outputs that support
// batching.
var DocsBatches = `
This output benefits from sending messages as a batch for improved performance.
Batches can be formed at both the input and output level. You can find out more
[in this doc](/docs/configuration/batching).`

// DocsAsync returns a documentation paragraph regarding outputs that support
// asynchronous sends.
var DocsAsync = `
This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field ` + "`max_in_flight`" + `.`

var header = "This document was generated with `benthos --list-outputs`" + `

An output is a sink where we wish to send our consumed data after applying an
optional array of [processors](/docs/components/processors/about). Only one output is configured at
the root of a Benthos config. However, the output can be a [broker](/docs/components/outputs/broker)
which combines multiple outputs under a chosen brokering pattern.

An output config section looks like this:

` + "``` yaml" + `
output:
  s3:
    bucket: TODO
    path: "${!meta(\"kafka_topic\")}/${!json(\"message.id\")}.json"

  # Optional list of processing steps
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
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
with a [` + "`retry`" + `](/docs/components/outputs/retry) output. Some other outputs, such as the
[` + "`broker`" + `](/docs/components/outputs/broker), might also retry indefinitely depending on their
configuration.

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific
outputs either by using the ` + "[`switch`](/docs/components/outputs/switch)" + ` output, or a
` + "[`broker`](/docs/components/outputs/broker)" + ` with the ` + "`fan_out`" + ` pattern and a
[filter processor](/docs/components/processors/filter_parts) on each output, which
is a processor that drops messages if the condition does not pass.
Conditions are content aware logical operators that can be combined using
boolean logic.

For more information regarding conditions, including a full list of available
conditions please [read the docs here](/docs/components/conditions/about).

### Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using
a ` + "[`try`](/docs/components/outputs/try)" + ` output.`

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
	i := 0
	for _, name := range names {
		if Constructors[name].Status == docs.StatusDeprecated {
			continue
		}
		i++
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		def := Constructors[name]
		if def.Status == docs.StatusDeprecated {
			continue
		}

		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := conf.Sanitised(true); err == nil {
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
[in this doc](/docs/configuration/batching).`)
		}
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
