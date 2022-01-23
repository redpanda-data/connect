package input

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/util/config"
)

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-inputs`" + `

An input is a source of data piped through an array of optional
[processors](/docs/components/processors/about). Only one input is configured at the root of a
Benthos config. However, the root input can be a [broker](/docs/components/inputs/broker) which
combines multiple inputs.

An input config section looks like this:

` + "```yaml" + `
input:
  redis_streams:
    url: tcp://localhost:6379
    streams:
      - benthos_stream
    body_key: body
    consumer_group: benthos_group

  # Optional list of processing steps
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
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
		conf.Processors = nil
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
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
