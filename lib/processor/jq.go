package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/itchyny/gojq"
)

func init() {
	Constructors[TypeJQ] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newJQ(conf.JQ, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2ToV1Processor("jq", p, mgr.Metrics()), nil
		},
		Status: docs.StatusStable,
		Categories: []Category{
			CategoryMapping,
		},
		Summary: `
Transforms and filters messages using jq queries.`,
		Description: `
:::note Try out Bloblang
For better performance and improved capabilities try out native Benthos mapping with the [bloblang processor](/docs/components/processors/bloblang).
:::

The provided query is executed on each message, targeting either the contents
as a structured JSON value or as a raw string using the field ` + "`raw`" + `,
and the message is replaced with the query result.

Message metadata is also accessible within the query from the variable
` + "`$metadata`" + `.

This processor uses the [gojq library][gojq], and therefore does not require
jq to be installed as a dependency. However, this also means there are some
differences in how these queries are executed versus the jq cli which you can
[read about here][gojq-difference].

If the query does not emit any value then the message is filtered, if the query
returns multiple values then the resulting message will be an array containing
all values.

The full query syntax is described in [jq's documentation][jq-docs].

## Error Handling

Queries can fail, in which case the message remains unchanged, errors are
logged, and the message is flagged as having failed, allowing you to use
[standard processor error handling patterns](/docs/configuration/error_handling).`,
		Footnotes: `
[gojq]: https://github.com/itchyny/gojq
[gojq-difference]: https://github.com/itchyny/gojq#difference-to-jq
[jq-docs]: https://stedolan.github.io/jq/manual/`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "Mapping",
				Summary: `
When receiving JSON documents of the form:

` + "```json" + `
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
` + "```" + `

We could collapse the location names from the state of Washington into a field ` + "`Cities`" + `:

` + "```json" + `
{"Cities": "Bellevue, Olympia, Seattle"}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
    - jq:
        query: '{Cities: .locations | map(select(.state == "WA").name) | sort | join(", ") }'
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("query", "The jq query to filter and transform messages with."),
			docs.FieldAdvanced("raw", "Whether to process the input as a raw string instead of as JSON."),
			docs.FieldAdvanced("output_raw", "Whether to output raw text (unquoted) instead of JSON strings when the emitted values are string types."),
		},
	}
}

//------------------------------------------------------------------------------

// JQConfig contains configuration fields for the JQ processor.
type JQConfig struct {
	Query     string `json:"query" yaml:"query"`
	Raw       bool   `json:"raw" yaml:"raw"`
	OutputRaw bool   `json:"output_raw" yaml:"output_raw"`
}

// NewJQConfig returns a JQConfig with default values.
func NewJQConfig() JQConfig {
	return JQConfig{
		Query: ".",
	}
}

//------------------------------------------------------------------------------

var jqCompileOptions = []gojq.CompilerOption{
	gojq.WithVariables([]string{"$metadata"}),
}

type jqProc struct {
	inRaw  bool
	outRaw bool
	log    log.Modular
	code   *gojq.Code
}

func newJQ(conf JQConfig, mgr interop.Manager) (*jqProc, error) {
	j := &jqProc{
		inRaw:  conf.Raw,
		outRaw: conf.OutputRaw,
		log:    mgr.Logger(),
	}

	query, err := gojq.Parse(conf.Query)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq query: %w", err)
	}

	j.code, err = gojq.Compile(query, jqCompileOptions...)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %w", err)
	}

	return j, nil
}

func (j *jqProc) getPartMetadata(part *message.Part) map[string]interface{} {
	metadata := map[string]interface{}{}
	_ = part.MetaIter(func(k, v string) error {
		metadata[k] = v
		return nil
	})
	return metadata
}

func (j *jqProc) getPartValue(part *message.Part, raw bool) (obj interface{}, err error) {
	if raw {
		return string(part.Get()), nil
	}
	obj, err = part.JSON()
	if err == nil {
		obj, err = message.CopyJSON(obj)
	}
	if err != nil {
		j.log.Debugf("Failed to parse part into json: %v\n", err)
		return nil, err
	}
	return obj, nil
}

func (j *jqProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	part := msg.Copy()

	in, err := j.getPartValue(part, j.inRaw)
	if err != nil {
		return nil, err
	}
	metadata := j.getPartMetadata(part)

	var emitted []interface{}
	iter := j.code.Run(in, metadata)
	for {
		out, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := out.(error); ok {
			j.log.Debugf(err.Error())
			return nil, err
		}
		emitted = append(emitted, out)
	}

	if j.outRaw {
		raw, err := j.marshalRaw(emitted)
		if err != nil {
			j.log.Debugf("Failed to marshal raw text: %s", err)
			return nil, err
		}

		// Sometimes the query result is an empty string. Example:
		//    echo '{ "foo": "" }' | jq .foo
		// In that case we want pass on the empty string instead of treating it as
		// an empty message and dropping it
		if len(raw) == 0 && len(emitted) == 0 {
			return nil, nil
		}

		part.Set(raw)
		return []*message.Part{part}, nil
	} else if len(emitted) > 1 {
		if err = part.SetJSON(emitted); err != nil {
			j.log.Debugf("Failed to set part JSON: %v\n", err)
			return nil, err
		}
	} else if len(emitted) == 1 {
		if err = part.SetJSON(emitted[0]); err != nil {
			j.log.Debugf("Failed to set part JSON: %v\n", err)
			return nil, err
		}
	} else {
		return nil, nil
	}
	return []*message.Part{part}, nil
}

func (*jqProc) Close(ctx context.Context) error {
	return nil
}

func (j *jqProc) marshalRaw(values []interface{}) ([]byte, error) {
	buf := bytes.NewBufferString("")

	for index, el := range values {
		var rawResult []byte

		val, isString := el.(string)
		if isString {
			rawResult = []byte(val)
		} else {
			marshalled, err := json.Marshal(el)
			if err != nil {
				return nil, fmt.Errorf("failed marshal JQ result at index %d: %w", index, err)
			}

			rawResult = marshalled
		}

		if _, err := buf.Write(rawResult); err != nil {
			return nil, fmt.Errorf("failed to write JQ result at index %d: %w", index, err)
		}
	}

	bs := buf.Bytes()
	return bs, nil
}
