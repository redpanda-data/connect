package pure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newJQ(conf.JQ, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("jq", p, mgr), nil
	}, docs.ComponentSpec{
		Name:   "jq",
		Status: docs.StatusStable,
		Categories: []string{
			"Mapping",
		},
		Summary: `
Transforms and filters messages using jq queries.`,
		Description: `
:::note Try out Bloblang
For better performance and improved capabilities try out native Benthos mapping with the [` + "`mapping`" + ` processor](/docs/components/processors/mapping).
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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("query", "The jq query to filter and transform messages with."),
			docs.FieldBool("raw", "Whether to process the input as a raw string instead of as JSON.").Advanced(),
			docs.FieldBool("output_raw", "Whether to output raw text (unquoted) instead of JSON strings when the emitted values are string types.").Advanced(),
		).ChildDefaultAndTypesFromStruct(processor.NewJQConfig()),
	})
	if err != nil {
		panic(err)
	}
}

var jqCompileOptions = []gojq.CompilerOption{
	gojq.WithVariables([]string{"$metadata"}),
}

type jqProc struct {
	inRaw  bool
	outRaw bool
	log    log.Modular
	code   *gojq.Code
}

func newJQ(conf processor.JQConfig, mgr bundle.NewManagement) (*jqProc, error) {
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

func (j *jqProc) getPartMetadata(part *message.Part) map[string]any {
	metadata := map[string]any{}
	_ = part.MetaIterMut(func(k string, v any) error {
		metadata[k] = v
		return nil
	})
	return metadata
}

func (j *jqProc) getPartValue(part *message.Part, raw bool) (obj any, err error) {
	if raw {
		return string(part.AsBytes()), nil
	}
	if obj, err = part.AsStructured(); err != nil {
		j.log.Debugf("Failed to parse part into json: %v\n", err)
		return nil, err
	}
	return obj, nil
}

func safeQuery(input any, meta map[string]any, c *gojq.Code) (emitted []any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jq panic: %v", r)
		}
	}()

	iter := c.Run(input, meta)
	for {
		out, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok = out.(error); ok {
			_ = err.Error() // This can panic :(
			return
		}
		emitted = append(emitted, out)
	}
	return
}

func (j *jqProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	in, err := j.getPartValue(msg, j.inRaw)
	if err != nil {
		return nil, err
	}
	metadata := j.getPartMetadata(msg)

	emitted, err := safeQuery(in, metadata, j.code)
	if err != nil {
		j.log.Debugf(err.Error())
		return nil, err
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

		msg.SetBytes(raw)
		return []*message.Part{msg}, nil
	} else if len(emitted) > 1 {
		msg.SetStructuredMut(emitted)
	} else if len(emitted) == 1 {
		msg.SetStructuredMut(emitted[0])
	} else {
		return nil, nil
	}
	return []*message.Part{msg}, nil
}

func (*jqProc) Close(ctx context.Context) error {
	return nil
}

func (j *jqProc) marshalRaw(values []any) ([]byte, error) {
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
