package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/itchyny/gojq"
	"github.com/opentracing/opentracing-go"
)

func init() {
	Constructors[TypeJQ] = TypeSpec{
		constructor: NewJQ,
		Status:      docs.StatusBeta,
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
		},
	}
}

//------------------------------------------------------------------------------

// JQConfig contains configuration fields for the JQ processor.
type JQConfig struct {
	Query string `json:"query" yaml:"query"`
	Raw   bool   `json:"raw" yaml:"raw"`
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

// JQ is a processor that passes messages through gojq.
type JQ struct {
	conf  JQConfig
	log   log.Modular
	stats metrics.Type
	code  *gojq.Code

	mCount        metrics.StatCounter
	mCountParts   metrics.StatCounter
	mSent         metrics.StatCounter
	mBatchSent    metrics.StatCounter
	mDropped      metrics.StatCounter
	mDroppedParts metrics.StatCounter
	mErr          metrics.StatCounter
	mErrJSONParse metrics.StatCounter
	mErrJSONSet   metrics.StatCounter
	mErrQuery     metrics.StatCounter
}

// NewJQ returns a JQ processor.
func NewJQ(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &JQ{
		conf:  conf.JQ,
		stats: stats,
		log:   log,

		mCount:        stats.GetCounter("count"),
		mCountParts:   stats.GetCounter("count_parts"),
		mSent:         stats.GetCounter("sent"),
		mBatchSent:    stats.GetCounter("batch.count"),
		mDropped:      stats.GetCounter("dropped"),
		mDroppedParts: stats.GetCounter("dropped_num_parts"),
		mErr:          stats.GetCounter("error"),
		mErrJSONParse: stats.GetCounter("error.json_parse"),
		mErrJSONSet:   stats.GetCounter("error.json_set"),
		mErrQuery:     stats.GetCounter("error.query"),
	}

	query, err := gojq.Parse(j.conf.Query)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq query: %w", err)
	}

	j.code, err = gojq.Compile(query, jqCompileOptions...)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %w", err)
	}

	return j, nil
}

//------------------------------------------------------------------------------

func (j *JQ) getPartMetadata(part types.Part) map[string]interface{} {
	metadata := map[string]interface{}{}
	part.Metadata().Iter(func(k, v string) error {
		metadata[k] = v
		return nil
	})
	return metadata
}

func (j *JQ) getPartValue(part types.Part, raw bool) (obj interface{}, err error) {
	if raw {
		return string(part.Get()), nil
	}
	obj, err = part.JSON()
	if err == nil {
		obj, err = message.CopyJSON(obj)
	}
	if err != nil {
		j.mErrJSONParse.Incr(1)
		j.log.Debugf("Failed to parse part into json: %v\n", err)
		return nil, err
	}
	return obj, nil
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (j *JQ) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	j.mCount.Incr(1)

	newMsg := msg.Copy()
	iteratePartsFilterableWithSpan(TypeJQ, nil, newMsg, func(index int, span opentracing.Span, part types.Part) (bool, error) {
		in, err := j.getPartValue(part, j.conf.Raw)
		if err != nil {
			j.mErr.Incr(1)
			return false, err
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
				j.log.Debugf("Failed to query part: %v\n", err)
				j.mErr.Incr(1)
				j.mErrQuery.Incr(1)
				return false, err
			}

			j.mSent.Incr(1)
			emitted = append(emitted, out)
		}

		if len(emitted) > 1 {
			if err = part.SetJSON(emitted); err != nil {
				j.log.Debugf("Failed to set part JSON: %v\n", err)
				j.mErr.Incr(1)
				j.mErrJSONSet.Incr(1)
				return false, err
			}
		} else if len(emitted) == 1 {
			if err = part.SetJSON(emitted[0]); err != nil {
				j.log.Debugf("Failed to set part JSON: %v\n", err)
				j.mErr.Incr(1)
				j.mErrJSONSet.Incr(1)
				return false, err
			}
		} else {
			j.mDroppedParts.Incr(1)
			return false, nil
		}

		return true, nil
	})

	if newMsg.Len() == 0 {
		j.mDropped.Incr(1)
		return nil, response.NewAck()
	}

	j.mBatchSent.Incr(1)
	j.mSent.Incr(int64(newMsg.Len()))

	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (*JQ) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (*JQ) WaitForClose(timeout time.Duration) error {
	return nil
}
