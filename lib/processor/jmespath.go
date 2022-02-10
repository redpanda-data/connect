package processor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	jmespath "github.com/jmespath/go-jmespath"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJMESPath] = TypeSpec{
		constructor: NewJMESPath,
		Categories: []Category{
			CategoryMapping,
		},
		Summary: `
Executes a [JMESPath query](http://jmespath.org/) on JSON documents and replaces
the message with the resulting document.`,
		Description: `
:::note Try out Bloblang
For better performance and improved capabilities try out native Benthos mapping with the [bloblang processor](/docs/components/processors/bloblang).
:::
`,
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
    - jmespath:
        query: "locations[?state == 'WA'].name | sort(@) | {Cities: join(', ', @)}"
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("query", "The JMESPath query to apply to messages."),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// JMESPathConfig contains configuration fields for the JMESPath processor.
type JMESPathConfig struct {
	Parts []int  `json:"parts" yaml:"parts"`
	Query string `json:"query" yaml:"query"`
}

// NewJMESPathConfig returns a JMESPathConfig with default values.
func NewJMESPathConfig() JMESPathConfig {
	return JMESPathConfig{
		Parts: []int{},
		Query: "",
	}
}

//------------------------------------------------------------------------------

// JMESPath is a processor that executes JMESPath queries on a message part and
// replaces the contents with the result.
type JMESPath struct {
	parts []int
	query *jmespath.JMESPath

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErrJMES   metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewJMESPath returns a JMESPath processor.
func NewJMESPath(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	query, err := jmespath.Compile(conf.JMESPath.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JMESPath query: %v", err)
	}
	j := &JMESPath{
		parts: conf.JMESPath.Parts,
		query: query,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErrJSONP:  stats.GetCounter("error.json_parse"),
		mErrJMES:   stats.GetCounter("error.jmespath_search"),
		mErrJSONS:  stats.GetCounter("error.json_set"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return j, nil
}

//------------------------------------------------------------------------------

func safeSearch(part interface{}, j *jmespath.JMESPath) (res interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jmespath panic: %v", r)
		}
	}()
	return j.Search(part)
}

// JMESPath doesn't like json.Number so we walk the tree and replace them.
func clearNumbers(v interface{}) (interface{}, bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, v := range t {
			if nv, ok := clearNumbers(v); ok {
				t[k] = nv
			}
		}
	case []interface{}:
		for i, v := range t {
			if nv, ok := clearNumbers(v); ok {
				t[i] = nv
			}
		}
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			if i, err := t.Int64(); err == nil {
				return i, true
			}
		}
		return f, true
	}
	return nil, false
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *JMESPath) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part *message.Part) error {
		jsonPart, err := part.JSON()
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return err
		}
		if v, replace := clearNumbers(jsonPart); replace {
			jsonPart = v
		}

		var result interface{}
		if result, err = safeSearch(jsonPart, p.query); err != nil {
			p.mErrJMES.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to search json: %v\n", err)
			return err
		}

		if err = newMsg.Get(index).SetJSON(result); err != nil {
			p.mErrJSONS.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to convert jmespath result into part: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2(TypeJMESPath, p.parts, newMsg, proc)

	msgs := [1]*message.Batch{newMsg}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *JMESPath) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *JMESPath) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
