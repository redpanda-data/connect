package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	jmespath "github.com/jmespath/go-jmespath"
	"github.com/opentracing/opentracing-go"
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
This processor is useful for performing large mappings in order to restructure
JSON documents. In order to map documents using more advanced logic consider
instead using the ` + "[`awk` processor](/docs/components/processors/awk)" + `.

It is possible to create boolean queries with JMESPath, in order to filter
messages with boolean queries please instead use the
` + "[`jmespath`](/docs/components/conditions/jmespath)" + ` condition.`,
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
			partsFieldSpec,
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
) (Type, error) {
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

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *JMESPath) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		jsonPart, err := part.JSON()
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return err
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

	IteratePartsWithSpan(TypeJMESPath, p.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

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
