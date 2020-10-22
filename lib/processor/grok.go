package processor

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	"github.com/trivago/grok"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGrok] = TypeSpec{
		constructor: NewGrok,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Parses messages into a structured format by attempting to apply a list of Grok
patterns, if a pattern returns at least one value a resulting structured object
is created according to the chosen output format.`,
		Description: `
Currently only json is a supported output format.

Type hints within patterns are respected, therefore with the pattern
` + "`%{WORD:first},%{INT:second:int}`" + ` and a payload of ` + "`foo,1`" + `
the resulting payload would be ` + "`{\"first\":\"foo\",\"second\":1}`" + `.

### Performance

This processor currently uses the [Go RE2](https://golang.org/s/re2syntax)
regular expression engine, which is guaranteed to run in time linear to the size
of the input. However, this property often makes it less performant than pcre
based implementations of grok. For more information see
[https://swtch.com/~rsc/regexp/regexp1.html](https://swtch.com/~rsc/regexp/regexp1.html).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("patterns", "A list of patterns to attempt against the incoming messages."),
			docs.FieldCommon("pattern_definitions", "A map of pattern definitions that can be referenced within `patterns`."),
			docs.FieldCommon("output_format", "The structured output format.").HasOptions("json"),
			docs.FieldAdvanced("named_captures_only", "Whether to only capture values from named patterns."),
			docs.FieldAdvanced("use_default_patterns", "Whether to use a [default set of patterns](#default-patterns)."),
			docs.FieldAdvanced("remove_empty_values", "Whether to remove values that are empty from the resulting structure."),
			partsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "VPC Flow Logs",
				Summary: `
Grok can be used to parse unstructured logs such as VPC flow logs that look like this:

` + "```text" + `
2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK
` + "```" + `

Into structured objects that look like this:

` + "```json" + `
{"accountid":"123456789010","action":"ACCEPT","bytes":4249,"dstaddr":"172.31.16.21","dstport":22,"end":1418530070,"interfaceid":"eni-1235b8ca123456789","logstatus":"OK","packets":20,"protocol":6,"srcaddr":"172.31.16.139","srcport":20641,"start":1418530010,"version":2}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
    - grok:
        output_format: json
        patterns:
          - '%{VPCFLOWLOG}'
        pattern_definitions:
          VPCFLOWLOG: '%{NUMBER:version:int} %{NUMBER:accountid} %{NOTSPACE:interfaceid} %{NOTSPACE:srcaddr} %{NOTSPACE:dstaddr} %{NOTSPACE:srcport:int} %{NOTSPACE:dstport:int} %{NOTSPACE:protocol:int} %{NOTSPACE:packets:int} %{NOTSPACE:bytes:int} %{NUMBER:start:int} %{NUMBER:end:int} %{NOTSPACE:action} %{NOTSPACE:logstatus}'
`,
			},
		},
		Footnotes: `
## Default Patterns

A summary of the default patterns on offer can be [found here](https://github.com/trivago/grok/blob/master/patterns.go#L5).`,
	}
}

//------------------------------------------------------------------------------

// GrokConfig contains configuration fields for the Grok processor.
type GrokConfig struct {
	Parts              []int             `json:"parts" yaml:"parts"`
	Patterns           []string          `json:"patterns" yaml:"patterns"`
	RemoveEmpty        bool              `json:"remove_empty_values" yaml:"remove_empty_values"`
	NamedOnly          bool              `json:"named_captures_only" yaml:"named_captures_only"`
	UseDefaults        bool              `json:"use_default_patterns" yaml:"use_default_patterns"`
	To                 string            `json:"output_format" yaml:"output_format"`
	PatternDefinitions map[string]string `json:"pattern_definitions" yaml:"pattern_definitions"`
}

// NewGrokConfig returns a GrokConfig with default values.
func NewGrokConfig() GrokConfig {
	return GrokConfig{
		Parts:              []int{},
		Patterns:           []string{},
		RemoveEmpty:        true,
		NamedOnly:          true,
		UseDefaults:        true,
		To:                 "json",
		PatternDefinitions: make(map[string]string),
	}
}

//------------------------------------------------------------------------------

// Grok is a processor that executes Grok queries on a message part and replaces
// the contents with the result.
type Grok struct {
	parts    []int
	gparsers []*grok.CompiledGrok

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrGrok   metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewGrok returns a Grok processor.
func NewGrok(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	gcompiler, err := grok.New(grok.Config{
		RemoveEmptyValues:   conf.Grok.RemoveEmpty,
		NamedCapturesOnly:   conf.Grok.NamedOnly,
		SkipDefaultPatterns: !conf.Grok.UseDefaults,
		Patterns:            conf.Grok.PatternDefinitions,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create grok compiler: %v", err)
	}

	var compiled []*grok.CompiledGrok
	for _, pattern := range conf.Grok.Patterns {
		var gcompiled *grok.CompiledGrok
		if gcompiled, err = gcompiler.Compile(pattern); err != nil {
			return nil, fmt.Errorf("failed to compile Grok pattern '%v': %v", pattern, err)
		}
		compiled = append(compiled, gcompiled)
	}

	g := &Grok{
		parts:    conf.Grok.Parts,
		gparsers: compiled,
		conf:     conf,
		log:      log,
		stats:    stats,

		mCount:     stats.GetCounter("count"),
		mErrGrok:   stats.GetCounter("error.grok_no_matches"),
		mErrJSONS:  stats.GetCounter("error.json_set"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (g *Grok) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	g.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		body := part.Get()

		var values map[string]interface{}
		for _, compiler := range g.gparsers {
			var err error
			if values, err = compiler.ParseTyped(body); err != nil {
				g.log.Debugf("Failed to parse body: %v\n", err)
				continue
			}
			if len(values) > 0 {
				break
			}
		}

		if len(values) == 0 {
			g.mErrGrok.Incr(1)
			g.mErr.Incr(1)
			g.log.Debugf("No matches found for payload: %s\n", body)
			return errors.New("no pattern matches found")
		}

		if err := newMsg.Get(index).SetJSON(values); err != nil {
			g.mErrJSONS.Incr(1)
			g.mErr.Incr(1)
			g.log.Debugf("Failed to convert grok result into json: %v\n", err)
			return err
		}

		return nil
	}

	IteratePartsWithSpan(TypeGrok, g.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	g.mBatchSent.Incr(1)
	g.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (g *Grok) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (g *Grok) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
