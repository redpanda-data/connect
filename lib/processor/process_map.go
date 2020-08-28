package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/mapper"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessMap] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			return NewProcessMap(conf.ProcessMap, mgr, log, stats)
		},
		Summary: `
A processor that extracts and maps fields identified via
[dot path](/docs/configuration/field_paths) from the original payload into a new
object, applies a list of processors to the newly constructed object, and
finally maps the result back into the original payload.

## Alternatives

All functionality of this processor has been superseded by the
[branch](/docs/components/processors/branch) processor.`,
		Deprecated: true,
		Description: `
This processor is useful for performing processors on subsections of a payload.
For example, you could extract sections of a JSON object in order to construct
a reduced request object for an ` + "[`http`](/docs/components/processors/http)" + `
processor, then map the result back into a field within the original object.

The order of stages of this processor are as follows:

- [Conditions](#conditions) are tested (if specified) against each message,
  messages that do not pass will not be processed.
- Messages that are flagged for processing are mapped according to the
  [premap](#premap) fields, creating a new object. If the premap stage fails
  (targets are not found) the message will not be processed.
- Messages that are mapped are processed as a batch.
- After all child processors are applied to the mapped messages they are mapped
  back into the original messages they originated from following the
  [postmap](#postmap) fields. If the postmap stage fails the mapping is skipped
  and the message payload remains as it started.

If the premap is empty then the full payload is sent to the processors, if the
postmap is empty then the processed result replaces the original contents
entirely.

### Batch Ordering

This processor supports batched messages, but the list of processors to apply
must NOT change the ordering (or count) of the messages (do not use a
` + "`group_by`" + ` processor, for example).

### Error Handling

When premap, processing or postmap stages fail the underlying message will
remain unchanged, the errors are logged, and the message is flagged as having
failed, allowing you to use
[standard processor error handling patterns](/docs/configuration/error_handling)
for recovery.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("conditions", "A list of [conditions](/docs/components/conditions/about) to test against messages. If any condition fails then the message will not be mapped and processed.",
				[]interface{}{
					map[string]interface{}{
						"bloblang": "document.urls.length() > 0",
					},
				},
			),
			docs.FieldCommon(
				"premap", "A map of source to destination [paths](/docs/configuration/field_paths) used to create a new object from the original. An empty (or dot `.`) path indicates the root of the object. If a map source is not found then the message will not be processed, for optional sources use the field [`premap_optional`](#premap_optional).",
				map[string]string{
					".": "field.from.document",
				},
				map[string]string{
					"foo":     "root.body.foo",
					"bar.baz": "root.extra.baz",
				},
			),
			docs.FieldCommon("premap_optional", "A map of optional source to destination [paths](/docs/configuration/field_paths) used to create a new object from the original."),
			docs.FieldCommon("processors", "A list of processors to apply to mapped payloads."),
			docs.FieldCommon(
				"postmap", "A map of destination to source [paths](/docs/configuration/field_paths) used to map results from processing back into the original payload. An empty (or dot `.`) path indicates the root of the object. If a source is not found then the mapping is abandoned, for optional sources use the [`postmap_optional`](#postmap_optional) field.",
				map[string]string{
					"results.foo": ".",
				},
			),
			docs.FieldCommon("postmap_optional", "A map of optional destination to source [paths](/docs/configuration/field_paths) used to map results from processing back into the original payload."),
			partsFieldSpec,
		},
		Footnotes: `
## Examples

Given a message payload of:

` + "```json" + `
{
  "doc": {
    "id": "foo",
    "title": "foo bar baz",
    "description": "here's a thing",
    "content": "this is a body"
  }
}
` + "```" + `

We might wish to perform language detection on the ` + "`doc.content`" + ` field
by sending it to a hypothetical HTTP service. We do not wish to overwrite the
original document with the result, and instead want to place it within the path
` + "`doc.language`" + `, and so this is a good use case for ` + "`process_map`" + `:

` + "```yaml" + `
pipeline:
  processors:
    - process_map:
        premap:
          content: doc.content
        processors:
          - http:
              url: http://localhost:1234
        postmap:
          doc.language: .
` + "```" + `

With the above config we would send our target HTTP service the payload
` + "`{\"content\":\"this is a body\"}`" + `, and whatever the service returns
will get mapped into our original document:

` + "```json" + `
{
  "doc": {
    "id": "foo",
    "title": "foo bar baz",
    "description": "here's a thing",
    "content": "this is a body",
    "language": {
      "code": "en",
      "certainty": 0.2
    }
  }
}
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return conf.ProcessMap.Sanitise()
		},
	}
}

//------------------------------------------------------------------------------

// ProcessMapConfig is a config struct containing fields for the
// ProcessMap processor.
type ProcessMapConfig struct {
	Parts           []int              `json:"parts" yaml:"parts"`
	Conditions      []condition.Config `json:"conditions" yaml:"conditions"`
	Premap          map[string]string  `json:"premap" yaml:"premap"`
	PremapOptional  map[string]string  `json:"premap_optional" yaml:"premap_optional"`
	Postmap         map[string]string  `json:"postmap" yaml:"postmap"`
	PostmapOptional map[string]string  `json:"postmap_optional" yaml:"postmap_optional"`
	Processors      []Config           `json:"processors" yaml:"processors"`
}

// NewProcessMapConfig returns a default ProcessMapConfig.
func NewProcessMapConfig() ProcessMapConfig {
	return ProcessMapConfig{
		Parts:           []int{},
		Conditions:      []condition.Config{},
		Premap:          map[string]string{},
		PremapOptional:  map[string]string{},
		Postmap:         map[string]string{},
		PostmapOptional: map[string]string{},
		Processors:      []Config{},
	}
}

// Sanitise the configuration into a minimal structure that can be printed
// without changing the intent.
func (p ProcessMapConfig) Sanitise() (map[string]interface{}, error) {
	var err error
	condConfs := make([]interface{}, len(p.Conditions))
	for i, cConf := range p.Conditions {
		if condConfs[i], err = condition.SanitiseConfig(cConf); err != nil {
			return nil, err
		}
	}
	procConfs := make([]interface{}, len(p.Processors))
	for i, pConf := range p.Processors {
		if procConfs[i], err = SanitiseConfig(pConf); err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{
		"parts":            p.Parts,
		"conditions":       condConfs,
		"premap":           p.Premap,
		"premap_optional":  p.PremapOptional,
		"postmap":          p.Postmap,
		"postmap_optional": p.PostmapOptional,
		"processors":       procConfs,
	}, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a slice the
// default values are still applied.
func (p *ProcessMapConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias ProcessMapConfig
	aliased := confAlias(NewProcessMapConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*p = ProcessMapConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (p *ProcessMapConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias ProcessMapConfig
	aliased := confAlias(NewProcessMapConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*p = ProcessMapConfig(aliased)
	return nil
}

//------------------------------------------------------------------------------

// ProcessMap is a processor that applies a list of child processors to a new
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type ProcessMap struct {
	parts []int

	mapper   *mapper.Type
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mErrPre    metrics.StatCounter
	mErrProc   metrics.StatCounter
	mErrPost   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewProcessMap returns a ProcessField processor.
func NewProcessMap(
	conf ProcessMapConfig, mgr types.Manager, log log.Modular, stats metrics.Type,
) (*ProcessMap, error) {
	var children []types.Processor
	for i, pconf := range conf.Processors {
		prefix := fmt.Sprintf("processor.%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	var conditions []types.Condition
	for i, cconf := range conf.Conditions {
		prefix := fmt.Sprintf("condition.%v", i)
		cond, err := condition.New(cconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	p := &ProcessMap{
		parts: conf.Parts,

		children: children,

		log:        log,
		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mErrPre:    stats.GetCounter("error.premap"),
		mErrProc:   stats.GetCounter("error.processors"),
		mErrPost:   stats.GetCounter("error.postmap"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	if p.mapper, err = mapper.New(
		mapper.OptSetLogger(log),
		mapper.OptSetStats(stats),
		mapper.OptSetConditions(conditions),
		mapper.OptSetReqMap(conf.Premap),
		mapper.OptSetOptReqMap(conf.PremapOptional),
		mapper.OptSetResMap(conf.Postmap),
		mapper.OptSetOptResMap(conf.PostmapOptional),
	); err != nil {
		return nil, err
	}

	return p, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *ProcessMap) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	propMsg, propSpans := tracing.WithChildSpans(TypeProcessMap, msg.Copy())
	defer func() {
		for _, s := range propSpans {
			s.Finish()
		}
	}()

	result := msg.DeepCopy()
	err := p.CreateResult(propMsg)
	if err != nil {
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			return nil
		})
		msgs := [1]types.Message{result}
		return msgs[:], nil
	}

	var failed []int
	if failed, err = p.OverlayResult(result, propMsg); err != nil {
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			return nil
		})
		msgs := [1]types.Message{result}
		return msgs[:], nil
	}
	for _, i := range failed {
		FlagErr(result.Get(i), errors.New("failed to overlay result from map processors"))
	}

	msgs := [1]types.Message{result}
	return msgs[:], nil
}

// TargetsUsed returns a list of target dependencies of this processor derived
// from its premap and premap_optional fields.
func (p *ProcessMap) TargetsUsed() []string {
	return p.mapper.TargetsUsed()
}

// TargetsProvided returns a list of targets provided by this processor derived
// from its postmap and postmap_optional fields.
func (p *ProcessMap) TargetsProvided() []string {
	return p.mapper.TargetsProvided()
}

// CreateResult performs reduction and child processors to a payload. The size
// of the payload will remain unchanged, where reduced indexes are nil. This
// result can be overlayed onto the original message in order to complete the
// map.
func (p *ProcessMap) CreateResult(msg types.Message) error {
	p.mCount.Incr(1)

	if len(p.parts) > 0 {
		parts := make([]types.Part, msg.Len())
		for _, sel := range p.parts {
			index := sel
			if index < 0 {
				index = msg.Len() + index
			}
			if index < 0 || index >= msg.Len() {
				continue
			}
			parts[index] = msg.Get(index)
		}
		msg.SetAll(parts)
	}

	originalLen := msg.Len()

	skipped, failed := p.mapper.MapRequests(msg)
	if msg.Len() == 0 {
		msg.SetAll(make([]types.Part, originalLen))
		errMapFailed := errors.New("mapping failed for this message")
		for _, i := range failed {
			FlagErr(msg.Get(i), errMapFailed)
		}
		return nil
	}

	procResults, err := processMap(msg, p.children)
	if err != nil {
		p.mErrProc.Incr(1)
		p.mErr.Incr(1)
		p.log.Errorf("Processors failed: %v\n", err)
		return err
	}

	var alignedResult types.Message
	if alignedResult, err = p.mapper.AlignResult(originalLen, skipped, failed, procResults); err != nil {
		p.mErrPost.Incr(1)
		p.mErr.Incr(1)
		p.log.Errorf("Postmap failed: %v\n", err)
		return err
	}

	for _, i := range failed {
		FlagFail(alignedResult.Get(i))
	}

	alignedParts := make([]types.Part, alignedResult.Len())
	for i := range alignedParts {
		alignedParts[i] = alignedResult.Get(i)
	}
	msg.SetAll(alignedParts)
	return nil
}

// OverlayResult attempts to merge the result of a process_map with the original
//  payload as per the map specified in the postmap and postmap_optional fields.
func (p *ProcessMap) OverlayResult(payload, response types.Message) ([]int, error) {
	failed, err := p.mapper.MapResponses(payload, response)
	if err != nil {
		p.mErrPost.Incr(1)
		p.mErr.Incr(1)
		p.log.Errorf("Postmap failed: %v\n", err)
		return nil, err
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(payload.Len()))
	return failed, nil
}

func processMap(mappedMsg types.Message, processors []types.Processor) ([]types.Message, error) {
	requestMsgs, res := ExecuteAll(processors, mappedMsg)
	if res != nil && res.Error() != nil {
		return nil, res.Error()
	}

	if len(requestMsgs) == 0 {
		return nil, errors.New("processors resulted in zero messages")
	}

	return requestMsgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *ProcessMap) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *ProcessMap) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}
