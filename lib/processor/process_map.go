// Copyright (c) 2018 Ashley Jeffs
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

package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
		description: `
A processor that extracts and maps fields identified via
[dot path](../field_paths.md) from the original payload into new objects,
applies a list of processors to the newly constructed objects, and finally maps
the result back into the original payload.

This processor is useful for performing processors on subsections of a payload.
For example, you could extract sections of a JSON object in order to construct
a request object for an ` + "`http`" + ` processor, then map the result back
into a field within the original object.

The order of stages of this processor are as follows:

- Conditions are applied to each _individual_ message part in the batch,
  determining whether the part will be mapped. If the conditions are empty all
  message parts will be mapped. If the field ` + "`parts`" + ` is populated the
  message parts not in this list are also excluded from mapping.
- Message parts that are flagged for mapping are mapped according to the premap
  fields, creating a new object. If the premap stage fails (targets are not
  found) the message part will not be processed.
- Message parts that are mapped are processed as a batch. You may safely break
  the batch into individual parts during processing with the ` + "`split`" + `
  processor.
- After all child processors are applied to the mapped messages they are mapped
  back into the original message parts they originated from as per your postmap.
  If the postmap stage fails the mapping is skipped and the message payload
  remains as it started.

Map paths are arbitrary dot paths, target path hierarchies are constructed if
they do not yet exist. Processing is skipped for message parts where the premap
targets aren't found, for optional premap targets use ` + "`premap_optional`" + `.

Map target paths that are parents of other map target paths will always be
mapped first, therefore it is possible to map subpath overrides.

If postmap targets are not found the merge is abandoned, for optional postmap
targets use ` + "`postmap_optional`" + `.

If the premap is empty then the full payload is sent to the processors, if the
postmap is empty then the processed result replaces the original contents
entirely.

Maps can reference the root of objects either with an empty string or '.', for
example the maps:

` + "``` yaml" + `
premap:
  .: foo.bar
postmap:
  foo.bar: .
` + "```" + `

Would create a new object where the root is the value of ` + "`foo.bar`" + ` and
would map the full contents of the result back into ` + "`foo.bar`" + `.

If the number of total message parts resulting from the processing steps does
not match the original count then this processor fails and the messages continue
unchanged. Therefore, you should avoid using batch and filter type processors in
this list.

### Batch Ordering

This processor supports batch messages. When message parts are post-mapped after
processing they will be correctly aligned with the original batch. However, the
ordering of premapped message parts as they are sent through processors are not
guaranteed to match the ordering of the original batch.`,
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
