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
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["process_field"] = TypeSpec{
		constructor: NewProcessField,
		description: `
A processor that extracts the value of a field within payloads as a string
(currently only JSON format is supported) then applies a list of processors to
the extracted value, and finally sets the field within the original payloads to
the processed result as a string.

If the number of messages resulting from the processing steps does not match the
original count then this processor fails and the messages continue unchanged.
Therefore, you should avoid using batch and filter type processors in this list.

### Batch Ordering

This processor supports batch messages. When processing results are mapped back
into the original payload they will be correctly aligned with the original
batch. However, the ordering of field extracted message parts as they are sent
through processors are not guaranteed to match the ordering of the original
batch.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.ProcessField.Processors))
			for i, pConf := range conf.ProcessField.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"parts":      conf.ProcessField.Parts,
				"path":       conf.ProcessField.Path,
				"processors": procConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// ProcessFieldConfig is a config struct containing fields for the ProcessField
// processor.
type ProcessFieldConfig struct {
	Parts      []int    `json:"parts" yaml:"parts"`
	Path       string   `json:"path" yaml:"path"`
	Processors []Config `json:"processors" yaml:"processors"`
}

// NewProcessFieldConfig returns a default ProcessFieldConfig.
func NewProcessFieldConfig() ProcessFieldConfig {
	return ProcessFieldConfig{
		Parts:      []int{},
		Path:       "",
		Processors: []Config{},
	}
}

//------------------------------------------------------------------------------

// ProcessField is a processor that applies a list of child processors to a
// field extracted from the original payload.
type ProcessField struct {
	parts    []int
	path     []string
	children []Type

	log log.Modular

	mCount              metrics.StatCounter
	mErr                metrics.StatCounter
	mErrJSONParse       metrics.StatCounter
	mErrMisaligned      metrics.StatCounter
	mErrMisalignedBatch metrics.StatCounter
	mSent               metrics.StatCounter
	mSentParts          metrics.StatCounter
	mDropped            metrics.StatCounter
}

// NewProcessField returns a ProcessField processor.
func NewProcessField(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	nsStats := metrics.Namespaced(stats, "processor.process_field")
	nsLog := log.NewModule(".processor.process_field")

	var children []Type
	for _, pconf := range conf.ProcessField.Processors {
		proc, err := New(pconf, mgr, nsLog, nsStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &ProcessField{
		parts:    conf.ProcessField.Parts,
		path:     strings.Split(conf.ProcessField.Path, "."),
		children: children,

		log: nsLog,

		mCount:              stats.GetCounter("processor.process_field.count"),
		mErr:                stats.GetCounter("processor.process_field.error"),
		mErrJSONParse:       stats.GetCounter("processor.process_field.error.json_parse"),
		mErrMisaligned:      stats.GetCounter("processor.process_field.error.misaligned"),
		mErrMisalignedBatch: stats.GetCounter("processor.process_field.error.misaligned_messages"),
		mSent:               stats.GetCounter("processor.process_field.sent"),
		mSentParts:          stats.GetCounter("processor.process_field.parts.sent"),
		mDropped:            stats.GetCounter("processor.process_field.dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage does nothing and returns the message unchanged.
func (p *ProcessField) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	p.mCount.Incr(1)
	payload := msg.ShallowCopy()
	resMsgs := [1]types.Message{payload}
	msgs = resMsgs[:]

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, payload.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	reqMsg := types.NewMessage(make([][]byte, len(targetParts)))
	gParts := make([]*gabs.Container, len(targetParts))

	for i, index := range targetParts {
		reqMsg.Set(i, []byte("nil"))
		var err error
		var jObj interface{}
		if jObj, err = payload.GetJSON(index); err != nil {
			p.mErrJSONParse.Incr(1)
			p.log.Errorf("Failed to decode part: %v\n", err)
		}
		if gParts[i], err = gabs.Consume(jObj); err != nil {
			p.mErrJSONParse.Incr(1)
			p.log.Errorf("Failed to decode part: %v\n", err)
		}
		gTarget := gParts[i].S(p.path...)
		switch t := gTarget.Data().(type) {
		case string:
			reqMsg.Set(i, []byte(t))
		default:
			reqMsg.Set(i, []byte(gTarget.String()))
		}
	}

	resultMsgs := []types.Message{reqMsg}
	for i := 0; len(resultMsgs) > 0 && i < len(p.children); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			var rMsgs []types.Message
			rMsgs, _ = p.children[i].ProcessMessage(m)
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	resMsg := types.NewMessage(nil)
	for _, rMsg := range resultMsgs {
		for _, part := range rMsg.GetAll() {
			resMsg.Append(part)
		}
	}

	if exp, act := len(targetParts), resMsg.Len(); exp != act {
		p.mSent.Incr(1)
		p.mSentParts.Incr(int64(payload.Len()))
		p.mErr.Incr(1)
		p.mErrMisalignedBatch.Incr(1)
		p.log.Errorf("Misaligned processor result batch. Expected %v messages, received %v\n", exp, act)
		return
	}

	for i, index := range targetParts {
		gParts[i].Set(string(resMsg.Get(i)), p.path...)
		payload.SetJSON(index, gParts[i].Data())
	}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(payload.Len()))
	return
}

//------------------------------------------------------------------------------
