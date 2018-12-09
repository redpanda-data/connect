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
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessField] = TypeSpec{
		constructor: NewProcessField,
		description: `
A processor that extracts the value of a field within payloads (currently only
JSON format is supported) then applies a list of processors to the extracted
value, and finally sets the field within the original payloads to the processed
result.

If the number of messages resulting from the processing steps does not match the
original count then this processor fails and the messages continue unchanged.
Therefore, you should avoid using batch and filter type processors in this list.`,
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
	children []types.Processor

	log log.Modular

	mCount              metrics.StatCounter
	mErr                metrics.StatCounter
	mErrJSONParse       metrics.StatCounter
	mErrMisaligned      metrics.StatCounter
	mErrMisalignedBatch metrics.StatCounter
	mSent               metrics.StatCounter
	mBatchSent          metrics.StatCounter
}

// NewProcessField returns a ProcessField processor.
func NewProcessField(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.ProcessField.Processors {
		prefix := fmt.Sprintf("%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &ProcessField{
		parts:    conf.ProcessField.Parts,
		path:     strings.Split(conf.ProcessField.Path, "."),
		children: children,

		log: log,

		mCount:              stats.GetCounter("count"),
		mErr:                stats.GetCounter("error"),
		mErrJSONParse:       stats.GetCounter("error.json_parse"),
		mErrMisaligned:      stats.GetCounter("error.misaligned"),
		mErrMisalignedBatch: stats.GetCounter("error.misaligned_messages"),
		mSent:               stats.GetCounter("sent"),
		mBatchSent:          stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *ProcessField) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	p.mCount.Incr(1)
	payload := msg.Copy()
	resMsgs := [1]types.Message{payload}
	msgs = resMsgs[:]

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, payload.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	reqMsg := message.New(make([][]byte, len(targetParts)))
	gParts := make([]*gabs.Container, len(targetParts))

	for i, index := range targetParts {
		reqMsg.Get(i).Set([]byte(""))
		var err error
		var jObj interface{}
		if jObj, err = payload.Get(index).JSON(); err != nil {
			p.mErrJSONParse.Incr(1)
			p.mErr.Incr(1)
			p.log.Errorf("Failed to decode part: %v\n", err)
		}
		if gParts[i], err = gabs.Consume(jObj); err != nil {
			p.mErrJSONParse.Incr(1)
			p.mErr.Incr(1)
			p.log.Errorf("Failed to decode part: %v\n", err)
		}
		gTarget := gParts[i].S(p.path...)
		switch t := gTarget.Data().(type) {
		case string:
			reqMsg.Get(i).Set([]byte(t))
		default:
			reqMsg.Get(i).SetJSON(gTarget.Data())
		}
	}

	resultMsgs, _ := ExecuteAll(p.children, reqMsg)
	resMsg := message.New(nil)
	for _, rMsg := range resultMsgs {
		rMsg.Iter(func(i int, p types.Part) error {
			resMsg.Append(p.Copy())
			return nil
		})
	}

	if exp, act := len(targetParts), resMsg.Len(); exp != act {
		p.mBatchSent.Incr(1)
		p.mSent.Incr(int64(payload.Len()))
		p.mErr.Incr(1)
		p.mErrMisalignedBatch.Incr(1)
		p.log.Errorf("Misaligned processor result batch. Expected %v messages, received %v\n", exp, act)
		resMsg.Iter(func(i int, p types.Part) error {
			FlagFail(p)
			return nil
		})
		return
	}

	for i, index := range targetParts {
		gParts[i].Set(string(resMsg.Get(i).Get()), p.path...)
		tPart := payload.Get(index)
		tPart.SetJSON(gParts[i].Data())
		tPartMeta := tPart.Metadata()
		resMsg.Get(i).Metadata().Iter(func(k, v string) error {
			tPartMeta.Set(k, v)
			return nil
		})
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(payload.Len()))
	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *ProcessField) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *ProcessField) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
