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
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["merge_json"] = TypeSpec{
		constructor: NewMergeJSON,
		description: `
Parses selected message parts as JSON blobs, attempts to merge them into one
single JSON value and then writes it to a new message part at the end of the
message. Merged parts are removed unless ` + "`retain_parts`" + ` is set to
true.`,
	}
}

//------------------------------------------------------------------------------

// MergeJSONConfig contains any configuration for the MergeJSON processor.
type MergeJSONConfig struct {
	Parts       []int `json:"parts" yaml:"parts"`
	RetainParts bool  `json:"retain_parts" yaml:"retain_parts"`
}

// NewMergeJSONConfig returns a MergeJSONConfig with default values.
func NewMergeJSONConfig() MergeJSONConfig {
	return MergeJSONConfig{
		Parts:       []int{},
		RetainParts: false,
	}
}

//------------------------------------------------------------------------------

// MergeJSON is a processor that merges JSON parsed message parts into a single
// value.
type MergeJSON struct {
	parts  []int
	retain bool

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewMergeJSON returns a MergeJSON processor.
func NewMergeJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &MergeJSON{
		parts:  conf.MergeJSON.Parts,
		retain: conf.MergeJSON.RetainParts,
		log:    log.NewModule(".processor.merge_json"),
		stats:  stats,

		mCount:     stats.GetCounter("processor.merge_json.count"),
		mErrJSONP:  stats.GetCounter("processor.merge_json.error.json_parse"),
		mErrJSONS:  stats.GetCounter("processor.merge_json.error.json_set"),
		mSucc:      stats.GetCounter("processor.merge_json.success"),
		mSent:      stats.GetCounter("processor.merge_json.sent"),
		mSentParts: stats.GetCounter("processor.merge_json.parts.sent"),
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, returning one or more
// resulting messages or a response.
func (p *MergeJSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	newPart := gabs.New()
	mergeFunc := func(index int) {
		jsonPart, err := msg.GetJSON(index)
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return
		}

		var gPart *gabs.Container
		if gPart, err = gabs.Consume(jsonPart); err != nil {
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return
		}

		newPart.Merge(gPart)
	}

	var newMsg types.Message
	if p.retain {
		newMsg = msg.ShallowCopy()
	} else {
		newMsg = types.NewMessage(nil)
	}

	if len(p.parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			mergeFunc(i)
		}
	} else {
		targetParts := make(map[int]struct{}, len(p.parts))
		for _, part := range p.parts {
			if part < 0 {
				part = msg.Len() + part
			}
			if part < 0 || part >= msg.Len() {
				continue
			}
			targetParts[part] = struct{}{}
		}
		msg.Iter(func(i int, b []byte) error {
			if _, isTarget := targetParts[i]; isTarget {
				mergeFunc(i)
			} else if !p.retain {
				msg.Append(b)
			}
			return nil
		})
	}

	i := newMsg.Append([]byte{})
	if err := newMsg.SetJSON(i, newPart.Data()); err != nil {
		p.mErrJSONS.Incr(1)
		p.log.Debugf("Failed to marshal merged part into json: %v\n", err)
	} else {
		p.mSucc.Incr(1)
	}

	msgs := [1]types.Message{newMsg}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
