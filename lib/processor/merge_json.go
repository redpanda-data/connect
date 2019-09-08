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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMergeJSON] = TypeSpec{
		constructor: NewMergeJSON,
		description: `
Parses selected messages of a batch as JSON documents, attempts to merge them
into one single JSON document and then writes it to a new message at the end of
the batch. Merged parts are removed unless ` + "`retain_parts`" + ` is set to
true. The new merged message will contain the metadata of the first part to be
merged.`,
	}
}

//------------------------------------------------------------------------------

// MergeJSONConfig contains configuration fields for the MergeJSON processor.
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
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewMergeJSON returns a MergeJSON processor.
func NewMergeJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &MergeJSON{
		parts:  conf.MergeJSON.Parts,
		retain: conf.MergeJSON.RetainParts,
		log:    log,
		stats:  stats,

		mCount:     stats.GetCounter("count"),
		mErrJSONP:  stats.GetCounter("error.json_parse"),
		mErrJSONS:  stats.GetCounter("error.json_set"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *MergeJSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeMergeJSON, msg)
	defer func() {
		for _, s := range spans {
			s.Finish()
		}
	}()

	newPart := gabs.New()
	mergeFunc := func(index int) {
		jsonPart, err := msg.Get(index).JSON()
		if err == nil {
			jsonPart, err = message.CopyJSON(jsonPart)
		}
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return
		}

		gPart := gabs.Wrap(jsonPart)
		newPart.Merge(gPart)
	}

	var newMsg types.Message
	if p.retain {
		newMsg = msg.Copy()
	} else {
		newMsg = message.New(nil)
	}

	var firstPartCopy types.Part
	if len(p.parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			mergeFunc(i)
		}
		firstPartCopy = msg.Get(0).Copy()
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
		msg.Iter(func(i int, b types.Part) error {
			if _, isTarget := targetParts[i]; isTarget {
				mergeFunc(i)
			} else if !p.retain {
				newMsg.Append(b.Copy())
			}
			return nil
		})
		firstPartCopy = msg.Get(p.parts[0]).Copy()
	}

	i := newMsg.Append(firstPartCopy)
	if err := newMsg.Get(i).SetJSON(newPart.Data()); err != nil {
		p.mErrJSONS.Incr(1)
		p.mErr.Incr(1)
		p.log.Debugf("Failed to marshal merged part into json: %v\n", err)
		FlagErr(newMsg.Get(i), err)
	}

	msgs := [1]types.Message{newMsg}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *MergeJSON) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *MergeJSON) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
