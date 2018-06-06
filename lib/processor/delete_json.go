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
	"errors"
	"strings"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["delete_json"] = TypeSpec{
		constructor: NewDeleteJSON,
		description: `
Parses a message part as a JSON blob, deletes a value at a given path (if it
exists), and writes the modified JSON back to the message part.

If the list of target parts is empty the processor will be applied to all
message parts. Part indexes can be negative, and if so the part will be selected
from the end counting backwards starting from -1. E.g. if part = -1 then the
selected part will be the last part of the message, if part = -2 then the part
before the last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// DeleteJSONConfig contains any configuration for the DeleteJSON processor.
type DeleteJSONConfig struct {
	Parts []int  `json:"parts" yaml:"parts"`
	Path  string `json:"path" yaml:"path"`
}

// NewDeleteJSONConfig returns a DeleteJSONConfig with default values.
func NewDeleteJSONConfig() DeleteJSONConfig {
	return DeleteJSONConfig{
		Parts: []int{},
		Path:  "",
	}
}

//------------------------------------------------------------------------------

// DeleteJSON is a processor that deletes a JSON field at a specific index.
type DeleteJSON struct {
	target []string
	parts  []int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount    metrics.StatCounter
	mErrJSONP metrics.StatCounter
	mErrJSONS metrics.StatCounter
	mSucc     metrics.StatCounter
	mSent     metrics.StatCounter
}

// NewDeleteJSON returns a DeleteJSON processor.
func NewDeleteJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &DeleteJSON{
		target: strings.Split(conf.DeleteJSON.Path, "."),
		parts:  conf.DeleteJSON.Parts,
		conf:   conf,
		log:    log.NewModule(".processor.delete_json"),
		stats:  stats,

		mCount:    stats.GetCounter("processor.delete_json.count"),
		mErrJSONP: stats.GetCounter("processor.delete_json.error.json_parse"),
		mErrJSONS: stats.GetCounter("processor.delete_json.error.json_set"),
		mSucc:     stats.GetCounter("processor.delete_json.success"),
		mSent:     stats.GetCounter("processor.delete_json.sent"),
	}
	if len(conf.DeleteJSON.Path) == 0 || conf.DeleteJSON.Path == "." {
		return nil, errors.New("path cannot be empty or the root '.'")
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message.
func (p *DeleteJSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	newMsg := msg.ShallowCopy()

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		jsonObj, err := msg.GetJSON(index)
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		var gPart *gabs.Container
		if gPart, err = gabs.Consume(jsonObj); err != nil {
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		gPart.Delete(p.target...)
		if err := newMsg.SetJSON(index, gPart.Data()); err != nil {
			p.mErrJSONS.Incr(1)
			p.log.Debugf("Failed to convert json into part: %v\n", err)
			continue
		}

		p.mSucc.Incr(1)
	}

	msgs := [1]types.Message{newMsg}

	p.mSent.Incr(1)
	return msgs[:], nil
}

//------------------------------------------------------------------------------
