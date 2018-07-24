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
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["decode"] = TypeSpec{
		constructor: NewDecode,
		description: `
Decodes parts of a message according to the selected scheme. Supported available
schemes are: base64.`,
	}
}

//------------------------------------------------------------------------------

// DecodeConfig contains any configuration for the Decode processor.
type DecodeConfig struct {
	Scheme string `json:"scheme" yaml:"scheme"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewDecodeConfig returns a DecodeConfig with default values.
func NewDecodeConfig() DecodeConfig {
	return DecodeConfig{
		Scheme: "base64",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type decodeFunc func(bytes []byte) ([]byte, error)

func base64Decode(b []byte) ([]byte, error) {
	e := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(b))
	return ioutil.ReadAll(e)
}

func strToDecoder(str string) (decodeFunc, error) {
	switch str {
	case "base64":
		return base64Decode, nil
	}
	return nil, fmt.Errorf("decode scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Decode is a processor that can selectively decodes parts of a message as a
// chosen scheme.
type Decode struct {
	conf DecodeConfig
	fn   decodeFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewDecode returns a Decode processor.
func NewDecode(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cor, err := strToDecoder(conf.Decode.Scheme)
	if err != nil {
		return nil, err
	}
	return &Decode{
		conf:  conf.Decode,
		fn:    cor,
		log:   log.NewModule(".processor.decode"),
		stats: stats,

		mCount:     stats.GetCounter("processor.decode.count"),
		mSucc:      stats.GetCounter("processor.decode.success"),
		mErr:       stats.GetCounter("processor.decode.error"),
		mSkipped:   stats.GetCounter("processor.decode.skipped"),
		mSent:      stats.GetCounter("processor.decode.sent"),
		mSentParts: stats.GetCounter("processor.decode.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message, attempts to decode parts of the message and
// returns the result.
func (c *Decode) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	newMsg := types.NewMessage(nil)
	lParts := msg.Len()

	noParts := len(c.conf.Parts) == 0
	for i, part := range msg.GetAll() {
		isTarget := noParts
		if !isTarget {
			nI := i - lParts
			for _, t := range c.conf.Parts {
				if t == nI || t == i {
					isTarget = true
					break
				}
			}
		}
		if !isTarget {
			newMsg.Append(part)
			continue
		}
		newPart, err := c.fn(part)
		if err == nil {
			c.mSucc.Incr(1)
			newMsg.Append(newPart)
		} else {
			c.log.Errorf("Failed to decode message part: %v\n", err)
			c.mErr.Incr(1)
		}
	}

	if newMsg.Len() == 0 {
		c.mSkipped.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	c.mSent.Incr(1)
	c.mSentParts.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
