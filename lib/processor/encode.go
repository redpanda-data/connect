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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["encode"] = TypeSpec{
		constructor: NewEncode,
		description: `
Encodes parts of a message according to the selected scheme. Supported available
schemes are: base64.`,
	}
}

//------------------------------------------------------------------------------

// EncodeConfig contains any configuration for the Encode processor.
type EncodeConfig struct {
	Scheme string `json:"scheme" yaml:"scheme"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewEncodeConfig returns a EncodeConfig with default values.
func NewEncodeConfig() EncodeConfig {
	return EncodeConfig{
		Scheme: "base64",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type encodeFunc func(bytes []byte) ([]byte, error)

func base64Encode(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	e := base64.NewEncoder(base64.StdEncoding, &buf)
	e.Write(b)
	e.Close()

	return buf.Bytes(), nil
}

func strToEncoder(str string) (encodeFunc, error) {
	switch str {
	case "base64":
		return base64Encode, nil
	}
	return nil, fmt.Errorf("encode scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Encode is a processor that can selectively encodes parts of a message as a
// chosen scheme.
type Encode struct {
	conf EncodeConfig
	fn   encodeFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewEncode returns a Encode processor.
func NewEncode(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cor, err := strToEncoder(conf.Encode.Scheme)
	if err != nil {
		return nil, err
	}
	return &Encode{
		conf:  conf.Encode,
		fn:    cor,
		log:   log.NewModule(".processor.encode"),
		stats: stats,

		mCount:     stats.GetCounter("processor.encode.count"),
		mSucc:      stats.GetCounter("processor.encode.success"),
		mErr:       stats.GetCounter("processor.encode.error"),
		mSkipped:   stats.GetCounter("processor.encode.skipped"),
		mSent:      stats.GetCounter("processor.encode.sent"),
		mSentParts: stats.GetCounter("processor.encode.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message, attempts to encode parts of the message and
// returns the result.
func (c *Encode) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
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
			c.log.Debugf("Failed to encode message part: %v\n", err)
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
