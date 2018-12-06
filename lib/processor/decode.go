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
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDecode] = TypeSpec{
		constructor: NewDecode,
		description: `
Decodes parts of a message according to the selected scheme. Supported available
schemes are: base64.`,
	}
}

//------------------------------------------------------------------------------

// DecodeConfig contains configuration fields for the Decode processor.
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

// Decode is a processor that can selectively decode parts of a message
// following a chosen scheme.
type Decode struct {
	conf DecodeConfig
	fn   decodeFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
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
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Decode) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int) {
		part := msg.Get(i).Get()
		newPart, err := c.fn(part)
		if err == nil {
			newMsg.Get(i).Set(newPart)
		} else {
			c.log.Errorf("Failed to decode message part: %v\n", err)
			c.mErr.Incr(1)
			FlagFail(newMsg.Get(i))
		}
	}

	if len(c.conf.Parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			proc(i)
		}
	} else {
		for _, i := range c.conf.Parts {
			proc(i)
		}
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
