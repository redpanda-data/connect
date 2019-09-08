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
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeEncode] = TypeSpec{
		constructor: NewEncode,
		description: `
Encodes messages according to the selected scheme. Supported schemes are:
hex, base64.`,
	}
}

//------------------------------------------------------------------------------

// EncodeConfig contains configuration fields for the Encode processor.
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

func hexEncode(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	e := hex.NewEncoder(&buf)
	if _, err := e.Write(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func strToEncoder(str string) (encodeFunc, error) {
	switch str {
	case "base64":
		return base64Encode, nil
	case "hex":
		return hexEncode, nil
	}
	return nil, fmt.Errorf("encode scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Encode is a processor that can selectively encode parts of a message
// following a chosen scheme.
type Encode struct {
	conf EncodeConfig
	fn   encodeFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
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
func (c *Encode) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int, span opentracing.Span, part types.Part) error {
		newPart, err := c.fn(part.Get())
		if err == nil {
			newMsg.Get(i).Set(newPart)
		} else {
			c.log.Debugf("Failed to encode message part: %v\n", err)
			c.mErr.Incr(1)
		}
		return err
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	IteratePartsWithSpan(TypeEncode, c.conf.Parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Encode) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Encode) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
