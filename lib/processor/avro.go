// Copyright (c) 2019 Ashley Jeffs
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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/linkedin/goavro/v2"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAvro] = TypeSpec{
		constructor: NewAvro,
		description: `
EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Performs Avro based operations on messages based on a schema. Supported encoding
types are textual, binary and single.

### Operators

#### ` + "`to_json`" + `

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

#### ` + "`from_json`" + `

Attempts to convert JSON documents into Avro documents according to the
specified encoding.`,
	}
}

//------------------------------------------------------------------------------

// AvroConfig contains configuration fields for the Avro processor.
type AvroConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Encoding string `json:"encoding" yaml:"encoding"`
	Schema   string `json:"schema" yaml:"schema"`
}

// NewAvroConfig returns a AvroConfig with default values.
func NewAvroConfig() AvroConfig {
	return AvroConfig{
		Parts:    []int{},
		Operator: "to_json",
		Encoding: "textual",
		Schema:   "",
	}
}

//------------------------------------------------------------------------------

type avroOperator func(part types.Part) error

func newAvroToJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part types.Part) error {
			jObj, _, err := codec.NativeFromTextual(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	case "binary":
		return func(part types.Part) error {
			jObj, _, err := codec.NativeFromBinary(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	case "single":
		return func(part types.Part) error {
			jObj, _, err := codec.NativeFromSingle(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func newAvroFromJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part types.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var textual []byte
			if textual, err = codec.TextualFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(textual)
			return nil
		}, nil
	case "binary":
		return func(part types.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var binary []byte
			if binary, err = codec.BinaryFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(binary)
			return nil
		}, nil
	case "single":
		return func(part types.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var single []byte
			if single, err = codec.SingleFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(single)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func strToAvroOperator(opStr, encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch opStr {
	case "to_json":
		return newAvroToJSONOperator(encoding, codec)
	case "from_json":
		return newAvroFromJSONOperator(encoding, codec)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Avro is a processor that performs an operation on an Avro payload.
type Avro struct {
	parts    []int
	operator avroOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewAvro returns an Avro processor.
func NewAvro(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	a := &Avro{
		parts: conf.Avro.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	codec, err := goavro.NewCodec(conf.Avro.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	if a.operator, err = strToAvroOperator(conf.Avro.Operator, conf.Avro.Encoding, codec); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Avro) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		if err := p.operator(part); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Operator failed: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeAvro, p.parts, newMsg, proc)

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Avro) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *Avro) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
