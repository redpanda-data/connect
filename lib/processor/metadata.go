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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["metadata"] = TypeSpec{
		constructor: NewMetadata,
		description: `
Performs operations on the metadata of a message. Metadata are key/value pairs
that are associated with a message within a Benthos pipeline. Message batches
usually carry the metadata of the last message to be added. Metadata values can
be referred to using configuration
[interpolation functions](../config_interpolation.md#metadata),
which allow you to set fields in certain outputs using these dynamic values.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions). This allows
you to set the contents of a metadata field using values taken from the message
payload.

### Operations

#### ` + "`set`" + `

Sets the value of a metadata key.`,
	}
}

//------------------------------------------------------------------------------

// MetadataConfig contains any configuration for the Metadata processor.
type MetadataConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Operator: "set",
		Key:      "example",
		Value:    `${!hostname}`,
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(msg types.Message, value []byte) error

func newMetadataSetOperator(key string) metadataOperator {
	return func(msg types.Message, value []byte) error {
		msg.SetMetadata(key, string(value))
		return nil
	}
}

func getMetadataOperator(opStr string, key string) (metadataOperator, error) {
	switch opStr {
	case "set":
		return newMetadataSetOperator(key), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Metadata is a processor that performs an operation on a Metadata payload.
type Metadata struct {
	interpolate bool
	valueBytes  []byte
	operator    metadataOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewMetadata returns a Metadata processor.
func NewMetadata(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	m := &Metadata{
		conf:  conf,
		log:   log.NewModule(".processor.metadata"),
		stats: stats,

		valueBytes: []byte(conf.Metadata.Value),

		mCount:     stats.GetCounter("processor.metadata.count"),
		mErr:       stats.GetCounter("processor.metadata.error"),
		mSucc:      stats.GetCounter("processor.metadata.success"),
		mSent:      stats.GetCounter("processor.metadata.sent"),
		mSentParts: stats.GetCounter("processor.metadata.parts.sent"),
	}

	m.interpolate = text.ContainsFunctionVariables(m.valueBytes)

	var err error
	if m.operator, err = getMetadataOperator(conf.Metadata.Operator, conf.Metadata.Key); err != nil {
		return nil, err
	}
	return m, nil
}

//------------------------------------------------------------------------------

// ProcessMessage performs a metadata operation on a message.
func (p *Metadata) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	valueBytes := p.valueBytes
	if p.interpolate {
		valueBytes = text.ReplaceFunctionVariables(msg, valueBytes)
	}

	if err := p.operator(msg, valueBytes); err != nil {
		p.mErr.Incr(1)
		p.log.Debugf("Failed to apply operator: %v\n", err)
	}

	msgs := [1]types.Message{msg}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(msg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
