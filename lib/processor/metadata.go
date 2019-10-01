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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetadata] = TypeSpec{
		constructor: NewMetadata,
		description: `
Performs operations on the metadata of a message. Metadata are key/value pairs
that are associated with message parts of a batch. Metadata values can be
referred to using configuration
[interpolation functions](../config_interpolation.md#metadata),
which allow you to set fields in certain outputs using these dynamic values.

This processor will interpolate functions within both the
` + "`key` and `value`" + ` fields, you can find a list of functions
[here](../config_interpolation.md#functions). This allows you to set the
contents of a metadata field using values taken from the message payload.

Value interpolations are resolved once per batch. In order to resolve them per
message of a batch place it within a ` + "[`for_each`](#for_each)" + `
processor:

` + "``` yaml" + `
for_each:
- metadata:
    operator: set
    key: foo
    value: ${!json_field:document.foo}
` + "```" + `

### Operators

#### ` + "`set`" + `

Sets the value of a metadata key.

#### ` + "`delete`" + `

Removes all metadata values from the message where the key matches the value
provided. If the value field is left empty the key value will instead be used.

#### ` + "`delete_all`" + `

Removes all metadata values from the message.

#### ` + "`delete_prefix`" + `

Removes all metadata values from the message where the key is prefixed with the
value provided. If the value field is left empty the key value will instead be
used as the prefix.`,
	}
}

//------------------------------------------------------------------------------

// MetadataConfig contains configuration fields for the Metadata processor.
type MetadataConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Parts:    []int{},
		Operator: "set",
		Key:      "example",
		Value:    `${!hostname}`,
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(m types.Metadata, key, value string) error

func newMetadataSetOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		m.Set(key, value)
		return nil
	}
}

func newMetadataDeleteAllOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		m.Iter(func(k, _ string) error {
			m.Delete(k)
			return nil
		})
		return nil
	}
}

func newMetadataDeleteOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		target := value
		if len(target) == 0 && len(key) > 0 {
			target = key
		}
		m.Delete(target)
		return nil
	}
}

func newMetadataDeletePrefixOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		prefix := value
		if len(prefix) == 0 && len(key) > 0 {
			prefix = key
		}
		m.Iter(func(k, _ string) error {
			if strings.HasPrefix(k, prefix) {
				m.Delete(k)
			}
			return nil
		})
		return nil
	}
}

func getMetadataOperator(opStr string) (metadataOperator, error) {
	switch opStr {
	case "set":
		return newMetadataSetOperator(), nil
	case "delete":
		return newMetadataDeleteOperator(), nil
	case "delete_all":
		return newMetadataDeleteAllOperator(), nil
	case "delete_prefix":
		return newMetadataDeletePrefixOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Metadata is a processor that performs an operation on the Metadata of a
// message.
type Metadata struct {
	value *text.InterpolatedString
	key   *text.InterpolatedString

	operator metadataOperator

	parts []int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewMetadata returns a Metadata processor.
func NewMetadata(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	m := &Metadata{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.Metadata.Parts,

		value: text.NewInterpolatedString(conf.Metadata.Value),
		key:   text.NewInterpolatedString(conf.Metadata.Key),

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	if m.operator, err = getMetadataOperator(conf.Metadata.Operator); err != nil {
		return nil, err
	}
	return m, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Metadata) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	key := p.key.Get(msg)
	value := p.value.Get(msg)

	proc := func(index int, span opentracing.Span, part types.Part) error {
		if err := p.operator(part.Metadata(), key, value); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to apply operator: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeMetadata, p.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Metadata) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *Metadata) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
