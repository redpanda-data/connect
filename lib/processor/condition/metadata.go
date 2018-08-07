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

package condition

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetadata] = TypeSpec{
		constructor: NewMetadata,
		description: `
Metadata is a condition that checks metadata keys of a message part against an
operator from the following list:

### ` + "`exists`" + `

Checks whether a metadata key exists.

### ` + "`equals`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case insensitive.

### ` + "`equals_cs`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case sensitive.`,
	}
}

//------------------------------------------------------------------------------

// Errors for the metadata condition.
var (
	ErrInvalidMetadataOperator = errors.New("invalid metadata operator type")
)

// MetadataConfig is a configuration struct containing fields for the metadata
// condition.
type MetadataConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Part     int    `json:"part" yaml:"part"`
	Key      string `json:"key" yaml:"key"`
	Arg      string `json:"arg" yaml:"arg"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Operator: "equals_cs",
		Part:     0,
		Key:      "",
		Arg:      "",
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(md types.Metadata) bool

func metadataExistsOperator(key string) metadataOperator {
	return func(md types.Metadata) bool {
		return len(md.Get(key)) > 0
	}
}

func metadataEqualsOperator(key string, arg string) metadataOperator {
	return func(md types.Metadata) bool {
		return strings.ToLower(md.Get(key)) == strings.ToLower(arg)
	}
}

func metadataEqualsCSOperator(key string, arg string) metadataOperator {
	return func(md types.Metadata) bool {
		return md.Get(key) == arg
	}
}

func strToMetadataOperator(str, key, arg string) (metadataOperator, error) {
	switch str {
	case "exists":
		return metadataExistsOperator(key), nil
	case "equals":
		return metadataEqualsOperator(key, arg), nil
	case "equals_cs":
		return metadataEqualsCSOperator(key, arg), nil
	}
	return nil, ErrInvalidMetadataOperator
}

//------------------------------------------------------------------------------

// Metadata is a condition that checks message text against logical operators.
type Metadata struct {
	stats    metrics.Type
	operator metadataOperator
	part     int

	mSkippedEmpty metrics.StatCounter
	mSkipped      metrics.StatCounter
	mSkippedOOB   metrics.StatCounter
	mApplied      metrics.StatCounter
}

// NewMetadata returns a Metadata condition.
func NewMetadata(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToMetadataOperator(conf.Metadata.Operator, conf.Metadata.Key, conf.Metadata.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Metadata.Operator, err)
	}
	return &Metadata{
		stats:    stats,
		operator: op,
		part:     conf.Metadata.Part,

		mSkippedEmpty: stats.GetCounter("condition.text.skipped.empty_message"),
		mSkipped:      stats.GetCounter("condition.text.skipped"),
		mSkippedOOB:   stats.GetCounter("condition.text.skipped.out_of_bounds"),
		mApplied:      stats.GetCounter("condition.text.applied"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Metadata) Check(msg types.Message) bool {
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mSkippedEmpty.Incr(1)
		c.mSkipped.Incr(1)
		return false
	}

	c.mApplied.Incr(1)
	return c.operator(msg.Get(index).Metadata())
}

//------------------------------------------------------------------------------
