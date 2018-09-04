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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/armon/go-radix"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetadata] = TypeSpec{
		constructor: NewMetadata,
		description: `
Metadata is a condition that checks metadata keys of a message part against an
operator from the following list:

### ` + "`enum`" + `

Checks whether the contents of a metadata key matches one of the defined enum
values.

` + "```yaml" + `
type: metadata
metadata:
	operator: enum
	part: 0
	key: foo
	arg:
		- bar
		- baz
		- qux
		- quux
` + "```" + `

### ` + "`equals`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case insensitive.

` + "```yaml" + `
type: metadata
metadata:
	operator: equals
	part: 0
	key: foo
	arg: bar
` + "```" + `

### ` + "`equals_cs`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case sensitive.

` + "```yaml" + `
type: metadata
metadata:
	operator: equals_cs
	part: 0
	key: foo
	arg: BAR
` + "```" + `

### ` + "`exists`" + `

Checks whether a metadata key exists.

` + "```yaml" + `
type: metadata
metadata:
	operator: exists
	part: 0
	key: foo
` + "```" + `

### ` + "`less_than`" + `

Checks whether the contents of a metadata key, parsed as a floating point
number, is less than an argument. Returns false if the metadata value cannot be
parsed into a number.

` + "```yaml" + `
type: metadata
metadata:
	operator: less_than
	part: 0
	key: foo
	arg: 3
` + "```" + `

### ` + "`greater_than`" + `

Checks whether the contents of a metadata key, parsed as a floating point
number, is greater than an argument. Returns false if the metadata value cannot
be parsed into a number.

` + "```yaml" + `
type: metadata
metadata:
	operator: greater_than
	part: 0
	key: foo
	arg: 3
` + "```" + `

### ` + "`regexp_partial`" + `

Checks whether any section of the contents of a metadata key matches a regular
expression (RE2 syntax).

` + "```yaml" + `
type: metadata
metadata:
	operator: regexp_partial
	part: 0
	key: foo
	arg: "1[a-z]2"
` + "```" + `

### ` + "`regexp_exact`" + `

Checks whether the contents of a metadata key exactly matches a regular expression 
(RE2 syntax).

` + "```yaml" + `
type: metadata
metadata:
	operator: regexp_partial
	part: 0
	key: foo
	arg: "1[a-z]2"
` + "```" + `
`,
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
	Operator string       `json:"operator" yaml:"operator"`
	Part     int          `json:"part" yaml:"part"`
	Key      string       `json:"key" yaml:"key"`
	Arg      rawJSONValue `json:"arg" yaml:"arg"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Operator: "equals_cs",
		Part:     0,
		Key:      "",
		Arg:      []byte(`""`),
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(md types.Metadata) bool

func metadataEnumOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var entries []string
	if err := json.Unmarshal(arg, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse argument as array of strings: %v", err)
	}
	tree := radix.New()
	for _, entry := range entries {
		tree.Insert(entry, struct{}{})
	}
	return func(md types.Metadata) bool {
		_, ok := tree.Get(md.Get(key))
		return ok
	}, nil
}

func metadataEqualsCSOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var argStr string
	if err := json.Unmarshal(arg, &argStr); err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	return func(md types.Metadata) bool {
		return md.Get(key) == argStr
	}, nil
}

func metadataEqualsOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var argStr string
	if err := json.Unmarshal(arg, &argStr); err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	return func(md types.Metadata) bool {
		return strings.ToLower(md.Get(key)) == strings.ToLower(argStr)
	}, nil
}

func metadataExistsOperator(key string) metadataOperator {
	return func(md types.Metadata) bool {
		return len(md.Get(key)) > 0
	}
}

func metadataGreaterThanOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var v float64
	if err := json.Unmarshal(arg, &v); err != nil {
		return nil, fmt.Errorf("failed to parse argument as float64: %v", err)
	}
	return func(md types.Metadata) bool {
		val, verr := strconv.ParseFloat(md.Get(key), 10)
		if verr != nil {
			return false
		}
		return val > v
	}, nil
}

func metadataHasPrefixOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var entries []string
	if err := json.Unmarshal(arg, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse argument as float64: %v", err)
	}
	tree := radix.New()
	for _, entry := range entries {
		tree.Insert(entry, struct{}{})
	}
	return func(md types.Metadata) bool {
		_, _, ok := tree.LongestPrefix(md.Get(key))
		return ok
	}, nil
}

func metadataLessThanOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var v float64
	if err := json.Unmarshal(arg, &v); err != nil {
		return nil, fmt.Errorf("failed to parse argument as float64: %v", err)
	}
	return func(md types.Metadata) bool {
		val, verr := strconv.ParseFloat(md.Get(key), 10)
		if verr != nil {
			return false
		}
		return val < v
	}, nil
}

func metadataRegexpPartialOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var argStr string
	if err := json.Unmarshal(arg, &argStr); err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	compiled, err := regexp.Compile(argStr)
	if err != nil {
		return nil, err
	}
	return func(md types.Metadata) bool {
		return compiled.MatchString(md.Get(key))
	}, nil
}

func metadataRegexpExactOperator(key string, arg rawJSONValue) (metadataOperator, error) {
	var argStr string
	if err := json.Unmarshal(arg, &argStr); err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	compiled, err := regexp.Compile(argStr)
	if err != nil {
		return nil, err
	}
	return func(md types.Metadata) bool {
		val := md.Get(key)
		return len(compiled.FindString(val)) == len(val)
	}, nil
}

func strToMetadataOperator(str, key string, arg rawJSONValue) (metadataOperator, error) {
	switch str {
	case "enum":
		return metadataEnumOperator(key, arg)
	case "equals":
		return metadataEqualsOperator(key, arg)
	case "equals_cs":
		return metadataEqualsCSOperator(key, arg)
	case "exists":
		return metadataExistsOperator(key), nil
	case "greater_than":
		return metadataGreaterThanOperator(key, arg)
	case "has_prefix":
		return metadataHasPrefixOperator(key, arg)
	case "less_than":
		return metadataLessThanOperator(key, arg)
	case "regexp_partial":
		return metadataRegexpPartialOperator(key, arg)
	case "regexp_exact":
		return metadataRegexpExactOperator(key, arg)
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
