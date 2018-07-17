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
	"bytes"
	"errors"
	"fmt"
	"regexp"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["text"] = TypeSpec{
		constructor: NewText,
		description: `
Text is a condition that checks the contents of a message part as plain text
against a logical operator and an argument.

Available logical operators are:

### ` + "`equals_cs`" + `

Checks whether the part equals the argument (case sensitive.)

### ` + "`equals`" + `

Checks whether the part equals the argument under unicode case-folding (case
insensitive.)

### ` + "`contains_cs`" + `

Checks whether the part contains the argument (case sensitive.)

### ` + "`contains`" + `

Checks whether the part contains the argument under unicode case-folding (case
insensitive.)

### ` + "`prefix_cs`" + `

Checks whether the part begins with the argument (case sensitive.)

### ` + "`prefix`" + `

Checks whether the part begins with the argument under unicode case-folding
(case insensitive.)

### ` + "`suffix_cs`" + `

Checks whether the part ends with the argument (case sensitive.)

### ` + "`suffix`" + `

Checks whether the part ends with the argument under unicode case-folding (case
insensitive.)

### ` + "`regexp_partial`" + `

Checks whether any section of the message part matches a regular expression (RE2
syntax).

### ` + "`regexp_exact`" + `

Checks whether the message part exactly matches a regular expression (RE2
syntax).`,
	}
}

//------------------------------------------------------------------------------

// Errors for the text condition.
var (
	ErrInvalidTextOperator = errors.New("invalid text operator type")
)

// TextConfig is a configuration struct containing fields for the text
// condition.
type TextConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Part     int    `json:"part" yaml:"part"`
	Arg      string `json:"arg" yaml:"arg"`
}

// NewTextConfig returns a TextConfig with default values.
func NewTextConfig() TextConfig {
	return TextConfig{
		Operator: "equals_cs",
		Part:     0,
		Arg:      "",
	}
}

//------------------------------------------------------------------------------

type textOperator func(c []byte) bool

func textEqualsOperator(arg []byte) textOperator {
	return func(c []byte) bool {
		return bytes.Equal(c, arg)
	}
}

func textEqualsFoldOperator(arg []byte) textOperator {
	return func(c []byte) bool {
		return bytes.EqualFold(c, arg)
	}
}

func textContainsOperator(arg []byte) textOperator {
	return func(c []byte) bool {
		return bytes.Contains(c, arg)
	}
}

func textContainsFoldOperator(arg []byte) textOperator {
	argLower := bytes.ToLower(arg)
	return func(c []byte) bool {
		return bytes.Contains(bytes.ToLower(c), argLower)
	}
}

func textPrefixOperator(arg []byte) textOperator {
	return func(c []byte) bool {
		return bytes.HasPrefix(c, arg)
	}
}

func textPrefixFoldOperator(arg []byte) textOperator {
	argLower := bytes.ToLower(arg)
	return func(c []byte) bool {
		return bytes.HasPrefix(bytes.ToLower(c), argLower)
	}
}

func textSuffixOperator(arg []byte) textOperator {
	return func(c []byte) bool {
		return bytes.HasSuffix(c, arg)
	}
}

func textSuffixFoldOperator(arg []byte) textOperator {
	argLower := bytes.ToLower(arg)
	return func(c []byte) bool {
		return bytes.HasSuffix(bytes.ToLower(c), argLower)
	}
}

func textRegexpPartialOperator(arg []byte) (textOperator, error) {
	compiled, err := regexp.Compile(string(arg))
	if err != nil {
		return nil, err
	}
	return func(c []byte) bool {
		return compiled.Match(c)
	}, nil
}

func textRegexpExactOperator(arg []byte) (textOperator, error) {
	compiled, err := regexp.Compile(string(arg))
	if err != nil {
		return nil, err
	}
	return func(c []byte) bool {
		return len(compiled.Find(c)) == len(c)
	}, nil
}

func strToTextOperator(str, arg string) (textOperator, error) {
	switch str {
	case "equals_cs":
		return textEqualsOperator([]byte(arg)), nil
	case "equals":
		return textEqualsFoldOperator([]byte(arg)), nil
	case "contains_cs":
		return textContainsOperator([]byte(arg)), nil
	case "contains":
		return textContainsFoldOperator([]byte(arg)), nil
	case "prefix_cs":
		return textPrefixOperator([]byte(arg)), nil
	case "prefix":
		return textPrefixFoldOperator([]byte(arg)), nil
	case "suffix_cs":
		return textSuffixOperator([]byte(arg)), nil
	case "suffix":
		return textSuffixFoldOperator([]byte(arg)), nil
	case "regexp_partial":
		return textRegexpPartialOperator([]byte(arg))
	case "regexp_exact":
		return textRegexpExactOperator([]byte(arg))
	}
	return nil, ErrInvalidTextOperator
}

//------------------------------------------------------------------------------

// Text is a condition that checks message text against logical operators.
type Text struct {
	stats    metrics.Type
	operator textOperator
	part     int

	mSkippedEmpty metrics.StatCounter
	mSkipped      metrics.StatCounter
	mSkippedOOB   metrics.StatCounter
	mApplied      metrics.StatCounter
}

// NewText returns a Text processor.
func NewText(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToTextOperator(conf.Text.Operator, conf.Text.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Text.Operator, err)
	}
	return &Text{
		stats:    stats,
		operator: op,
		part:     conf.Text.Part,

		mSkippedEmpty: stats.GetCounter("condition.text.skipped.empty_message"),
		mSkipped:      stats.GetCounter("condition.text.skipped"),
		mSkippedOOB:   stats.GetCounter("condition.text.skipped.out_of_bounds"),
		mApplied:      stats.GetCounter("condition.text.applied"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Text) Check(msg types.Message) bool {
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mSkippedEmpty.Incr(1)
		c.mSkipped.Incr(1)
		return false
	}

	msgPart := msg.Get(index)
	if msgPart == nil {
		c.mSkippedOOB.Incr(1)
		c.mSkipped.Incr(1)
		return false
	}

	c.mApplied.Incr(1)
	return c.operator(msgPart)
}

//------------------------------------------------------------------------------
