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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["content"] = TypeSpec{
		constructor: NewContent,
		description: `
Content is a condition that checks the content of a message part against a
logical operator and an argument.

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
insensitive.)`,
	}
}

//------------------------------------------------------------------------------

// Errors for the content condition.
var (
	ErrInvalidContentOperator = errors.New("invalid content operator type")
)

// ContentConfig is a configuration struct containing fields for the content
// condition.
type ContentConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Part     int    `json:"part" yaml:"part"`
	Arg      string `json:"arg" yaml:"arg"`
}

// NewContentConfig returns a ContentConfig with default values.
func NewContentConfig() ContentConfig {
	return ContentConfig{
		Operator: "equals_cs",
		Part:     0,
		Arg:      "",
	}
}

//------------------------------------------------------------------------------

type contentOperator func(c []byte) bool

func contentEqualsOperator(arg []byte) contentOperator {
	return func(c []byte) bool {
		return bytes.Equal(c, arg)
	}
}

func contentEqualsFoldOperator(arg []byte) contentOperator {
	return func(c []byte) bool {
		return bytes.EqualFold(c, arg)
	}
}

func contentContainsOperator(arg []byte) contentOperator {
	return func(c []byte) bool {
		return bytes.Contains(c, arg)
	}
}

func contentContainsFoldOperator(arg []byte) contentOperator {
	argLower := bytes.ToLower(arg)
	return func(c []byte) bool {
		return bytes.Contains(bytes.ToLower(c), argLower)
	}
}

func strToContentOperator(str, arg string) (contentOperator, error) {
	switch str {
	case "equals_cs":
		return contentEqualsOperator([]byte(arg)), nil
	case "equals":
		return contentEqualsFoldOperator([]byte(arg)), nil
	case "contains_cs":
		return contentContainsOperator([]byte(arg)), nil
	case "contains":
		return contentContainsFoldOperator([]byte(arg)), nil
	}
	return nil, ErrInvalidContentOperator
}

//------------------------------------------------------------------------------

// Content is a condition that checks message content against logical operators.
type Content struct {
	stats    metrics.Type
	operator contentOperator
	part     int
}

// NewContent returns a Content processor.
func NewContent(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToContentOperator(conf.Content.Operator, conf.Content.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Content.Operator, err)
	}
	return &Content{
		stats:    stats,
		operator: op,
		part:     conf.Content.Part,
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Content) Check(msg types.Message) bool {
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.stats.Incr("condition.content.skipped.empty_message", 1)
		c.stats.Incr("condition.content.skipped", 1)
		return false
	}

	msgPart := msg.Get(index)
	if msgPart == nil {
		c.stats.Incr("condition.content.skipped.out_of_bounds", 1)
		c.stats.Incr("condition.content.skipped", 1)
		return false
	}

	c.stats.Incr("condition.content.applied", 1)
	return c.operator(msgPart)
}

//------------------------------------------------------------------------------
