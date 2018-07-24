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
	"fmt"
	"regexp"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/microcosm-cc/bluemonday"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["text"] = TypeSpec{
		constructor: NewText,
		description: `
Performs text based mutations on payloads.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operations

#### ` + "`append`" + `

Appends text to the end of the payload.

#### ` + "`prepend`" + `

Prepends text to the beginning of the payload.

#### ` + "`replace`" + `

Replaces all occurrences of the argument in a message with a value.

#### ` + "`replace_regexp`" + `

Replaces all occurrences of the argument regular expression in a message with a
value.

#### ` + "`strip_html`" + `

Removes all HTML tags from a message.

#### ` + "`trim_space`" + `

Removes all leading and trailing whitespace from the payload.

#### ` + "`trim`" + `

Removes all leading and trailing occurrences of characters within the arg field.`,
	}
}

//------------------------------------------------------------------------------

// TextConfig contains any configuration for the Text processor.
type TextConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Arg      string `json:"arg" yaml:"arg"`
	Value    string `json:"value" yaml:"value"`
}

// NewTextConfig returns a TextConfig with default values.
func NewTextConfig() TextConfig {
	return TextConfig{
		Parts:    []int{},
		Operator: "trim_space",
		Arg:      "",
		Value:    "",
	}
}

//------------------------------------------------------------------------------

type textOperator func(body []byte, value []byte) ([]byte, error)

func newTextAppendOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		if len(value) == 0 {
			return body, nil
		}
		return append(body[:], value...), nil
	}
}

func newTextPrependOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		if len(value) == 0 {
			return body, nil
		}
		return append(value[:], body...), nil
	}
}

func newTextTrimSpaceOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.TrimSpace(body), nil
	}
}

func newTextTrimOperator(arg string) textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.Trim(body, arg), nil
	}
}

func newTextReplaceOperator(arg string) textOperator {
	replaceArg := []byte(arg)
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.Replace(body, replaceArg, value, -1), nil
	}
}

func newTextReplaceRegexpOperator(arg string) (textOperator, error) {
	rp, err := regexp.Compile(arg)
	if err != nil {
		return nil, err
	}
	return func(body []byte, value []byte) ([]byte, error) {
		return rp.ReplaceAll(body, value), nil
	}, nil
}

func newTextStripHTMLOperator(arg string) textOperator {
	p := bluemonday.NewPolicy()
	return func(body []byte, value []byte) ([]byte, error) {
		return p.SanitizeBytes(body), nil
	}
}

func getTextOperator(opStr string, arg string) (textOperator, error) {
	switch opStr {
	case "append":
		return newTextAppendOperator(), nil
	case "prepend":
		return newTextPrependOperator(), nil
	case "trim_space":
		return newTextTrimSpaceOperator(), nil
	case "trim":
		return newTextTrimOperator(arg), nil
	case "replace":
		return newTextReplaceOperator(arg), nil
	case "replace_regexp":
		return newTextReplaceRegexpOperator(arg)
	case "strip_html":
		return newTextStripHTMLOperator(arg), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Text is a processor that performs an operation on a Text payload.
type Text struct {
	parts       []int
	interpolate bool
	valueBytes  []byte
	operator    textOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewText returns a Text processor.
func NewText(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	t := &Text{
		conf:  conf,
		log:   log.NewModule(".processor.text"),
		stats: stats,

		valueBytes: []byte(conf.Text.Value),

		mCount:     stats.GetCounter("processor.text.count"),
		mSucc:      stats.GetCounter("processor.text.success"),
		mErr:       stats.GetCounter("processor.text.error"),
		mSent:      stats.GetCounter("processor.text.sent"),
		mSentParts: stats.GetCounter("processor.text.parts.sent"),
	}

	t.interpolate = text.ContainsFunctionVariables(t.valueBytes)

	var err error
	if t.operator, err = getTextOperator(conf.Text.Operator, conf.Text.Arg); err != nil {
		return nil, err
	}
	return t, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (t *Text) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	t.mCount.Incr(1)

	newMsg := msg.ShallowCopy()

	valueBytes := t.valueBytes
	if t.interpolate {
		valueBytes = text.ReplaceFunctionVariables(valueBytes)
	}

	targetParts := t.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		data := newMsg.Get(index)
		var err error
		if data, err = t.operator(data, valueBytes); err != nil {
			t.mErr.Incr(1)
			t.log.Debugf("Failed to apply operator: %v\n", err)
			continue
		}

		newMsg.Set(index, data)
		t.mSucc.Incr(1)
	}

	msgs := [1]types.Message{newMsg}

	t.mSent.Incr(1)
	t.mSentParts.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
