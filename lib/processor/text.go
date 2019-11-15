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
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/microcosm-cc/bluemonday"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeText] = TypeSpec{
		constructor: NewText,
		description: `
Performs text based mutations on payloads.

This processor will interpolate functions within the ` + "`value`" + ` field,
you can find a list of functions [here](../config_interpolation.md#functions).

Value interpolations are resolved once per message batch, in order to resolve it
for each message of the batch place it within a
` + "[`for_each`](#for_each)" + ` processor:

` + "``` yaml" + `
for_each:
- text:
    operator: set
    value: ${!json_field:document.content}
` + "```" + `

### Operators

#### ` + "`append`" + `

Appends text to the end of the payload.

#### ` + "`escape_url_query`" + `

Escapes text so that it is safe to place within the query section of a URL.

#### ` + "`unescape_url_query`" + `

Unescapes text that has been url escaped.

#### ` + "`find_regexp`" + `

Extract the matching section of the argument regular expression in a message.

#### ` + "`prepend`" + `

Prepends text to the beginning of the payload.

#### ` + "`quote`" + `

Returns a doubled-quoted string, using escape sequences (\t, \n, \xFF, \u0100)
for control characters and other non-printable characters.

#### ` + "`regexp_expand`" + `

Expands each matched occurrence of the argument regular expression according to
a template specified with the ` + "`value`" + ` field, and replaces the message
with the aggregated results.

Inside the template $ signs are interpreted as submatch expansions, e.g. $1
represents the text of the first submatch.

For example, given the following config:

` + "```yaml" + `
  - text:
      operator: regexp_expand
      arg: "(?m)(?P<key>\\w+):\\s+(?P<value>\\w+)$"
      value: "$key=$value\n"
` + "```" + `

And a message containing:

` + "```text" + `
option1: value1
# comment line
option2: value2
` + "```" + `

The resulting payload would be:

` + "```text" + `
option1=value1
option2=value2
` + "```" + `

#### ` + "`replace`" + `

Replaces all occurrences of the argument in a message with a value.

#### ` + "`replace_regexp`" + `

Replaces all occurrences of the argument regular expression in a message with a
value. Inside the value $ signs are interpreted as submatch expansions, e.g. $1
represents the text of the first submatch.

#### ` + "`set`" + `

Replace the contents of a message entirely with a value.

#### ` + "`strip_html`" + `

Removes all HTML tags from a message.

#### ` + "`to_lower`" + `

Converts all text into lower case.

#### ` + "`to_upper`" + `

Converts all text into upper case.

#### ` + "`trim`" + `

Removes all leading and trailing occurrences of characters within the arg field.

#### ` + "`trim_space`" + `

Removes all leading and trailing whitespace from the payload.

#### ` + "`unquote`" + `

Unquotes a single, double, or back-quoted string literal`,
	}
}

//------------------------------------------------------------------------------

// TextConfig contains configuration fields for the Text processor.
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
		return append(body[:len(body):len(body)], value...), nil
	}
}

func newTextEscapeURLQueryOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return []byte(url.QueryEscape(string(body))), nil
	}
}

func newTextUnescapeURLQueryOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		s, err := url.QueryUnescape(string(body))
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
}

func newTextPrependOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		if len(value) == 0 {
			return body, nil
		}
		return append(value[:len(value):len(value)], body...), nil
	}
}

func newTextQuoteOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return []byte(strconv.Quote(string(body))), nil
	}
}

func newTextTrimSpaceOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.TrimSpace(body), nil
	}
}

func newTextToUpperOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.ToUpper(body), nil
	}
}

func newTextToLowerOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.ToLower(body), nil
	}
}

func newTextTrimOperator(arg string) textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return bytes.Trim(body, arg), nil
	}
}

func newTextSetOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		return value, nil
	}
}

func newTextRegexpExpandOperator(arg string) (textOperator, error) {
	rp, err := regexp.Compile(arg)
	if err != nil {
		return nil, err
	}
	return func(body []byte, value []byte) ([]byte, error) {
		var result []byte
		for _, submatches := range rp.FindAllSubmatchIndex(body, -1) {
			result = rp.Expand(result, value, body, submatches)
		}
		return result, nil
	}, nil
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

func newTextFindRegexpOperator(arg string) (textOperator, error) {
	rp, err := regexp.Compile(arg)
	if err != nil {
		return nil, err
	}
	return func(body []byte, value []byte) ([]byte, error) {
		return rp.Find(body), nil
	}, nil
}

func newTextStripHTMLOperator(arg string) textOperator {
	p := bluemonday.NewPolicy()
	return func(body []byte, value []byte) ([]byte, error) {
		return p.SanitizeBytes(body), nil
	}
}

func newTextUnquoteOperator() textOperator {
	return func(body []byte, value []byte) ([]byte, error) {
		res, err := strconv.Unquote(string(body))
		if err != nil {
			return nil, err
		}
		return []byte(res), err
	}
}

func getTextOperator(opStr string, arg string) (textOperator, error) {
	switch opStr {
	case "append":
		return newTextAppendOperator(), nil
	case "escape_url_query":
		return newTextEscapeURLQueryOperator(), nil
	case "unescape_url_query":
		return newTextUnescapeURLQueryOperator(), nil
	case "find_regexp":
		return newTextFindRegexpOperator(arg)
	case "prepend":
		return newTextPrependOperator(), nil
	case "quote":
		return newTextQuoteOperator(), nil
	case "regexp_expand":
		return newTextRegexpExpandOperator(arg)
	case "replace":
		return newTextReplaceOperator(arg), nil
	case "replace_regexp":
		return newTextReplaceRegexpOperator(arg)
	case "set":
		return newTextSetOperator(), nil
	case "strip_html":
		return newTextStripHTMLOperator(arg), nil
	case "to_lower":
		return newTextToLowerOperator(), nil
	case "to_upper":
		return newTextToUpperOperator(), nil
	case "trim":
		return newTextTrimOperator(arg), nil
	case "trim_space":
		return newTextTrimSpaceOperator(), nil
	case "unquote":
		return newTextUnquoteOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Text is a processor that performs a text based operation on a payload.
type Text struct {
	parts       []int
	interpolate bool
	valueBytes  []byte
	operator    textOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewText returns a Text processor.
func NewText(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	t := &Text{
		parts: conf.Text.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		valueBytes: []byte(conf.Text.Value),

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	t.interpolate = text.ContainsFunctionVariables(t.valueBytes)

	var err error
	if t.operator, err = getTextOperator(conf.Text.Operator, conf.Text.Arg); err != nil {
		return nil, err
	}
	return t, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (t *Text) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	t.mCount.Incr(1)
	newMsg := msg.Copy()

	valueBytes := t.valueBytes
	if t.interpolate {
		valueBytes = text.ReplaceFunctionVariables(msg, valueBytes)
	}

	proc := func(index int, span opentracing.Span, part types.Part) error {
		data := part.Get()
		var err error
		if data, err = t.operator(data, valueBytes); err != nil {
			t.mErr.Incr(1)
			t.log.Debugf("Failed to apply operator: %v\n", err)
			return err
		}
		part.Set(data)
		return nil
	}

	IteratePartsWithSpan(TypeText, t.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	t.mBatchSent.Incr(1)
	t.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (t *Text) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (t *Text) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
