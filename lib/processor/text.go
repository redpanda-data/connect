package processor

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/microcosm-cc/bluemonday"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeText] = TypeSpec{
		constructor: NewText,
		Status:      docs.StatusDeprecated,
		Footnotes: `
## Alternatives

All functionality of this processor has been superseded by the
[bloblang](/docs/components/processors/bloblang) processor.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "A text based [operation](#operators) to execute.").HasOptions(
				"append", "escape_url_query", "unescape_url_query",
				"find_regexp", "prepend", "quote", "regexp_expand", "replace",
				"replace_regexp", "set", "strip_html", "to_lower", "to_upper",
				"trim", "trim_space", "unquote",
			),
			docs.FieldCommon("arg", "An argument for the operator (not always applicable)."),
			docs.FieldCommon("value", "A value to use with the operator.").IsInterpolated(),
			PartsFieldSpec,
		},
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
		return bytes.ReplaceAll(body, replaceArg, value), nil
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

func getTextOperator(opStr, arg string) (textOperator, error) {
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
	parts    []int
	value    *field.Expression
	operator textOperator

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
	value, err := interop.NewBloblangField(mgr, conf.Text.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	t := &Text{
		parts: conf.Text.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		value: value,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

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

	proc := func(index int, span *tracing.Span, part types.Part) error {
		valueBytes := t.value.BytesLegacy(index, msg)
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

	IteratePartsWithSpanV2(TypeText, t.parts, newMsg, proc)

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
