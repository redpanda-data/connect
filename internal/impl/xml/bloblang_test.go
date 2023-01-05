package xml_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	_ "github.com/benthosdev/benthos/v4/internal/impl/xml"
)

func TestParseXML(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "simple parsing",
			method: "parse_xml",
			target: "<root><title>This is a title</title><content>This is some content</content></root>",
			args:   []any{},
			exp:    map[string]any{"root": map[string]any{"content": "This is some content", "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools without casting",
			method: "parse_xml",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			args:   []any{},
			exp:    map[string]any{"root": map[string]any{"bool": "True", "number": map[string]any{"#text": "123", "-id": "99"}, "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools with casting",
			method: "parse_xml",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			args:   []any{true},
			exp:    map[string]any{"root": map[string]any{"bool": true, "number": map[string]any{"#text": float64(123), "-id": float64(99)}, "title": "This is a title"}},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := query.IClone(test.target)
			argsClone := query.IClone(test.args).([]any)

			fn, err := query.InitMethodHelper(test.method, query.NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(query.FunctionContext{
				Maps:     map[string]query.Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}
