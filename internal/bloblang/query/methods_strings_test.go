package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseXML(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target interface{}
		args   []interface{}
		exp    interface{}
	}{
		{
			name:   "simple parsing",
			method: "parse_xml",
			target: "<root><title>This is a title</title><content>This is some content</content></root>",
			args:   []interface{}{},
			exp:    map[string]interface{}{"root": map[string]interface{}{"content": "This is some content", "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools without casting",
			method: "parse_xml",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			args:   []interface{}{},
			exp:    map[string]interface{}{"root": map[string]interface{}{"bool": "True", "number": map[string]interface{}{"#text": "123", "-id": "99"}, "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools with casting",
			method: "parse_xml",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			args:   []interface{}{true},
			exp:    map[string]interface{}{"root": map[string]interface{}{"bool": true, "number": map[string]interface{}{"#text": float64(123), "-id": float64(99)}, "title": "This is a title"}},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := IClone(test.target)
			argsClone := IClone(test.args).([]interface{})

			fn, err := InitMethodHelper(test.method, NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(FunctionContext{
				Maps:     map[string]Function{},
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
