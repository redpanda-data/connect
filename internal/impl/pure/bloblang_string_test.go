package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

func TestParseUrlencoded(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "simple parsing",
			method: "parse_form_url_encoded",
			target: "username=example",
			args:   []any{},
			exp:    map[string]any{"username": "example"},
		},
		{
			name:   "parsing multiple values under the same key",
			method: "parse_form_url_encoded",
			target: "usernames=userA&usernames=userB",
			args:   []any{},
			exp:    map[string]any{"usernames": []any{"userA", "userB"}},
		},
		{
			name:   "decodes data correctly",
			method: "parse_form_url_encoded",
			target: "email=example%40email.com",
			args:   []any{},
			exp:    map[string]any{"email": "example@email.com"},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := value.IClone(test.target)
			argsClone := value.IClone(test.args).([]any)

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
