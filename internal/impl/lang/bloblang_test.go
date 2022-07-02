package lang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func TestFakeFunction_Invalid(t *testing.T) {
	e, err := query.InitFunctionHelper("fake", "foo")
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.Error(t, err, "invalid faker function: foo")
	assert.Empty(t, res)
}

func TestFieldsFromNode(t *testing.T) {
	tests := []struct {
		name     string
		function string
	}{
		{
			name:     "default",
			function: "",
		},
		{
			name:     "email function",
			function: "email",
		},
		{
			name:     "phone number function",
			function: "phone_number",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := query.InitFunctionHelper("fake", test.function)
			require.Nil(t, err)

			res, err := e.Exec(query.FunctionContext{})
			require.NoError(t, err)

			assert.NotEmpty(t, res)
		})
	}
}
