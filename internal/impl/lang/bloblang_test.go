package lang

import (
	"testing"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeFunction_Email(t *testing.T) {
	e, err := query.InitFunctionHelper("fake", "email")
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)

	assert.NotEmpty(t, res)
}

func TestFakeFunction_Default(t *testing.T) {
	e, err := query.InitFunctionHelper("fake", "")
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)

	assert.NotEmpty(t, res)
}
