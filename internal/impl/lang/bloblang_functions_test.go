package lang

import (
	"net/mail"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeFunction(t *testing.T) {
	e, err := query.InitFunctionHelper("fake", "email")
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)

	_, err = mail.ParseAddress(res.(string))
	assert.NoError(t, err)
}
