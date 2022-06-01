package lang

import (
	"net/mail"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeMethod(t *testing.T) {
	target := map[string]interface{}{"foo": "email"}

	targetClone := query.IClone(target)
	argsClone := query.IClone([]interface{}{}).([]interface{})

	fn, err := query.InitMethodHelper("fake", query.NewLiteralFunction("", targetClone), argsClone...)
	require.NoError(t, err)

	res, err := fn.Exec(query.FunctionContext{
		Maps:     map[string]query.Function{},
		Index:    0,
		MsgBatch: nil,
	})
	require.NoError(t, err)

	resMap := res.(map[string]interface{})
	email := resMap["foo"]
	_, err = mail.ParseAddress(email.(string))
	assert.NoError(t, err)

	assert.Equal(t, target, targetClone)
	assert.Equal(t, []interface{}{}, argsClone)
}
