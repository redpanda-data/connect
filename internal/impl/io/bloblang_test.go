package io_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func TestEnvFunction(t *testing.T) {
	key := "BENTHOS_TEST_BLOBLANG_FUNCTION"
	os.Setenv(key, "foobar")
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	e, err := query.InitFunctionHelper("env", key)
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "foobar", res)
}

func TestHostname(t *testing.T) {
	hostname, _ := os.Hostname()

	e, err := query.InitFunctionHelper("hostname")
	require.Nil(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, hostname, res)
}
