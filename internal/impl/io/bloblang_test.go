package io_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

func TestEnvFunctionCaching(t *testing.T) {
	key := "BENTHOS_TEST_BLOBLANG_FUNCTION"
	require.NoError(t, os.Setenv(key, "foobar"))
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	eCached, err := query.InitFunctionHelper("env", key)
	require.NoError(t, err)

	eNotCached, err := query.InitFunctionHelper("env", key, true)
	require.NoError(t, err)

	res, err := eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "foobar", res)

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "foobar", res)

	require.NoError(t, os.Setenv(key, "barbaz"))

	res, err = eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "foobar", res)

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "barbaz", res)
}

func TestHostname(t *testing.T) {
	hostname, _ := os.Hostname()

	e, err := query.InitFunctionHelper("hostname")
	require.NoError(t, err)

	res, err := e.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, hostname, res)
}

func TestFileFunctionCaching(t *testing.T) {
	tmpDir := t.TempDir()
	fooFile := filepath.Join(tmpDir, "foo.txt")

	require.NoError(t, os.WriteFile(fooFile, []byte("hello world 123"), 0o644))

	eCached, err := query.InitFunctionHelper("file", fooFile)
	require.NoError(t, err)

	eNotCached, err := query.InitFunctionHelper("file", fooFile, true)
	require.NoError(t, err)

	res, err := eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	require.NoError(t, os.WriteFile(fooFile, []byte("hello world 456"), 0x644))

	res, err = eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 456", value.IToString(res))
}

func TestFileRelFunctionCaching(t *testing.T) {
	tmpDir := t.TempDir()
	fooFile := filepath.Join(tmpDir, "foo.txt")

	require.NoError(t, os.WriteFile(fooFile, []byte("hello world 123"), 0o644))

	eCached, err := query.InitFunctionHelper("file_rel", fooFile)
	require.NoError(t, err)

	eNotCached, err := query.InitFunctionHelper("file_rel", fooFile, true)
	require.NoError(t, err)

	res, err := eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	require.NoError(t, os.WriteFile(fooFile, []byte("hello world 456"), 0x644))

	res, err = eCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 123", value.IToString(res))

	res, err = eNotCached.Exec(query.FunctionContext{})
	require.NoError(t, err)
	assert.Equal(t, "hello world 456", value.IToString(res))
}
