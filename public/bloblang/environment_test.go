package bloblang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvironment(t *testing.T) {
	env1, env2 := NewEnvironment(), NewEnvironment()

	require.NoError(t, env1.RegisterMethod("foo", func(_ ...any) (Method, error) {
		return StringMethod(func(s string) (any, error) {
			return "foo:" + s, nil
		}), nil
	}))

	require.NoError(t, env2.RegisterFunction("bar", func(_ ...any) (Function, error) {
		return func() (any, error) {
			return "bar", nil
		}, nil
	}))

	_, err := env1.Parse(`root = bar()`)
	assert.EqualError(t, err, "unrecognised function 'bar'")

	exe, err := env1.Parse(`root = "bar".foo()`)
	require.NoError(t, err)

	v, err := exe.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, "foo:bar", v)

	_, err = env2.Parse(`root = "bar".foo()`)
	assert.EqualError(t, err, "unrecognised method 'foo'")

	exe, err = env2.Parse(`root = bar()`)
	require.NoError(t, err)

	v, err = exe.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)
}

func TestEnvironmentV2(t *testing.T) {
	env1, env2 := NewEnvironment(), NewEnvironment()

	require.NoError(t, env1.RegisterMethodV2("foo", NewPluginSpec(), func(_ *ParsedParams) (Method, error) {
		return StringMethod(func(s string) (any, error) {
			return "foo:" + s, nil
		}), nil
	}))

	require.NoError(t, env2.RegisterFunctionV2("bar", NewPluginSpec(), func(_ *ParsedParams) (Function, error) {
		return func() (any, error) {
			return "bar", nil
		}, nil
	}))

	_, err := env1.Parse(`root = bar()`)
	assert.EqualError(t, err, "unrecognised function 'bar'")

	exe, err := env1.Parse(`root = "bar".foo()`)
	require.NoError(t, err)

	v, err := exe.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, "foo:bar", v)

	_, err = env2.Parse(`root = "bar".foo()`)
	assert.EqualError(t, err, "unrecognised method 'foo'")

	exe, err = env2.Parse(`root = bar()`)
	require.NoError(t, err)

	v, err = exe.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)
}

func TestEmptyEnvironment(t *testing.T) {
	env := NewEmptyEnvironment()

	require.NoError(t, env.RegisterMethod("foo", func(_ ...any) (Method, error) {
		return StringMethod(func(s string) (any, error) {
			return "foo:" + s, nil
		}), nil
	}))

	_, err := env.Parse(`root = now()`)
	assert.EqualError(t, err, "unrecognised function 'now'")

	exe, err := env.Parse(`root = "hello world".foo()`)
	require.NoError(t, err)

	v, err := exe.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, "foo:hello world", v)
}

func TestEnvironmentDisabledImports(t *testing.T) {
	env := NewEmptyEnvironment().WithDisabledImports()

	_, err := env.Parse(`from "/tmp/foo.blobl"`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "imports are disabled in this context")
}
