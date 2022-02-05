package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigBloblang(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBloblangField("a")).
		Field(NewStringField("b"))

	parsedConfig, err := spec.ParseYAML(`
a: 'root = this.uppercase()'
b: 'root = this.filter('
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldBloblang("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldBloblang("c")
	require.Error(t, err)

	exec, err := parsedConfig.FieldBloblang("a")
	require.NoError(t, err)

	res, err := exec.Query("hello world")
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", res)
}
