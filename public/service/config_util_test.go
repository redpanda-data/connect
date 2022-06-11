package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
)

func TestConfigDeprecatedExraction(t *testing.T) {
	inputConf := input.NewConfig()
	inputConf.Type = "generate"
	inputConf.Generate.Interval = "5m"
	inputConf.Generate.Mapping = `root = "foobar"`

	spec := NewConfigSpec().
		Field(NewStringField("interval")).
		Field(NewStringField("mapping"))

	pConf, err := extractConfig(nil, spec, "generate", nil, inputConf)
	require.NoError(t, err)

	v, err := pConf.FieldString("interval")
	require.NoError(t, err)
	assert.Equal(t, "5m", v)

	v, err = pConf.FieldString("mapping")
	require.NoError(t, err)
	assert.Equal(t, `root = "foobar"`, v)
}
