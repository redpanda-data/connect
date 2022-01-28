package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigBackOff(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBackOffField("a", true))

	parsedConfig, err := spec.ParseYAML(`
a:
  max_interval: 300s
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldBackOff("b")
	require.Error(t, err)

	bConf, err := parsedConfig.FieldBackOff("a")
	require.NoError(t, err)

	assert.Equal(t, time.Millisecond*500, bConf.InitialInterval)
	assert.Equal(t, time.Second*300, bConf.MaxInterval)
	assert.Equal(t, time.Minute*15, bConf.MaxElapsedTime)
}
