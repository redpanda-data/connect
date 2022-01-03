package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigBatching(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBatchPolicyField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  count: 20
  period: 5s
  processors:
    - bloblang: 'root = content().uppercase()'
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldTLS("b")
	require.Error(t, err)

	bConf, err := parsedConfig.FieldBatchPolicy("a")
	require.NoError(t, err)

	assert.Equal(t, 20, bConf.Count)
	assert.Equal(t, "5s", bConf.Period)
	require.Len(t, bConf.procs, 1)
	assert.Equal(t, "bloblang", bConf.procs[0].Type)
	assert.Equal(t, "root = content().uppercase()", string(bConf.procs[0].Bloblang))
}
