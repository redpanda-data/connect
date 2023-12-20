package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
)

func TestConfigDeprecatedExraction(t *testing.T) {
	oldConf := processor.NewConfig()
	oldConf.Type = "insert_part"
	oldConf.InsertPart.Index = 3

	spec := NewConfigSpec().
		Field(NewIntField("index"))

	pConf, err := extractConfig(nil, spec, "insert_part", nil, oldConf)
	require.NoError(t, err)

	v, err := pConf.FieldInt("index")
	require.NoError(t, err)
	assert.Equal(t, 3, v)
}
