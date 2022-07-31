package main

import (
	"testing"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/encoding/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/config/schema"
	"github.com/benthosdev/benthos/v4/internal/cuegen"

	_ "embed"
)

//go:embed cue_test/test.cue
var inputCue string

//go:embed cue_test/expected.yml
var expectedYAML string

func TestCUEGenerate(t *testing.T) {
	source, err := cuegen.GenerateSchema(schema.New("", ""))
	require.NoError(t, err)

	ctx := cuecontext.New()
	schemaV := ctx.CompileBytes(source)
	require.NoError(t, schemaV.Validate())

	v := ctx.CompileString(inputCue, cue.Scope(schemaV))
	require.NoError(t, v.Validate())

	vBytes, err := yaml.Encode(v)
	require.NoError(t, err)
	assert.Equal(t, expectedYAML, string(vBytes))
}
