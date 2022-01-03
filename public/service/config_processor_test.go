package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigProcessor(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewProcessorField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  bloblang: 'root = content().uppercase()'
`, nil)
	require.NoError(t, err)

	proc, err := parsedConfig.FieldProcessor("a")
	require.NoError(t, err)

	res, err := proc.Process(context.Background(), NewMessage([]byte("hello world")))
	require.NoError(t, err)
	require.Len(t, res, 1)

	resBytes, err := res[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(resBytes))

	// Batch processing should work the same
	resBatches, err := proc.ProcessBatch(context.Background(), MessageBatch{
		NewMessage([]byte("hello world")),
		NewMessage([]byte("hello world two")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 2)

	resBytes, err = resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(resBytes))

	resBytes, err = resBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD TWO", string(resBytes))

	require.NoError(t, proc.Close(context.Background()))
}

func TestConfigProcessorList(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewProcessorListField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  - bloblang: 'root = content().uppercase()'
  - bloblang: 'root = "foo: " + content()'
`, nil)
	require.NoError(t, err)

	procs, err := parsedConfig.FieldProcessorList("a")
	require.NoError(t, err)
	require.Len(t, procs, 2)

	res, err := procs[0].Process(context.Background(), NewMessage([]byte("hello world")))
	require.NoError(t, err)
	require.Len(t, res, 1)

	resBytes, err := res[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(resBytes))

	res, err = procs[1].Process(context.Background(), NewMessage([]byte("hello world")))
	require.NoError(t, err)
	require.Len(t, res, 1)

	resBytes, err = res[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo: hello world", string(resBytes))

	require.NoError(t, procs[0].Close(context.Background()))
	require.NoError(t, procs[1].Close(context.Background()))
}
