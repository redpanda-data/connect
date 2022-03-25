package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigOutput(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "foo.txt")

	spec := NewConfigSpec().
		Field(NewOutputField("a"))

	parsedConfig, err := spec.ParseYAML(fmt.Sprintf(`
a:
  file:
    path: %v
    codec: lines
`, testFile), nil)
	require.NoError(t, err)

	output, err := parsedConfig.FieldOutput("a")
	require.NoError(t, err)

	require.NoError(t, output.Write(context.Background(), NewMessage([]byte("first line"))))
	require.NoError(t, output.WriteBatch(context.Background(), MessageBatch{
		NewMessage([]byte("second line")),
		NewMessage([]byte("third line")),
	}))

	require.NoError(t, output.Close(context.Background()))

	resultBytes, err := os.ReadFile(testFile)
	require.NoError(t, err)
	assert.Equal(t, "first line\nsecond line\nthird line\n\n", string(resultBytes))
}

func TestConfigOutputList(t *testing.T) {
	tmpDir := t.TempDir()

	firstFile := filepath.Join(tmpDir, "foo.txt")
	secondFile := filepath.Join(tmpDir, "bar.txt")

	spec := NewConfigSpec().
		Field(NewOutputListField("a"))

	parsedConfig, err := spec.ParseYAML(fmt.Sprintf(`
a:
  - file:
      path: %v
      codec: lines
  - file:
      path: %v
      codec: lines
`, firstFile, secondFile), nil)
	require.NoError(t, err)

	outputs, err := parsedConfig.FieldOutputList("a")
	require.NoError(t, err)
	require.Len(t, outputs, 2)

	require.NoError(t, outputs[0].Write(context.Background(), NewMessage([]byte("first line"))))
	require.NoError(t, outputs[1].Write(context.Background(), NewMessage([]byte("second line"))))

	require.NoError(t, outputs[0].Close(context.Background()))
	require.NoError(t, outputs[1].Close(context.Background()))

	resultBytes, err := os.ReadFile(firstFile)
	require.NoError(t, err)
	assert.Equal(t, "first line\n", string(resultBytes))

	resultBytes, err = os.ReadFile(secondFile)
	require.NoError(t, err)
	assert.Equal(t, "second line\n", string(resultBytes))
}
