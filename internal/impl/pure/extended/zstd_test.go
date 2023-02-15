package extended

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestZstdCompressionDecompression(t *testing.T) {
	exec, err := bloblang.Parse(`root = this.compress(algorithm: "zstd")`)
	require.NoError(t, err)

	input := []byte("hello world this is a really long string")

	compressed, err := exec.Query(input)
	require.NoError(t, err)

	assert.NotEqual(t, input, compressed)
	assert.Greater(t, len(compressed.([]byte)), 1)

	exec, err = bloblang.Parse(`root = this.decompress(algorithm: "zstd")`)
	require.NoError(t, err)

	decompressed, err := exec.Query(compressed)
	require.NoError(t, err)

	assert.Equal(t, input, decompressed)
}
