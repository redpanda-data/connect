package pure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestCompressionDecompression(t *testing.T) {
	seen := map[string]struct{}{}
	for _, alg := range []string{`flate`, `gzip`, `lz4`, `snappy`, `zlib`} {
		exec, err := bloblang.Parse(fmt.Sprintf(`root = this.compress(algorithm: "%v")`, alg))
		require.NoError(t, err)

		input := []byte("hello world this is a really long string")

		compressed, err := exec.Query(input)
		require.NoError(t, err)

		compressedBytes, ok := compressed.([]byte)
		require.True(t, ok)

		_, exists := seen[string(compressedBytes)]
		require.False(t, exists)
		seen[string(compressedBytes)] = struct{}{}

		assert.NotEqual(t, input, compressed)
		assert.Greater(t, len(compressedBytes), 1)

		exec, err = bloblang.Parse(fmt.Sprintf(`root = this.decompress(algorithm: "%v")`, alg))
		require.NoError(t, err)

		decompressed, err := exec.Query(compressed)
		require.NoError(t, err)

		assert.Equal(t, input, decompressed)
	}
}
