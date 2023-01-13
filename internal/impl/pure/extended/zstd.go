package extended

import (
	"github.com/DataDog/zstd"

	"github.com/benthosdev/benthos/v4/internal/impl/pure"
)

var _ = pure.AddCompressFunc("zstd", func(level int, b []byte) ([]byte, error) {
	return zstd.CompressLevel(nil, b, level)
})

var _ = pure.AddDecompressFunc("zstd", func(b []byte) ([]byte, error) {
	return zstd.Decompress(nil, b)
})
