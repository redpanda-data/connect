package extended

import (
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/benthosdev/benthos/v4/internal/impl/pure"
)

var _ = pure.AddKnownCompressionAlgorithm("zstd", pure.KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
		if err != nil {
			return nil, err
		}
		return &pure.CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &pure.CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})
