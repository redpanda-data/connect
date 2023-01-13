package extended

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/benthosdev/benthos/v4/internal/impl/pure"
)

var _ = pure.AddCompressFunc("zstd", func(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := zstd.NewWriter(buf)
	if err != nil {
		return nil, err
	}

	if _, err = w.Write(b); err != nil {
		w.Close()
		return nil, err
	}
	// Must flush writer before calling buf.Bytes()
	w.Close()
	return buf.Bytes(), nil
})

var _ = pure.AddDecompressFunc("zstd", func(b []byte) ([]byte, error) {
	r, err := zstd.NewReader(bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	outBuf := bytes.Buffer{}
	if _, err = io.Copy(&outBuf, r); err != nil {
		r.Close()
		return nil, err
	}
	r.Close()
	return outBuf.Bytes(), nil
})
