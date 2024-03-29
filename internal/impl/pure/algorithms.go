package pure

import (
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4/v4"
)

type (
	CompressFunc     func(level int, b []byte) ([]byte, error)
	CompressWriter   func(level int, w io.Writer) (io.Writer, error)
	DecompressFunc   func(b []byte) ([]byte, error)
	DecompressReader func(r io.Reader) (io.Reader, error)
)

type KnownCompressionAlgorithm struct {
	CompressFunc     CompressFunc
	CompressWriter   CompressWriter
	DecompressFunc   DecompressFunc
	DecompressReader DecompressReader
}

var knownCompressionAlgorithms = map[string]KnownCompressionAlgorithm{}

var knownCompressionAlgorithmsLock sync.Mutex

func AddKnownCompressionAlgorithm(name string, a KnownCompressionAlgorithm) struct{} {
	if a.CompressFunc == nil && a.CompressWriter != nil {
		a.CompressFunc = func(level int, b []byte) ([]byte, error) {
			var buf bytes.Buffer
			wtr, err := a.CompressWriter(level, &buf)
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(wtr, bytes.NewReader(b))
			if c, ok := wtr.(io.Closer); ok {
				if cerr := c.Close(); cerr != nil {
					return nil, cerr
				}
			}
			return buf.Bytes(), err
		}
	}

	if a.DecompressFunc == nil && a.DecompressReader != nil {
		a.DecompressFunc = func(b []byte) ([]byte, error) {
			rdr, err := a.DecompressReader(bytes.NewReader(b))
			if err != nil {
				return nil, err
			}
			mBytes, err := io.ReadAll(rdr)
			if c, ok := rdr.(io.Closer); ok {
				if cerr := c.Close(); cerr != nil {
					return nil, cerr
				}
			}
			return mBytes, err
		}
	}

	knownCompressionAlgorithmsLock.Lock()
	knownCompressionAlgorithms[name] = a
	knownCompressionAlgorithmsLock.Unlock()
	return struct{}{}
}

func CompressionAlgsList() (v []string) {
	knownCompressionAlgorithmsLock.Lock()
	v = make([]string, 0, len(knownCompressionAlgorithms))
	for k, a := range knownCompressionAlgorithms {
		if a.CompressFunc != nil {
			v = append(v, k)
		}
	}
	knownCompressionAlgorithmsLock.Unlock()
	sort.Strings(v)
	return v
}

func DecompressionAlgsList() (v []string) {
	knownCompressionAlgorithmsLock.Lock()
	v = make([]string, 0, len(knownCompressionAlgorithms))
	for k, a := range knownCompressionAlgorithms {
		if a.DecompressFunc != nil {
			v = append(v, k)
		}
	}
	knownCompressionAlgorithmsLock.Unlock()
	sort.Strings(v)
	return v
}

func strToCompressAlg(str string) (KnownCompressionAlgorithm, error) {
	fn, exists := knownCompressionAlgorithms[str]
	if !exists {
		return KnownCompressionAlgorithm{}, fmt.Errorf("compression type not recognised: %v", str)
	}
	return fn, nil
}

func strToCompressFunc(str string) (CompressFunc, error) {
	alg, err := strToCompressAlg(str)
	if err != nil {
		return nil, err
	}
	if alg.CompressFunc == nil {
		return nil, fmt.Errorf("compression type not recognised: %v", str)
	}
	return alg.CompressFunc, nil
}

func strToDecompressFunc(str string) (DecompressFunc, error) {
	alg, err := strToCompressAlg(str)
	if err != nil {
		return nil, err
	}
	if alg.DecompressFunc == nil {
		return nil, fmt.Errorf("decompression type not recognised: %v", str)
	}
	return alg.DecompressFunc, nil
}

func strToDecompressReader(str string) (DecompressReader, error) {
	alg, err := strToCompressAlg(str)
	if err != nil {
		return nil, err
	}
	if alg.DecompressReader == nil {
		return nil, fmt.Errorf("decompression type not recognised: %v", str)
	}
	return alg.DecompressReader, nil
}

//------------------------------------------------------------------------------

// The Primary is written to and closed first. The Sink is closed second.
type CombinedWriteCloser struct {
	Primary, Sink io.Writer
}

func (c *CombinedWriteCloser) Write(b []byte) (int, error) {
	return c.Primary.Write(b)
}

func (c *CombinedWriteCloser) Close() error {
	if closer, ok := c.Primary.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if closer, ok := c.Sink.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

// The Primary is read from and closed second. The Source is closed first.
type CombinedReadCloser struct {
	Primary, Source io.Reader
}

func (c *CombinedReadCloser) Read(b []byte) (int, error) {
	return c.Primary.Read(b)
}

func (c *CombinedReadCloser) Close() error {
	if closer, ok := c.Source.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if closer, ok := c.Primary.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

var _ = AddKnownCompressionAlgorithm("gzip", KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw, err := gzip.NewWriterLevel(w, level)
		if err != nil {
			return nil, err
		}
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("pgzip", KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw, err := pgzip.NewWriterLevel(w, level)
		if err != nil {
			return nil, err
		}
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar, err := pgzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("zlib", KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw, err := zlib.NewWriterLevel(w, level)
		if err != nil {
			return nil, err
		}
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar, err := zlib.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("flate", KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw, err := flate.NewWriter(w, level)
		if err != nil {
			return nil, err
		}
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar := flate.NewReader(r)
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("bzip2", KnownCompressionAlgorithm{
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar := bzip2.NewReader(r)
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("lz4", KnownCompressionAlgorithm{
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw := lz4.NewWriter(w)
		if level > 0 {
			// The default compression level is 0 (lz4.Fast)
			if err := aw.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(1 << (8 + level)))); err != nil {
				return nil, err
			}
		}
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar := lz4.NewReader(r)
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})

var _ = AddKnownCompressionAlgorithm("snappy", KnownCompressionAlgorithm{
	CompressFunc: func(level int, b []byte) ([]byte, error) {
		return snappy.Encode(nil, b), nil
	},
	CompressWriter: func(level int, w io.Writer) (io.Writer, error) {
		aw := snappy.NewBufferedWriter(w)
		return &CombinedWriteCloser{Primary: aw, Sink: w}, nil
	},
	DecompressFunc: func(b []byte) ([]byte, error) {
		return snappy.Decode(nil, b)
	},
	DecompressReader: func(r io.Reader) (io.Reader, error) {
		ar := snappy.NewReader(r)
		return &CombinedReadCloser{Primary: ar, Source: r}, nil
	},
})
