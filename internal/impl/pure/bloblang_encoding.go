package pure

import (
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zlib"

	"github.com/pierrec/lz4/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("compress",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryEncoding).
			Description(`Compresses a string or byte array value according to a specified algorithm.`).
			Param(bloblang.NewStringParam("algorithm").Description("One of `flate`, `gzip`, `lz4`, `snappy`, `zlib`, `zstd`.")).
			Param(bloblang.NewInt64Param("level").Description("The level of compression to use. May not be applicable to all algorithms.").Default(-1)).
			Example("", `let long_content = range(0, 1000).map_each(content()).join(" ")
root.a_len = $long_content.length()
root.b_len = $long_content.compress("gzip").length()
`,
				[2]string{
					`hello world this is some content`,
					`{"a_len":32999,"b_len":161}`,
				},
			).
			Example("", `root.compressed = content().compress("lz4").encode("base64")`,
				[2]string{
					`hello world I love space`,
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			level, err := args.GetInt64("level")
			if err != nil {
				return nil, err
			}
			algStr, err := args.GetString("algorithm")
			if err != nil {
				return nil, err
			}
			algFn, err := strToCompressor(algStr)
			if err != nil {
				return nil, err
			}
			return bloblang.BytesMethod(func(data []byte) (any, error) {
				return algFn(int(level), data)
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("decompress",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryEncoding).
			Description(`Decompresses a string or byte array value according to a specified algorithm. The result of decompression `).
			Param(bloblang.NewStringParam("algorithm").Description("One of `gzip`, `zlib`, `bzip2`, `flate`, `snappy`, `lz4`, `zstd`.")).
			Example("", `root = this.compressed.decode("base64").decompress("lz4")`,
				[2]string{
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
					`hello world I love space`,
				},
			).
			Example(
				"Use the `.string()` method in order to coerce the result into a string, this makes it possible to place the data within a JSON document without automatic base64 encoding.",
				`root.result = this.compressed.decode("base64").decompress("lz4").string()`,
				[2]string{
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
					`{"result":"hello world I love space"}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			algStr, err := args.GetString("algorithm")
			if err != nil {
				return nil, err
			}
			algFn, err := strToDecompressor(algStr)
			if err != nil {
				return nil, err
			}
			return bloblang.BytesMethod(func(data []byte) (any, error) {
				return algFn(data)
			}), nil
		}); err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// CompressFunc represents a compression algorithm and fully encapsulates it.
type CompressFunc func(level int, bytes []byte) ([]byte, error)

var compressImpls = map[string]CompressFunc{}

var compressImplsLock sync.Mutex

// AddCompressFunc adds a compression algorithm to components. The return struct
// serves no purpose other than allowing you to call it within the global
// context as an assignment.
func AddCompressFunc(name string, fn CompressFunc) struct{} {
	compressImplsLock.Lock()
	compressImpls[name] = fn
	compressImplsLock.Unlock()
	return struct{}{}
}

func strToCompressor(str string) (CompressFunc, error) {
	fn, exists := compressImpls[str]
	if !exists {
		return nil, fmt.Errorf("compression type not recognised: %v", str)
	}
	return fn, nil
}

var _ = AddCompressFunc("gzip", func(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := gzip.NewWriterLevel(buf, level)
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

var _ = AddCompressFunc("zlib", func(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := zlib.NewWriterLevel(buf, level)
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

var _ = AddCompressFunc("flate", func(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := flate.NewWriter(buf, level)
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

var _ = AddCompressFunc("snappy", func(level int, b []byte) ([]byte, error) {
	return snappy.Encode(nil, b), nil
})

var _ = AddCompressFunc("lz4", func(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := lz4.NewWriter(buf)
	if level > 0 {
		// The default compression level is 0 (lz4.Fast)
		if err := w.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(1 << (8 + level)))); err != nil {
			return nil, err
		}
	}

	if _, err := w.Write(b); err != nil {
		w.Close()
		return nil, err
	}
	// Must flush writer before calling buf.Bytes()
	w.Close()

	return buf.Bytes(), nil
})

//------------------------------------------------------------------------------

// DecompressFunc represents a decompression algorithm and fully encapsulates it.
type DecompressFunc func(bytes []byte) ([]byte, error)

var decompressImpls = map[string]DecompressFunc{}

var decompressImplsLock sync.Mutex

// AddDecompressFunc adds a decompression algorithm to components. The return
// struct serves no purpose other than allowing you to call it within the global
// context as an assignment.
func AddDecompressFunc(name string, fn DecompressFunc) struct{} {
	decompressImplsLock.Lock()
	decompressImpls[name] = fn
	decompressImplsLock.Unlock()
	return struct{}{}
}

func strToDecompressor(str string) (DecompressFunc, error) {
	fn, exists := decompressImpls[str]
	if !exists {
		return nil, fmt.Errorf("decompression type not recognised: %v", str)
	}
	return fn, nil
}

var _ = AddDecompressFunc("gzip", func(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(b))
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

var _ = AddDecompressFunc("snappy", func(b []byte) ([]byte, error) {
	return snappy.Decode(nil, b)
})

var _ = AddDecompressFunc("zlib", func(b []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewBuffer(b))
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

var _ = AddDecompressFunc("flate", func(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewBuffer(b))

	outBuf := bytes.Buffer{}
	if _, err := io.Copy(&outBuf, r); err != nil {
		r.Close()
		return nil, err
	}
	r.Close()
	return outBuf.Bytes(), nil
})

var _ = AddDecompressFunc("bzip2", func(b []byte) ([]byte, error) {
	r := bzip2.NewReader(bytes.NewBuffer(b))

	outBuf := bytes.Buffer{}
	if _, err := io.Copy(&outBuf, r); err != nil {
		return nil, err
	}
	return outBuf.Bytes(), nil
})

var _ = AddDecompressFunc("lz4", func(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	r := lz4.NewReader(buf)

	outBuf := bytes.Buffer{}
	if _, err := outBuf.ReadFrom(r); err != nil && err != io.EOF {
		return nil, err
	}

	return outBuf.Bytes(), nil
})
