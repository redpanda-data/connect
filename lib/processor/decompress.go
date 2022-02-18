package processor

import (
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"fmt"
	"io"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDecompress] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newDecompress(conf.Decompress, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2ToV1Processor("decompress", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Decompresses messages according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate, snappy, lz4.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("algorithm", "The decompression algorithm to use.").HasOptions("gzip", "zlib", "bzip2", "flate", "snappy", "lz4"),
		},
	}
}

//------------------------------------------------------------------------------

// DecompressConfig contains configuration fields for the Decompress processor.
type DecompressConfig struct {
	Algorithm string `json:"algorithm" yaml:"algorithm"`
}

// NewDecompressConfig returns a DecompressConfig with default values.
func NewDecompressConfig() DecompressConfig {
	return DecompressConfig{
		Algorithm: "gzip",
	}
}

//------------------------------------------------------------------------------

type decompressFunc func(bytes []byte) ([]byte, error)

func gzipDecompress(b []byte) ([]byte, error) {
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
}

func snappyDecompress(b []byte) ([]byte, error) {
	return snappy.Decode(nil, b)
}

func zlibDecompress(b []byte) ([]byte, error) {
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
}

func flateDecompress(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewBuffer(b))

	outBuf := bytes.Buffer{}
	if _, err := io.Copy(&outBuf, r); err != nil {
		r.Close()
		return nil, err
	}
	r.Close()
	return outBuf.Bytes(), nil
}

func bzip2Decompress(b []byte) ([]byte, error) {
	r := bzip2.NewReader(bytes.NewBuffer(b))

	outBuf := bytes.Buffer{}
	if _, err := io.Copy(&outBuf, r); err != nil {
		return nil, err
	}
	return outBuf.Bytes(), nil
}

func lz4Decompress(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	r := lz4.NewReader(buf)

	outBuf := bytes.Buffer{}
	if _, err := outBuf.ReadFrom(r); err != nil && err != io.EOF {
		return nil, err
	}

	return outBuf.Bytes(), nil
}

func strToDecompressor(str string) (decompressFunc, error) {
	switch str {
	case "gzip":
		return gzipDecompress, nil
	case "zlib":
		return zlibDecompress, nil
	case "flate":
		return flateDecompress, nil
	case "bzip2":
		return bzip2Decompress, nil
	case "snappy":
		return snappyDecompress, nil
	case "lz4":
		return lz4Decompress, nil
	}
	return nil, fmt.Errorf("decompression type not recognised: %v", str)
}

//------------------------------------------------------------------------------

type decompressProc struct {
	decomp decompressFunc
	log    log.Modular
}

func newDecompress(conf DecompressConfig, mgr interop.Manager) (*decompressProc, error) {
	dcor, err := strToDecompressor(conf.Algorithm)
	if err != nil {
		return nil, err
	}
	return &decompressProc{
		decomp: dcor,
		log:    mgr.Logger(),
	}, nil
}

func (d *decompressProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newBytes, err := d.decomp(msg.Get())
	if err != nil {
		d.log.Errorf("Failed to decompress message part: %v\n", err)
		return nil, err
	}

	newMsg := msg.Copy()
	newMsg.Set(newBytes)
	return []*message.Part{newMsg}, nil
}

func (d *decompressProc) Close(context.Context) error {
	return nil
}
