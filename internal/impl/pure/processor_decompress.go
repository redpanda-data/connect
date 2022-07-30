package pure

import (
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newDecompress(conf.Decompress, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("decompress", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "decompress",
		Categories: []string{
			"Parsing",
		},
		Summary: `
Decompresses messages according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate, snappy, lz4.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", "The decompression algorithm to use.").HasOptions("gzip", "zlib", "bzip2", "flate", "snappy", "lz4"),
		).ChildDefaultAndTypesFromStruct(processor.NewDecompressConfig()),
	})
	if err != nil {
		panic(err)
	}
}

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

type decompressProc struct {
	decomp decompressFunc
	log    log.Modular
}

func newDecompress(conf processor.DecompressConfig, mgr bundle.NewManagement) (*decompressProc, error) {
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
	newBytes, err := d.decomp(msg.AsBytes())
	if err != nil {
		d.log.Errorf("Failed to decompress message part: %v\n", err)
		return nil, err
	}

	msg.SetBytes(newBytes)
	return []*message.Part{msg}, nil
}

func (d *decompressProc) Close(context.Context) error {
	return nil
}
