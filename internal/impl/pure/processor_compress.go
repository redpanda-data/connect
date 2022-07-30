package pure

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"fmt"

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
		p, err := newCompress(conf.Compress, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("compress", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "compress",
		Categories: []string{
			"Parsing",
		},
		Summary: `
Compresses messages according to the selected algorithm. Supported compression
algorithms are: gzip, zlib, flate, snappy, lz4.`,
		Description: `
The 'level' field might not apply to all algorithms.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", "The compression algorithm to use.").HasOptions("gzip", "zlib", "flate", "snappy", "lz4"),
			docs.FieldInt("level", "The level of compression to use. May not be applicable to all algorithms."),
		).ChildDefaultAndTypesFromStruct(processor.NewCompressConfig()),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type compressFunc func(level int, bytes []byte) ([]byte, error)

func gzipCompress(level int, b []byte) ([]byte, error) {
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
}

func zlibCompress(level int, b []byte) ([]byte, error) {
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
}

func flateCompress(level int, b []byte) ([]byte, error) {
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
}

func snappyCompress(level int, b []byte) ([]byte, error) {
	return snappy.Encode(nil, b), nil
}

func lz4Compress(level int, b []byte) ([]byte, error) {
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
}

func strToCompressor(str string) (compressFunc, error) {
	switch str {
	case "gzip":
		return gzipCompress, nil
	case "zlib":
		return zlibCompress, nil
	case "flate":
		return flateCompress, nil
	case "snappy":
		return snappyCompress, nil
	case "lz4":
		return lz4Compress, nil
	}
	return nil, fmt.Errorf("compression type not recognised: %v", str)
}

type compressProc struct {
	level int
	comp  compressFunc
	log   log.Modular
}

func newCompress(conf processor.CompressConfig, mgr bundle.NewManagement) (*compressProc, error) {
	cor, err := strToCompressor(conf.Algorithm)
	if err != nil {
		return nil, err
	}
	return &compressProc{
		level: conf.Level,
		comp:  cor,
		log:   mgr.Logger(),
	}, nil
}

func (c *compressProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newBytes, err := c.comp(c.level, msg.AsBytes())
	if err != nil {
		c.log.Errorf("Failed to compress message: %v\n", err)
		return nil, err
	}
	msg.SetBytes(newBytes)
	return []*message.Part{msg}, nil
}

func (c *compressProc) Close(context.Context) error {
	return nil
}
