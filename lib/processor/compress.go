package processor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCompress] = TypeSpec{
		constructor: NewCompress,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Compresses messages according to the selected algorithm. Supported compression
algorithms are: gzip, zlib, flate, snappy, lz4.`,
		Description: `
The 'level' field might not apply to all algorithms.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("algorithm", "The compression algorithm to use.").HasOptions("gzip", "zlib", "flate", "snappy", "lz4"),
			docs.FieldCommon("level", "The level of compression to use. May not be applicable to all algorithms."),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// CompressConfig contains configuration fields for the Compress processor.
type CompressConfig struct {
	Algorithm string `json:"algorithm" yaml:"algorithm"`
	Level     int    `json:"level" yaml:"level"`
	Parts     []int  `json:"parts" yaml:"parts"`
}

// NewCompressConfig returns a CompressConfig with default values.
func NewCompressConfig() CompressConfig {
	return CompressConfig{
		Algorithm: "gzip",
		Level:     gzip.DefaultCompression,
		Parts:     []int{},
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

//------------------------------------------------------------------------------

// Compress is a processor that can selectively compress parts of a message as a
// chosen compression algorithm.
type Compress struct {
	conf CompressConfig
	comp compressFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewCompress returns a Compress processor.
func NewCompress(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cor, err := strToCompressor(conf.Compress.Algorithm)
	if err != nil {
		return nil, err
	}
	return &Compress{
		conf:  conf.Compress,
		comp:  cor,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Compress) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int, span *tracing.Span, part *message.Part) error {
		newBytes, err := c.comp(c.conf.Level, part.Get())
		if err == nil {
			part.Set(newBytes)
		} else {
			c.log.Errorf("Failed to compress message part: %v\n", err)
			c.mErr.Incr(1)
			return err
		}
		return nil
	}

	if newMsg.Len() == 0 {
		return nil, nil
	}

	IteratePartsWithSpanV2(TypeCompress, c.conf.Parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]*message.Batch{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Compress) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Compress) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
