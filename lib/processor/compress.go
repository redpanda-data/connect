// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCompress] = TypeSpec{
		constructor: NewCompress,
		description: `
Compresses parts of a message according to the selected algorithm. Supported
compression types are: gzip, zlib, flate.

The 'level' field might not apply to all algorithms.`,
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
	zw, err := gzip.NewWriterLevel(buf, level)
	if err != nil {
		return nil, err
	}

	if _, err = zw.Write(b); err != nil {
		return nil, err
	}
	zw.Close()
	return buf.Bytes(), nil
}

func zlibCompress(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	zw, err := zlib.NewWriterLevel(buf, level)
	if err != nil {
		return nil, err
	}

	if _, err = zw.Write(b); err != nil {
		return nil, err
	}
	zw.Close()
	return buf.Bytes(), nil
}

func flateCompress(level int, b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	zw, err := flate.NewWriter(buf, level)
	if err != nil {
		return nil, err
	}

	if _, err = zw.Write(b); err != nil {
		return nil, err
	}
	zw.Close()
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
func (c *Compress) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int) {
		part := msg.Get(i).Get()
		newPart, err := c.comp(c.conf.Level, part)
		if err == nil {
			newMsg.Get(i).Set(newPart)
		} else {
			c.log.Errorf("Failed to compress message part: %v\n", err)
			c.mErr.Incr(1)
			FlagFail(newMsg.Get(i))
		}
	}

	if len(c.conf.Parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			proc(i)
		}
	} else {
		for _, i := range c.conf.Parts {
			proc(i)
		}
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
