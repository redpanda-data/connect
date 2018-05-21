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
	"compress/gzip"
	"fmt"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["compress"] = TypeSpec{
		constructor: NewCompress,
		description: `
Compresses parts of a message according to the selected algorithm. Supported
compression types are: gzip (I'll add more later). If the list of target parts
is empty the compression will be applied to all message parts.

The 'level' field might not apply to all algorithms.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// CompressConfig contains any configuration for the Compress processor.
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

func strToCompressor(str string) (compressFunc, error) {
	switch str {
	case "gzip":
		return gzipCompress, nil
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
		log:   log.NewModule(".processor.compress"),
		stats: stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message, attempts to compress parts of the message and
// returns the result.
func (c *Compress) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.stats.Incr("processor.compress.count", 1)

	newMsg := types.NewMessage(nil)
	lParts := msg.Len()

	noParts := len(c.conf.Parts) == 0
	for i, part := range msg.GetAll() {
		isTarget := noParts
		if !isTarget {
			nI := i - lParts
			for _, t := range c.conf.Parts {
				if t == nI || t == i {
					isTarget = true
					break
				}
			}
		}
		if !isTarget {
			newMsg.Append(part)
			continue
		}
		newPart, err := c.comp(c.conf.Level, part)
		if err == nil {
			c.stats.Incr("processor.compress.success", 1)
			newMsg.Append(newPart)
		} else {
			c.log.Debugf("Failed to compress message part: %v\n", err)
			c.stats.Incr("processor.compress.error", 1)
		}
	}

	if newMsg.Len() == 0 {
		c.stats.Incr("processor.compress.skipped", 1)
		return nil, types.NewSimpleResponse(nil)
	}

	c.stats.Incr("processor.compress.sent", 1)
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
