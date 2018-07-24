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
	"archive/tar"
	"bytes"
	"fmt"
	"io"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["unarchive"] = TypeSpec{
		constructor: NewUnarchive,
		description: `
Unarchives parts of a message according to the selected archive type into
multiple parts. Supported archive types are: tar, binary, lines.

When a part is unarchived it is split into more message parts that replace the
original part. If you wish to split the archive into one message per file then
follow this with the 'split' processor.

Parts that are selected but fail to unarchive (invalid format) will be removed
from the message. If the message results in zero parts it is skipped entirely.`,
	}
}

//------------------------------------------------------------------------------

// UnarchiveConfig contains any configuration for the Unarchive processor.
type UnarchiveConfig struct {
	Format string `json:"format" yaml:"format"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewUnarchiveConfig returns a UnarchiveConfig with default values.
func NewUnarchiveConfig() UnarchiveConfig {
	return UnarchiveConfig{
		Format: "binary",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type unarchiveFunc func(bytes []byte) ([][]byte, error)

func tarUnarchive(b []byte) ([][]byte, error) {
	buf := bytes.NewBuffer(b)
	tr := tar.NewReader(buf)

	var newParts [][]byte

	// Iterate through the files in the archive.
	for {
		_, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			return nil, err
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(tr); err != nil {
			return nil, err
		}

		newParts = append(newParts, newPartBuf.Bytes())
	}

	return newParts, nil
}

func binaryUnarchive(b []byte) ([][]byte, error) {
	msg, err := types.FromBytes(b)
	if err != nil {
		return nil, err
	}

	return msg.GetAll(), nil
}

func linesUnarchive(b []byte) ([][]byte, error) {
	return bytes.Split(b, []byte("\n")), nil
}

func strToUnarchiver(str string) (unarchiveFunc, error) {
	switch str {
	case "tar":
		return tarUnarchive, nil
	case "binary":
		return binaryUnarchive, nil
	case "lines":
		return linesUnarchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Unarchive is a processor that can selectively unarchive parts of a message as a
// chosen archive type.
type Unarchive struct {
	conf      UnarchiveConfig
	unarchive unarchiveFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewUnarchive returns a Unarchive processor.
func NewUnarchive(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	dcor, err := strToUnarchiver(conf.Unarchive.Format)
	if err != nil {
		return nil, err
	}
	return &Unarchive{
		conf:      conf.Unarchive,
		unarchive: dcor,
		log:       log.NewModule(".processor.unarchive"),
		stats:     stats,

		mCount:     stats.GetCounter("processor.unarchive.count"),
		mSucc:      stats.GetCounter("processor.unarchive.success"),
		mErr:       stats.GetCounter("processor.unarchive.error"),
		mSkipped:   stats.GetCounter("processor.unarchive.skipped"),
		mDropped:   stats.GetCounter("processor.unarchive.dropped"),
		mSent:      stats.GetCounter("processor.unarchive.sent"),
		mSentParts: stats.GetCounter("processor.unarchive.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message, attempts to unarchive parts of the message,
// and returns the result.
func (d *Unarchive) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	newMsg := types.NewMessage(nil)
	lParts := msg.Len()

	noParts := len(d.conf.Parts) == 0
	for i, part := range msg.GetAll() {
		isTarget := noParts
		if !isTarget {
			nI := i - lParts
			for _, t := range d.conf.Parts {
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
		newParts, err := d.unarchive(part)
		if err == nil {
			d.mSucc.Incr(1)
			newMsg.Append(newParts...)
		} else {
			d.mErr.Incr(1)
		}
	}

	if newMsg.Len() == 0 {
		d.mSkipped.Incr(1)
		d.mDropped.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	d.mSent.Incr(1)
	d.mSentParts.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
