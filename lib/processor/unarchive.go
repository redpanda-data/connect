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
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeUnarchive] = TypeSpec{
		constructor: NewUnarchive,
		description: `
Unarchives messages according to the selected archive format into multiple
messages within a batch. Supported archive formats are:
` + "`tar`, `zip`, `binary`, `lines`, `json_documents` and `json_array`." + `

When a message is unarchived the new messages replaces the original message in
the batch. Messages that are selected but fail to unarchive (invalid format)
will remain unchanged in the message batch but will be flagged as having failed.

The ` + "`json_documents`" + ` format attempts to parse the message as a stream
of concatenated JSON documents. Each parsed document is expanded into a new
message.

The ` + "`json_array`" + ` format attempts to parse the message as a JSON array
and for each element of the array expands its contents into a new message.

For the unarchive formats that contain file information (tar, zip), a metadata
field is added to each message called ` + "`archive_filename`" + ` with the
extracted filename.`,
	}
}

//------------------------------------------------------------------------------

// UnarchiveConfig contains configuration fields for the Unarchive processor.
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

type unarchiveFunc func(part types.Part) ([]types.Part, error)

func tarUnarchive(part types.Part) ([]types.Part, error) {
	buf := bytes.NewBuffer(part.Get())
	tr := tar.NewReader(buf)

	var newParts []types.Part

	// Iterate through the files in the archive.
	for {
		h, err := tr.Next()
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

		newPart := part.Copy()
		newPart.Set(newPartBuf.Bytes())
		newPart.Metadata().Set("archive_filename", h.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func zipUnarchive(part types.Part) ([]types.Part, error) {
	buf := bytes.NewReader(part.Get())
	zr, err := zip.NewReader(buf, int64(buf.Len()))
	if err != nil {
		return nil, err
	}

	var newParts []types.Part

	// Iterate through the files in the archive.
	for _, f := range zr.File {
		fr, err := f.Open()
		if err != nil {
			return nil, err
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(fr); err != nil {
			return nil, err
		}

		newPart := part.Copy()
		newPart.Set(newPartBuf.Bytes())
		newPart.Metadata().Set("archive_filename", f.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func binaryUnarchive(part types.Part) ([]types.Part, error) {
	msg, err := message.FromBytes(part.Get())
	if err != nil {
		return nil, err
	}
	parts := make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		newPart := part.Copy()
		newPart.Set(p.Get())
		parts[i] = newPart
		return nil
	})

	return parts, nil
}

func linesUnarchive(part types.Part) ([]types.Part, error) {
	lines := bytes.Split(part.Get(), []byte("\n"))
	parts := make([]types.Part, len(lines))
	for i, l := range lines {
		newPart := part.Copy()
		newPart.Set(l)
		parts[i] = newPart
	}
	return parts, nil
}

func jsonDocumentsUnarchive(part types.Part) ([]types.Part, error) {
	var parts []types.Part
	dec := json.NewDecoder(bytes.NewReader(part.Get()))
	for {
		var m interface{}
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		newPart := part.Copy()
		if err := newPart.SetJSON(m); err != nil {
			return nil, fmt.Errorf("failed to set JSON contents of message: %v", err)
		}
		parts = append(parts, newPart)
	}
	return parts, nil
}

func jsonArrayUnarchive(part types.Part) ([]types.Part, error) {
	jDoc, err := part.JSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse message into JSON array: %v", err)
	}

	jArray, ok := jDoc.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse message into JSON array: invalid type '%T'", jDoc)
	}

	parts := make([]types.Part, len(jArray))
	for i, ele := range jArray {
		newPart := part.Copy()
		if err = newPart.SetJSON(ele); err != nil {
			return nil, fmt.Errorf("failed to marshal element into new message: %v", err)
		}
		parts[i] = newPart
	}
	return parts, nil
}

func strToUnarchiver(str string) (unarchiveFunc, error) {
	switch str {
	case "tar":
		return tarUnarchive, nil
	case "zip":
		return zipUnarchive, nil
	case "binary":
		return binaryUnarchive, nil
	case "lines":
		return linesUnarchive, nil
	case "json_documents":
		return jsonDocumentsUnarchive, nil
	case "json_array":
		return jsonArrayUnarchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Unarchive is a processor that can selectively unarchive parts of a message
// following a chosen archive type.
type Unarchive struct {
	conf      UnarchiveConfig
	unarchive unarchiveFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
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
		log:       log,
		stats:     stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSkipped:   stats.GetCounter("skipped"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (d *Unarchive) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	newMsg := message.New(nil)
	lParts := msg.Len()

	noParts := len(d.conf.Parts) == 0
	msg.Iter(func(i int, part types.Part) error {
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
			newMsg.Append(msg.Get(i).Copy())
			return nil
		}

		span := tracing.CreateChildSpan(TypeUnarchive, part)
		defer span.Finish()

		newParts, err := d.unarchive(part)
		if err == nil {
			newMsg.Append(newParts...)
		} else {
			d.mErr.Incr(1)
			d.log.Errorf("Failed to unarchive message part: %v\n", err)
			newMsg.Append(part)
			FlagErr(newMsg.Get(-1), err)
			span.LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
		}
		return nil
	})

	d.mBatchSent.Incr(1)
	d.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (d *Unarchive) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (d *Unarchive) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
