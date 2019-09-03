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
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeArchive] = TypeSpec{
		constructor: NewArchive,
		description: `
Archives all the messages of a batch into a single message according to the
selected archive format. Supported archive formats are:
` + "`tar`, `zip`, `binary`, `lines` and `json_array`." + `

Some archive formats (such as tar, zip) treat each archive item (message part)
as a file with a path. Since message parts only contain raw data a unique path
must be generated for each part. This can be done by using function
interpolations on the 'path' field as described
[here](../config_interpolation.md#functions). For types that aren't file based
(such as binary) the file field is ignored.

The ` + "`json_array`" + ` format attempts to JSON parse each message and append
the result to an array, which becomes the contents of the resulting message.

The resulting archived message adopts the metadata of the _first_ message part
of the batch.`,
	}
}

//------------------------------------------------------------------------------

// ArchiveConfig contains configuration fields for the Archive processor.
type ArchiveConfig struct {
	Format string `json:"format" yaml:"format"`
	Path   string `json:"path" yaml:"path"`
}

// NewArchiveConfig returns a ArchiveConfig with default values.
func NewArchiveConfig() ArchiveConfig {
	return ArchiveConfig{
		Format: "binary",
		Path:   "${!count:files}-${!timestamp_unix_nano}.txt",
	}
}

//------------------------------------------------------------------------------

type archiveFunc func(hFunc headerFunc, msg types.Message) (types.Part, error)

type headerFunc func(index int, body types.Part) os.FileInfo

func tarArchive(hFunc headerFunc, msg types.Message) (types.Part, error) {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)

	// Iterate through the parts of the message.
	err := msg.Iter(func(i int, part types.Part) error {
		hdr, err := tar.FileInfoHeader(hFunc(i, part), "")
		if err != nil {
			return err
		}
		if err = tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err = tw.Write(part.Get()); err != nil {
			return err
		}
		return nil
	})
	tw.Close()

	if err != nil {
		return nil, err
	}
	newPart := msg.Get(0).Copy()
	newPart.Set(buf.Bytes())
	return newPart, nil
}

func zipArchive(hFunc headerFunc, msg types.Message) (types.Part, error) {
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)

	// Iterate through the parts of the message.
	err := msg.Iter(func(i int, part types.Part) error {
		h, err := zip.FileInfoHeader(hFunc(i, part))
		if err != nil {
			return err
		}
		h.Method = zip.Deflate

		w, err := zw.CreateHeader(h)
		if err != nil {
			return err
		}
		if _, err = w.Write(part.Get()); err != nil {
			return err
		}
		return nil
	})
	zw.Close()

	if err != nil {
		return nil, err
	}
	newPart := msg.Get(0).Copy()
	newPart.Set(buf.Bytes())
	return newPart, nil
}

func binaryArchive(hFunc headerFunc, msg types.Message) (types.Part, error) {
	newPart := msg.Get(0).Copy()
	newPart.Set(message.ToBytes(msg))
	return newPart, nil
}

func linesArchive(hFunc headerFunc, msg types.Message) (types.Part, error) {
	tmpParts := make([][]byte, msg.Len())
	msg.Iter(func(i int, part types.Part) error {
		tmpParts[i] = part.Get()
		return nil
	})
	newPart := msg.Get(0).Copy()
	newPart.Set(bytes.Join(tmpParts, []byte("\n")))
	return newPart, nil
}

func jsonArrayArchive(hFunc headerFunc, msg types.Message) (types.Part, error) {
	var array []interface{}

	// Iterate through the parts of the message.
	err := msg.Iter(func(i int, part types.Part) error {
		doc, jerr := part.JSON()
		if jerr != nil {
			return fmt.Errorf("failed to parse message as JSON: %v", jerr)
		}
		array = append(array, doc)
		return nil
	})
	if err != nil {
		return nil, err
	}

	newPart := msg.Get(0).Copy()
	if err = newPart.SetJSON(array); err != nil {
		return nil, fmt.Errorf("failed to marshal archived array into a JSON document: %v", err)
	}
	return newPart, nil
}

func strToArchiver(str string) (archiveFunc, error) {
	switch str {
	case "tar":
		return tarArchive, nil
	case "zip":
		return zipArchive, nil
	case "binary":
		return binaryArchive, nil
	case "lines":
		return linesArchive, nil
	case "json_array":
		return jsonArrayArchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Archive is a processor that can selectively archive parts of a message into a
// single part using a chosen archive type.
type Archive struct {
	conf    ArchiveConfig
	archive archiveFunc

	pathBytes       []byte
	interpolatePath bool

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter

	log   log.Modular
	stats metrics.Type
}

// NewArchive returns a Archive processor.
func NewArchive(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	pathBytes := []byte(conf.Archive.Path)
	interpolatePath := text.ContainsFunctionVariables(pathBytes)

	archiver, err := strToArchiver(conf.Archive.Format)
	if err != nil {
		return nil, err
	}

	return &Archive{
		conf:            conf.Archive,
		pathBytes:       pathBytes,
		interpolatePath: interpolatePath,
		archive:         archiver,
		log:             log,
		stats:           stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSucc:      stats.GetCounter("success"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

type fakeInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (f fakeInfo) Name() string {
	return f.name
}
func (f fakeInfo) Size() int64 {
	return f.size
}
func (f fakeInfo) Mode() os.FileMode {
	return f.mode
}
func (f fakeInfo) ModTime() time.Time {
	return time.Now()
}
func (f fakeInfo) IsDir() bool {
	return false
}
func (f fakeInfo) Sys() interface{} {
	return nil
}

func (d *Archive) createHeaderFunc(msg types.Message) func(int, types.Part) os.FileInfo {
	return func(index int, body types.Part) os.FileInfo {
		path := d.conf.Path
		if d.interpolatePath {
			path = string(text.ReplaceFunctionVariables(message.Lock(msg, index), d.pathBytes))
		}
		return fakeInfo{
			name: path,
			size: int64(len(body.Get())),
			mode: 0666,
		}
	}
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (d *Archive) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	if msg.Len() == 0 {
		return nil, response.NewAck()
	}

	d.mSent.Incr(1)
	d.mBatchSent.Incr(1)

	newMsg := msg.Copy()

	spans := tracing.CreateChildSpans(TypeArchive, newMsg)
	newPart, err := d.archive(d.createHeaderFunc(msg), msg)
	if err != nil {
		newMsg.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			spans[i].LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
			return nil
		})
		d.log.Errorf("Failed to create archive: %v\n", err)
		d.mErr.Incr(1)
	} else {
		d.mSucc.Incr(1)
		newMsg.SetAll([]types.Part{newPart})
	}
	for _, s := range spans {
		s.Finish()
	}

	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (d *Archive) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (d *Archive) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
