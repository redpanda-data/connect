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
	"os"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["archive"] = TypeSpec{
		constructor: NewArchive,
		description: `
Archives all the parts of a message into a single part according to the selected
archive type. Supported archive types are: tar, binary, lines.

Some archive types (such as tar) treat each archive item (message part) as a
file with a path. Since message parts only contain raw data a unique path must
be generated for each part. This can be done by using function interpolations on
the 'path' field as described [here](../config_interpolation.md#functions). For
types that aren't file based (such as binary) the file field is ignored.`,
	}
}

//------------------------------------------------------------------------------

// ArchiveConfig contains any configuration for the Archive processor.
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

type archiveFunc func(hFunc headerFunc, parts [][]byte) ([]byte, error)

type headerFunc func(body []byte) os.FileInfo

func tarArchive(hFunc headerFunc, parts [][]byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)

	// Iterate through the parts of the message.
	for _, part := range parts {
		hdr, err := tar.FileInfoHeader(hFunc(part), "")
		if err != nil {
			return nil, err
		}
		if err = tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err = tw.Write(part); err != nil {
			return nil, err
		}
	}
	tw.Close()

	return buf.Bytes(), nil
}

func binaryArchive(hFunc headerFunc, parts [][]byte) ([]byte, error) {
	return types.NewMessage(parts).Bytes(), nil
}

func linesArchive(hFunc headerFunc, parts [][]byte) ([]byte, error) {
	return bytes.Join(parts, []byte("\n")), nil
}

func strToArchiver(str string) (archiveFunc, error) {
	switch str {
	case "tar":
		return tarArchive, nil
	case "binary":
		return binaryArchive, nil
	case "lines":
		return linesArchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Archive is a processor that can selectively archive parts of a message as a
// chosen archive type.
type Archive struct {
	conf    ArchiveConfig
	archive archiveFunc

	pathBytes       []byte
	interpolatePath bool

	mCount   metrics.StatCounter
	mSkipped metrics.StatCounter
	mErr     metrics.StatCounter
	mSucc    metrics.StatCounter
	mSent    metrics.StatCounter

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
		log:             log.NewModule(".processor.archive"),
		stats:           stats,

		mCount:   stats.GetCounter("processor.archive.count"),
		mSkipped: stats.GetCounter("processor.archive.skipped"),
		mErr:     stats.GetCounter("processor.archive.error"),
		mSucc:    stats.GetCounter("processor.archive.success"),
		mSent:    stats.GetCounter("processor.archive.sent"),
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

func (d *Archive) createHeader(body []byte) os.FileInfo {
	path := d.conf.Path
	if d.interpolatePath {
		path = string(text.ReplaceFunctionVariables(d.pathBytes))
	}
	return fakeInfo{
		name: path,
		size: int64(len(body)),
		mode: 0666,
	}
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message, attempts to archive the parts of the message,
// and returns the result as a single part message.
func (d *Archive) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	if msg.Len() == 0 {
		d.mSkipped.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	newPart, err := d.archive(d.createHeader, msg.GetAll())
	if err != nil {
		d.log.Errorf("Failed to create archive: %v\n", err)
		d.mErr.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	d.mSucc.Incr(1)
	d.mSent.Incr(1)
	msgs := [1]types.Message{types.NewMessage([][]byte{newPart})}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
