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

package writer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

// FilesConfig contains configuration fields for the files output type.
type FilesConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFilesConfig creates a new Config with default values.
func NewFilesConfig() FilesConfig {
	return FilesConfig{
		Path: "${!count:files}-${!timestamp_unix_nano}.txt",
	}
}

//------------------------------------------------------------------------------

// Files is a benthos writer.Type implementation that writes message parts each
// to their own file.
type Files struct {
	conf FilesConfig

	path *text.InterpolatedString

	log   log.Modular
	stats metrics.Type
}

// NewFiles creates a new file based writer.Type.
func NewFiles(
	conf FilesConfig,
	log log.Modular,
	stats metrics.Type,
) *Files {
	return &Files{
		conf:  conf,
		path:  text.NewInterpolatedString(conf.Path),
		log:   log,
		stats: stats,
	}
}

// Connect is a noop.
func (f *Files) Connect() error {
	f.log.Infoln("Writing message parts as files.")
	return nil
}

// Write attempts to write message contents to a directory as files.
func (f *Files) Write(msg types.Message) error {
	return msg.Iter(func(i int, p types.Part) error {
		path := f.path.Get(message.Lock(msg, i))

		err := os.MkdirAll(filepath.Dir(path), os.FileMode(0777))
		if err != nil {
			return err
		}

		return ioutil.WriteFile(path, p.Get(), os.FileMode(0666))
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (f *Files) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (f *Files) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
