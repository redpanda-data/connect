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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

// FilesConfig is configuration values for the input type.
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

// Files is a benthos writer.Type implementation that writes messages parts each
// to their own file.
type Files struct {
	conf FilesConfig

	pathBytes       []byte
	interpolatePath bool

	log   log.Modular
	stats metrics.Type
}

// NewFiles creates a new file based writer.Type.
func NewFiles(
	conf FilesConfig,
	log log.Modular,
	stats metrics.Type,
) *Files {
	pathBytes := []byte(conf.Path)
	interpolatePath := text.ContainsFunctionVariables(pathBytes)
	return &Files{
		conf:            conf,
		pathBytes:       pathBytes,
		interpolatePath: interpolatePath,
		log:             log.NewModule(".output.files"),
		stats:           stats,
	}
}

// Connect is a noop.
func (f *Files) Connect() error {
	f.log.Infoln("Writing message parts as files.")
	return nil
}

// Write attempts to write message contents to a directory as files.
func (f *Files) Write(msg types.Message) error {
	for _, part := range msg.GetAll() {
		path := f.conf.Path
		if f.interpolatePath {
			path = string(text.ReplaceFunctionVariables(f.pathBytes))
		}

		err := os.MkdirAll(filepath.Dir(path), os.FileMode(0777))
		if err != nil {
			return err
		}
		if err = ioutil.WriteFile(path, part, os.FileMode(0666)); err != nil {
			return err
		}
	}
	return nil
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
