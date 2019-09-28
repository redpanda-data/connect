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

package reader

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// FilesConfig contains configuration for the Files input type.
type FilesConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFilesConfig creates a new FilesConfig with default values.
func NewFilesConfig() FilesConfig {
	return FilesConfig{
		Path: "",
	}
}

//------------------------------------------------------------------------------

// Files is an input type that reads file contents at a path as messages.
type Files struct {
	targets []string
}

// NewFiles creates a new Files input type.
func NewFiles(conf FilesConfig) (*Files, error) {
	f := Files{}

	if info, err := os.Stat(conf.Path); err != nil {
		return nil, err
	} else if !info.IsDir() {
		f.targets = append(f.targets, conf.Path)
		return &f, nil
	}

	err := filepath.Walk(conf.Path, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() {
			return nil
		}
		f.targets = append(f.targets, path)
		return nil
	})

	return &f, err
}

//------------------------------------------------------------------------------

// Connect establishes a connection.
func (f *Files) Connect() (err error) {
	return nil
}

// ConnectWithContext establishes a connection.
func (f *Files) ConnectWithContext(ctx context.Context) (err error) {
	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new Files message.
func (f *Files) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, err := f.Read()
	if err != nil {
		return nil, nil, err
	}
	return msg, noopAsyncAckFn, nil
}

// Read a new Files message.
func (f *Files) Read() (types.Message, error) {
	if len(f.targets) == 0 {
		return nil, types.ErrTypeClosed
	}

	path := f.targets[0]
	f.targets = f.targets[1:]

	file, openerr := os.Open(path)
	if openerr != nil {
		return nil, fmt.Errorf("failed to read file '%v': %v", path, openerr)
	}
	defer file.Close()

	msgBytes, readerr := ioutil.ReadAll(file)
	if readerr != nil {
		return nil, readerr
	}

	msg := message.New([][]byte{msgBytes})
	msg.Get(0).Metadata().Set("path", path)
	return msg, nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (f *Files) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the Files input and stops processing requests.
func (f *Files) CloseAsync() {
}

// WaitForClose blocks until the Files input has closed down.
func (f *Files) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
