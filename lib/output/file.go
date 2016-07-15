/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["file"] = typeSpec{
		constructor: NewFile,
		description: "TODO",
	}
}

//--------------------------------------------------------------------------------------------------

// FileConfig - Configuration values for the file based output type.
type FileConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFileConfig - Create a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path: "",
	}
}

//--------------------------------------------------------------------------------------------------

// File - An output type that pushes messages to a single file.
type File struct {
	running int32

	conf Config
	log  log.Modular

	messages     <-chan types.Message
	responseChan chan types.Response

	file *os.File

	closedChan chan struct{}
}

// NewFile - Create a new File output type.
func NewFile(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	f := File{
		running:      1,
		conf:         conf,
		log:          log.NewModule(".output.file"),
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
	}

	file, err := os.OpenFile(f.conf.File.Path, os.O_RDWR|os.O_APPEND, os.FileMode(0666))
	if err != nil {
		return nil, err
	}

	f.file = file
	return &f, nil
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (f *File) loop() {
	defer func() {
		close(f.responseChan)
		close(f.closedChan)
		f.file.Close()
	}()

	f.log.Infof("Writing messages to: %v\n", f.conf.File.Path)

	for atomic.LoadInt32(&f.running) == 1 {
		msg, open := <-f.messages
		if !open {
			return
		}
		var err error
		if len(msg.Parts) == 1 {
			_, err = fmt.Fprintf(f.file, "%s\n", msg.Parts[0])
		} else {
			_, err = fmt.Fprintf(f.file, "%s\n\n", bytes.Join(msg.Parts, []byte("\n")))
		}
		f.responseChan <- types.NewSimpleResponse(err)
	}
}

// StartReceiving - Assigns a messages channel for the output to read.
func (f *File) StartReceiving(msgs <-chan types.Message) error {
	if f.messages != nil {
		return types.ErrAlreadyStarted
	}
	f.messages = msgs
	go f.loop()
	return nil
}

// ResponseChan - Returns the errors channel.
func (f *File) ResponseChan() <-chan types.Response {
	return f.responseChan
}

// CloseAsync - Shuts down the File output and stops processing messages.
func (f *File) CloseAsync() {
	atomic.StoreInt32(&f.running, 0)
}

// WaitForClose - Blocks until the File output has closed down.
func (f *File) WaitForClose(timeout time.Duration) error {
	select {
	case <-f.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
