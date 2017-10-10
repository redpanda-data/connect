// Copyright (c) 2014 Ashley Jeffs
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

package input

import (
	"bufio"
	"os"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["file"] = typeSpec{
		constructor: NewFile,
		description: `
The 'file' type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.`,
	}
}

//------------------------------------------------------------------------------

// FileConfig is configuration values for the File input type.
type FileConfig struct {
	Path      string `json:"path" yaml:"path"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:      "",
		Multipart: false,
		MaxBuffer: bufio.MaxScanTokenSize,
	}
}

//------------------------------------------------------------------------------

// File is an input type that reads lines from a file, creating a message per
// line.
type File struct {
	running int32

	conf Config
	log  log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewFile creates a new File input type.
func NewFile(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	f := File{
		running:    1,
		conf:       conf,
		log:        log.NewModule(".input.file"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}
	return &f, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (f *File) loop() {
	defer func() {
		atomic.StoreInt32(&f.running, 0)

		close(f.messages)
		close(f.closedChan)
	}()

	file, err := os.Open(f.conf.File.Path)
	if err != nil {
		f.log.Errorf("Read %v error: %v\n", f.conf.File.Path, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if f.conf.File.MaxBuffer != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, f.conf.File.MaxBuffer)
	}

	f.log.Infof("Reading messages from: %v\n", f.conf.File.Path)

	var partsToSend, parts [][]byte

	for atomic.LoadInt32(&f.running) == 1 {
		if len(partsToSend) == 0 {
			if !scanner.Scan() {
				if err = scanner.Err(); err != nil {
					f.log.Errorf("File read error: %v\n", err)
				}
				return
			}
			data := make([]byte, len(scanner.Bytes()))
			copy(data, scanner.Bytes())
			if len(data) > 0 {
				if f.conf.File.Multipart {
					parts = append(parts, data)
				} else {
					partsToSend = append(partsToSend, data)
				}
			} else if f.conf.File.Multipart {
				partsToSend = parts
				parts = nil
			}
		}
		if len(partsToSend) > 0 {
			select {
			case f.messages <- types.Message{Parts: partsToSend}:
			case <-f.closeChan:
				return
			}
			res, open := <-f.responses
			if !open {
				return
			}
			if res.Error() == nil {
				partsToSend = nil
			}
		}
	}
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (f *File) StartListening(responses <-chan types.Response) error {
	if f.responses != nil {
		return types.ErrAlreadyStarted
	}
	f.responses = responses
	go f.loop()
	return nil
}

// MessageChan returns the messages channel.
func (f *File) MessageChan() <-chan types.Message {
	return f.messages
}

// CloseAsync shuts down the File input and stops processing requests.
func (f *File) CloseAsync() {
	if atomic.CompareAndSwapInt32(&f.running, 1, 0) {
		close(f.closeChan)
	}
}

// WaitForClose blocks until the File input has closed down.
func (f *File) WaitForClose(timeout time.Duration) error {
	select {
	case <-f.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
