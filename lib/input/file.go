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

	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["file"] = typeSpec{
		constructor: NewFile,
		description: `
The file type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.`,
	}
}

//------------------------------------------------------------------------------

// FileConfig is configuration values for the File input type.
type FileConfig struct {
	Path        string `json:"path" yaml:"path"`
	Multipart   bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer   int    `json:"max_buffer" yaml:"max_buffer"`
	CustomDelim string `json:"custom_delimiter" yaml:"custom_delimiter"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:        "",
		Multipart:   false,
		MaxBuffer:   bufio.MaxScanTokenSize,
		CustomDelim: "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File input type.
func NewFile(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	file, err := os.Open(conf.File.Path)
	if err != nil {
		return nil, err
	}
	delim := []byte("\n")
	if len(conf.File.CustomDelim) > 0 {
		delim = []byte(conf.File.CustomDelim)
	}
	return newReader(
		file,
		conf.File.MaxBuffer,
		conf.File.Multipart,
		delim,
		log, stats,
	)
}

//------------------------------------------------------------------------------
