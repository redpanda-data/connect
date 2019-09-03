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

package output

import (
	"os"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		description: `
The file output type simply appends all messages to an output file. Single part
messages are printed with a delimiter (defaults to '\n' if left empty).
Multipart messages are written with each part delimited, with the final part
followed by two delimiters, e.g. a multipart message [ "foo", "bar", "baz" ]
would be written as:

foo\n
bar\n
baz\n\n`,
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration fields for the file based output type.
type FileConfig struct {
	Path  string `json:"path" yaml:"path"`
	Delim string `json:"delimiter" yaml:"delimiter"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:  "",
		Delim: "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File output type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	file, err := os.OpenFile(conf.File.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0666))
	if err != nil {
		return nil, err
	}
	return NewLineWriter(file, true, []byte(conf.File.Delim), "file", log, stats)
}

//------------------------------------------------------------------------------
