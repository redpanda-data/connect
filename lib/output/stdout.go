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
	Constructors[TypeSTDOUT] = TypeSpec{
		constructor: NewSTDOUT,
		description: `
The stdout output type prints messages to stdout. Single part messages are
printed with a delimiter (defaults to '\n' if left empty). Multipart messages
are written with each part delimited, with the final part followed by two
delimiters, e.g. a multipart message [ "foo", "bar", "baz" ] would be written
as:

foo\n
bar\n
baz\n\n`,
	}
}

//------------------------------------------------------------------------------

// STDOUTConfig contains configuration fields for the stdout based output type.
type STDOUTConfig struct {
	Delim string `json:"delimiter" yaml:"delimiter"`
}

// NewSTDOUTConfig creates a new STDOUTConfig with default values.
func NewSTDOUTConfig() STDOUTConfig {
	return STDOUTConfig{
		Delim: "",
	}
}

//------------------------------------------------------------------------------

// NewSTDOUT creates a new STDOUT output type.
func NewSTDOUT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return NewLineWriter(os.Stdout, false, []byte(conf.STDOUT.Delim), "stdout", log, stats)
}

//------------------------------------------------------------------------------
