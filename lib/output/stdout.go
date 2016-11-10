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
	"os"

	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["stdout"] = typeSpec{
		constructor: NewSTDOUT,
		description: `
The 'stdout' output type prints messages to stdout. Single part messages are
printed with a line separator '\n'. Multipart messages are written with each
part line separated, with the final part followed by two line separators, e.g.
a multipart message [ "foo", "bar", "baz" ] would be written as:

foo\n
bar\n
baz\n\n`,
	}
}

//--------------------------------------------------------------------------------------------------

// NewSTDOUT - Create a new STDOUT output type.
func NewSTDOUT(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return newWriter(os.Stdout, log, stats)
}

//--------------------------------------------------------------------------------------------------
